use anyhow::anyhow;
use btleplug::{
    api::{Central, CharPropFlags, Characteristic, Peripheral as _, Service, WriteType},
    platform::{Adapter, Peripheral},
};
use openwhoop_entities::packets::Model;
use futures::StreamExt;
use std::{
    collections::BTreeSet,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::time::{sleep, timeout};
use uuid::Uuid;
use openwhoop_codec::{
    WhoopData, WhoopPacket,
    constants::{
        CMD_FROM_STRAP, CMD_TO_STRAP, DATA_FROM_STRAP, EVENTS_FROM_STRAP, MEMFAULT, WHOOP_SERVICE,
    },
};

use crate::{db::DatabaseHandler, openwhoop::OpenWhoop};

pub struct WhoopDevice {
    peripheral: Peripheral,
    whoop: OpenWhoop,
    debug_packets: bool,
    adapter: Adapter,
}

impl WhoopDevice {
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
    const DISCOVER_SERVICES_TIMEOUT: Duration = Duration::from_secs(15);
    const HISTORY_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

    pub fn new(
        peripheral: Peripheral,
        adapter: Adapter,
        db: DatabaseHandler,
        debug_packets: bool,
    ) -> Self {
        Self {
            peripheral,
            whoop: OpenWhoop::new(db),
            debug_packets,
            adapter,
        }
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        println!("Stage: connect -> starting");
        info!("Connecting to peripheral...");
        timeout(Self::CONNECT_TIMEOUT, self.peripheral.connect())
            .await
            .map_err(|_| anyhow!("Timed out after {}s connecting to WHOOP", Self::CONNECT_TIMEOUT.as_secs()))??;
        println!("Stage: connect -> connected");
        info!("Peripheral connection established");
        let _ = self.adapter.stop_scan().await;
        println!("Stage: discover_services -> starting");
        info!("Discovering WHOOP services...");
        timeout(
            Self::DISCOVER_SERVICES_TIMEOUT,
            self.peripheral.discover_services(),
        )
        .await
        .map_err(|_| {
            anyhow!(
                "Timed out after {}s discovering WHOOP services",
                Self::DISCOVER_SERVICES_TIMEOUT.as_secs()
            )
        })??;
        let services = self.peripheral.services();
        println!("Stage: discover_services -> found {} services", services.len());
        info!("Discovered {} services", services.len());
        self.whoop.packet_buffer.clear();
        Ok(())
    }

    pub async fn is_connected(&mut self) -> anyhow::Result<bool> {
        let is_connected = self.peripheral.is_connected().await?;
        Ok(is_connected)
    }

    fn create_char(characteristic: Uuid) -> Characteristic {
        Characteristic {
            uuid: characteristic,
            service_uuid: WHOOP_SERVICE,
            properties: CharPropFlags::empty(),
            descriptors: BTreeSet::new(),
        }
    }

    async fn subscribe(&self, char: Uuid) -> anyhow::Result<()> {
        debug!("Subscribing to characteristic {}", char);
        self.peripheral.subscribe(&Self::create_char(char)).await?;
        Ok(())
    }

    pub async fn initialize(&mut self) -> anyhow::Result<()> {
        println!("Stage: subscribe -> starting");
        info!("Subscribing to WHOOP notification characteristics...");
        self.subscribe(DATA_FROM_STRAP).await?;
        self.subscribe(CMD_FROM_STRAP).await?;
        self.subscribe(EVENTS_FROM_STRAP).await?;
        self.subscribe(MEMFAULT).await?;
        println!("Stage: subscribe -> ready");

        println!("Stage: initialize -> sending bootstrap commands");
        info!("Sending WHOOP initialization commands...");
        println!("Stage: initialize -> hello_harvard");
        self.send_command(WhoopPacket::hello_harvard()).await?;
        println!("Stage: initialize -> set_time");
        self.send_command(WhoopPacket::set_time()?).await?;
        println!("Stage: initialize -> get_name");
        self.send_command(WhoopPacket::get_name()).await?;

        println!("Stage: initialize -> entering high frequency sync");
        info!("Entering high-frequency sync mode...");
        self.send_command(WhoopPacket::enter_high_freq_sync())
            .await?;
        println!("Stage: initialize -> complete");
        Ok(())
    }

    pub async fn send_command(&mut self, packet: WhoopPacket) -> anyhow::Result<()> {
        let packet = packet.framed_packet()?;
        trace!("Writing WHOOP command ({} bytes)", packet.len());
        self.peripheral
            .write(
                &Self::create_char(CMD_TO_STRAP),
                &packet,
                WriteType::WithoutResponse,
            )
            .await?;
        Ok(())
    }

    pub async fn sync_history(&mut self, should_exit: Arc<AtomicBool>) -> anyhow::Result<()> {
        println!("Stage: notifications -> opening stream");
        info!("Subscribing to WHOOP notifications stream...");
        let mut notifications = self.peripheral.notifications().await?;
        println!("Stage: notifications -> stream ready");

        info!("Starting WHOOP history sync...");
        self.send_command(WhoopPacket::history_start()).await?;
        println!("Stage: history_start -> sent");
        info!("Sent history_start command, waiting for notifications...");
        let mut notification_count = 0usize;
        let mut first_notification_logged = false;

        'a: loop {
            if should_exit.load(Ordering::SeqCst) {
                info!("Stopping history sync (CTRL+C requested)");
                break;
            }
            let notification = notifications.next();
            let sleep_ = sleep(Self::HISTORY_IDLE_TIMEOUT);

            tokio::select! {
                _ = sleep_ => {
                    if self.on_sleep().await? {
                        error!("WHOOP disconnected during sync; attempting reconnect");
                        for attempt in 1..=5 {
                            info!("Reconnect attempt {attempt}/5");
                            if self.connect().await.is_ok() {
                                info!("Reconnected successfully, re-initializing sync");
                                self.initialize().await?;
                                self.send_command(WhoopPacket::history_start()).await?;
                                continue 'a;
                            }

                            sleep(Duration::from_secs(10)).await;
                        }

                        error!("Could not reconnect to WHOOP after 5 attempts");
                        break;
                    } else if self.whoop.is_history_caught_up() {
                        info!("Latest WHOOP history reading is near real time; finishing sync");
                        self.whoop.flush_current_history_batch().await?;
                        break;
                    }
                },
                Some(notification) = notification => {
                    notification_count += 1;
                    if !first_notification_logged {
                        println!(
                            "Stage: notifications -> first packet on {} ({} bytes)",
                            notification.uuid,
                            notification.value.len()
                        );
                        first_notification_logged = true;
                    } else if notification_count <= 5 {
                        println!(
                            "Stage: notifications -> packet {} on {} ({} bytes)",
                            notification_count,
                            notification.uuid,
                            notification.value.len()
                        );
                    }
                    let packet = match self.debug_packets {
                        true => self.whoop.store_packet(notification).await?,
                        false => Model {
                            id: 0,
                            device_id: self
                                .whoop
                                .database
                                .current_device_id()
                                .unwrap_or("unknown")
                                .to_string(),
                            uuid: notification.uuid,
                            bytes: notification.value,
                        },
                    };

                    if let Some(packet) = self.whoop.handle_packet(packet).await?{
                        self.send_command(packet).await?;
                    }
                }
            }
        }

        self.whoop.flush_pending_history_writes().await?;

        info!("History sync loop finished");
        Ok(())
    }

    pub async fn disconnect(&mut self) -> anyhow::Result<()> {
        if self.peripheral.is_connected().await? {
            println!("Stage: disconnect -> starting");
            self.peripheral.disconnect().await?;
            println!("Stage: disconnect -> complete");
        }

        Ok(())
    }

    pub async fn probe(&mut self) -> anyhow::Result<()> {
        self.connect().await?;

        let services = self.peripheral.services().into_iter().collect::<Vec<_>>();
        print_services(&services);

        println!("Stage: notifications_probe -> subscribing");
        self.subscribe(DATA_FROM_STRAP).await?;
        self.subscribe(CMD_FROM_STRAP).await?;
        self.subscribe(EVENTS_FROM_STRAP).await?;
        self.subscribe(MEMFAULT).await?;
        println!("Stage: notifications_probe -> subscribed");

        println!("Stage: notifications_probe -> opening stream");
        let _notifications = timeout(Duration::from_secs(5), self.peripheral.notifications())
            .await
            .map_err(|_| anyhow!("Timed out after 5s opening notifications stream"))??;
        println!("Stage: notifications_probe -> stream ready");

        println!("Stage: command_probe -> get_name");
        self.send_command(WhoopPacket::get_name()).await?;

        println!("Stage: command_probe -> version");
        self.send_command(WhoopPacket::version()).await?;

        println!("Probe completed successfully");
        self.disconnect().await?;
        Ok(())
    }

    async fn on_sleep(&mut self) -> anyhow::Result<bool> {
        let is_connected = self.peripheral.is_connected().await?;
        Ok(!is_connected)
    }

    pub async fn get_version(&mut self) -> anyhow::Result<()> {
        self.subscribe(CMD_FROM_STRAP).await?;

        let mut notifications = self.peripheral.notifications().await?;
        self.send_command(WhoopPacket::version()).await?;

        let timeout_duration = Duration::from_secs(5);
        match timeout(timeout_duration, notifications.next()).await {
            Ok(Some(notification)) => {
                let packet = WhoopPacket::from_data(notification.value)?;
                let data = WhoopData::from_packet(packet)?;
                if let WhoopData::VersionInfo { harvard, boylston } = data {
                    info!("version harvard {} boylston {}", harvard, boylston);
                }
                Ok(())
            }
            Ok(None) => Err(anyhow!("stream ended unexpectedly")),
            Err(_) => Err(anyhow!("timed out waiting for version notification")),
        }
    }

    pub async fn run_post_sync_processing(&self) -> anyhow::Result<()> {
        info!("Calculating derived metrics from synced history...");

        info!("Calculating stress...");
        self.whoop.calculate_stress().await?;

        info!("Calculating SpO2...");
        self.whoop.calculate_spo2().await?;

        info!("Calculating skin temperature...");
        self.whoop.calculate_skin_temp().await?;

        info!("Post-sync processing completed");
        Ok(())
    }
}

fn print_services(services: &[Service]) {
    println!("Discovered services and characteristics:");
    for service in services {
        println!("  Service {}", service.uuid);
        for characteristic in &service.characteristics {
            println!(
                "    Characteristic {} ({:?})",
                characteristic.uuid, characteristic.properties
            );
        }
    }
}
