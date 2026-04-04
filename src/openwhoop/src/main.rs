#[macro_use]
extern crate log;

use std::{
    cmp::Ordering as CmpOrdering,
    io,
    io::Write,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::anyhow;
use btleplug::{
    api::{Central, Manager as _, Peripheral as _, PeripheralProperties, ScanFilter},
    platform::{Adapter, Manager, Peripheral},
};
use chrono::{DateTime, Local, NaiveDateTime, NaiveTime, TimeDelta, Utc};
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{Shell, generate};
use openwhoop_entities::packets;
use dotenv::dotenv;
use openwhoop::{
    OpenWhoop, WhoopDevice,
    algo::{ExerciseMetrics, SleepConsistencyAnalyzer, helpers::format_hm::FormatHM},
    db::DatabaseHandler,
    types::activities::{ActivityType, SearchActivityPeriods},
};
use tokio::time::sleep;
use openwhoop::api;
use openwhoop_codec::{WhoopPacket, constants::WHOOP_SERVICE};

#[derive(Parser)]
pub struct OpenWhoopCli {
    #[arg(env, long)]
    pub debug_packets: bool,
    #[arg(env, long)]
    pub database_url: String,
    #[arg(env = "WHOOP", long)]
    pub whoop: Option<String>,
    #[cfg(target_os = "linux")]
    #[arg(env, long)]
    pub ble_interface: Option<String>,
    #[clap(subcommand)]
    pub subcommand: OpenWhoopCommand,
}

#[derive(Subcommand)]
pub enum OpenWhoopCommand {
    ///
    /// Scan for Whoop devices
    ///
    Scan,
    ///
    /// Download history data from whoop devices
    ///
    DownloadHistory,
    ///
    /// Probe a WHOOP connection without starting a full history sync
    ///
    Probe,
    ///
    /// Reruns the packet processing on stored packets
    /// This is used after new more of packets get handled
    ///
    ReRun,
    ///
    /// Detects sleeps and exercises
    ///
    DetectEvents,
    ///
    /// Print sleep statistics for all time and last week
    ///
    SleepStats,
    ///
    /// Print latest detected sleep summary
    ///
    LatestSleep,
    ///
    /// Print activity statistics for all time and last week
    ///
    ExerciseStats,
    ///
    /// Calculate stress for historical data
    ///
    CalculateStress,
    ///
    /// Calculate SpO2 from raw sensor data
    ///
    CalculateSpo2,
    ///
    /// Calculate skin temperature from raw sensor data
    ///
    CalculateSkinTemp,
    ///
    /// Set alarm
    ///
    SetAlarm { alarm_time: AlarmTime },
    ///
    /// Copy packets from one database into another
    ///
    Merge { from: String },
    Restart,
    ///
    /// Erase all history data from the device
    ///
    Erase,
    ///
    /// Get device firmware version info
    ///
    Version,
    ///
    /// Generate Shell completions
    ///
    Completions { shell: Shell },
    ///
    /// Enable IMU data
    ///
    EnableImu,
    ///
    /// Sync data between local and remote database
    ///
    Sync {
        #[arg(long, env)]
        remote: String,
    },
    ///
    /// Download firmware from WHOOP API
    ///
    DownloadFirmware {
        #[arg(long, env = "WHOOP_EMAIL")]
        email: String,
        #[arg(long, env = "WHOOP_PASSWORD")]
        password: String,
        #[arg(long, default_value = "HARVARD")]
        device_name: String,
        #[arg(long, default_value = "41.16.5.0")]
        maxim: String,
        #[arg(long, default_value = "17.2.2.0")]
        nordic: String,
        #[arg(long, default_value = "./firmware")]
        output_dir: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if let Err(error) = dotenv() {
        println!("{}", error);
    }

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .filter_module("sqlx::query", log::LevelFilter::Off)
        .filter_module("sea_orm_migration::migrator", log::LevelFilter::Off)
        .filter_module("bluez_async", log::LevelFilter::Off)
        .filter_module("sqlx::postgres::notice", log::LevelFilter::Off)
        .init();

    OpenWhoopCli::parse().run().await
}

async fn download_firmware(
    email: &str,
    password: &str,
    device_name: &str,
    maxim: &str,
    nordic: &str,
    output_dir: &str,
) -> anyhow::Result<()> {
    info!("authenticating...");
    let client = api::WhoopApiClient::sign_in(email, password).await?;

    let chip_names = match device_name {
        "HARVARD" => vec!["MAXIM", "NORDIC"],
        "PUFFIN" => vec!["MAXIM", "NORDIC", "RUGGLES", "PEARL"],
        other => anyhow::bail!("unknown device family: {other}"),
    };

    let target_versions: std::collections::HashMap<&str, &str> =
        [("MAXIM", maxim), ("NORDIC", nordic)]
            .into_iter()
            .collect();

    let current: Vec<api::ChipFirmware> = chip_names
        .iter()
        .map(|c| api::ChipFirmware {
            chip_name: c.to_string(),
            version: "1.0.0.0".into(),
        })
        .collect();

    let upgrade: Vec<api::ChipFirmware> = chip_names
        .iter()
        .map(|c| api::ChipFirmware {
            chip_name: c.to_string(),
            version: target_versions.get(c).unwrap_or(&"1.0.0.0").to_string(),
        })
        .collect();

    info!("device: {device_name}");
    for uv in &upgrade {
        info!("  target {}: {}", uv.chip_name, uv.version);
    }

    info!("downloading firmware...");
    let fw_b64 = client
        .download_firmware(device_name, current, upgrade)
        .await?;

    api::decode_and_extract(&fw_b64, std::path::Path::new(output_dir))?;
    Ok(())
}

async fn scan_command(
    adapter: &Adapter,
    device_id: Option<String>,
) -> anyhow::Result<Peripheral> {
    select_whoop_candidate(adapter, device_id, true)
        .await
        .map(|candidate| candidate.peripheral)
}

#[derive(Clone)]
struct WhoopScanCandidate {
    peripheral: Peripheral,
    identifier: String,
    address: String,
    name: Option<String>,
    rssi: Option<i16>,
}

impl WhoopScanCandidate {
    fn from_properties(peripheral: Peripheral, properties: PeripheralProperties) -> Option<Self> {
        if !is_whoop_candidate(&properties) {
            return None;
        }

        let name = candidate_name(&properties);

        Some(Self {
            identifier: peripheral.id().to_string(),
            address: properties.address.to_string(),
            peripheral,
            name,
            rssi: properties.rssi,
        })
    }

    fn update_from(&mut self, newer: Self) {
        if should_replace_name(self.name.as_deref(), newer.name.as_deref()) {
            self.name = newer.name;
        }

        if is_stronger_rssi(newer.rssi, self.rssi) {
            self.rssi = newer.rssi;
        }

        self.peripheral = newer.peripheral;
    }

    fn title(&self) -> String {
        self.name
            .clone()
            .unwrap_or_else(|| "Unnamed WHOOP".to_string())
    }

    fn config_value(&self) -> String {
        #[cfg(target_os = "linux")]
        {
            self.address.clone()
        }

        #[cfg(target_os = "macos")]
        {
            self.identifier.clone()
        }
    }

    fn print_summary(&self, index: usize) {
        println!("{}. {}", index + 1, self.title());
        println!("   Stable ID: {}", self.identifier);
        println!("   Address: {}", self.address);
        println!("   RSSI: {}", format_rssi(self.rssi));
    }
}

fn is_stronger_rssi(candidate: Option<i16>, current: Option<i16>) -> bool {
    match (candidate, current) {
        (Some(candidate), Some(current)) => candidate > current,
        (Some(_), None) => true,
        _ => false,
    }
}

fn should_replace_name(current: Option<&str>, candidate: Option<&str>) -> bool {
    let Some(candidate) = candidate else {
        return false;
    };

    let current = current.unwrap_or_default().trim();
    let candidate = candidate.trim();

    if candidate.is_empty() {
        return false;
    }

    current.is_empty() || candidate.len() > current.len()
}

fn format_rssi(rssi: Option<i16>) -> String {
    rssi.map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn candidate_name(properties: &PeripheralProperties) -> Option<String> {
    #[cfg(target_os = "macos")]
    {
        properties.local_name.as_deref().map(sanitize_name)
    }

    #[cfg(not(target_os = "macos"))]
    {
        properties.local_name.clone()
    }
}

fn is_whoop_candidate(properties: &PeripheralProperties) -> bool {
    if properties.services.contains(&WHOOP_SERVICE) {
        return true;
    }

    #[cfg(target_os = "macos")]
    {
        return candidate_name(properties)
            .map(|name| name.to_ascii_lowercase().contains("whoop"))
            .unwrap_or(false);
    }

    #[cfg(not(target_os = "macos"))]
    {
        false
    }
}

fn sort_candidates(candidates: &mut [WhoopScanCandidate]) {
    candidates.sort_by(|left, right| {
        match (right.rssi, left.rssi) {
            (Some(right_rssi), Some(left_rssi)) if right_rssi != left_rssi => {
                right_rssi.cmp(&left_rssi)
            }
            (Some(_), None) => CmpOrdering::Less,
            (None, Some(_)) => CmpOrdering::Greater,
            _ => left.title().cmp(&right.title()),
        }
    });
}

fn candidate_matches_request(candidate: &WhoopScanCandidate, requested: &str) -> bool {
    #[cfg(target_os = "linux")]
    {
        let requested = requested.trim();
        candidate.address.eq_ignore_ascii_case(requested)
    }

    #[cfg(target_os = "macos")]
    {
        let requested = requested.trim();
        if candidate.identifier.eq_ignore_ascii_case(requested) {
            return true;
        }

        let Some(name) = candidate.name.as_deref() else {
            return false;
        };

        let candidate_normalized = sanitize_name(name).to_ascii_lowercase();
        let requested_normalized = sanitize_name(requested).to_ascii_lowercase();

        candidate_normalized == requested_normalized
            || candidate_normalized.starts_with(&requested_normalized)
            || candidate_normalized.contains(&requested_normalized)
            || requested_normalized.contains(&candidate_normalized)
    }
}

async fn discover_whoop_candidates(
    adapter: &Adapter,
    scan_timeout: Duration,
) -> anyhow::Result<Vec<WhoopScanCandidate>> {
    const SCAN_POLL_INTERVAL: Duration = Duration::from_secs(1);

    #[cfg(target_os = "linux")]
    let scan_filter = ScanFilter {
        services: vec![WHOOP_SERVICE],
    };

    #[cfg(target_os = "macos")]
    let scan_filter = ScanFilter::default();

    adapter.start_scan(scan_filter).await?;
    println!(
        "Stage: scan -> started (timeout {}s)",
        scan_timeout.as_secs()
    );

    let started_at = tokio::time::Instant::now();
    let mut candidates: std::collections::BTreeMap<String, WhoopScanCandidate> =
        std::collections::BTreeMap::new();

    while started_at.elapsed() < scan_timeout {
        let peripherals = adapter.peripherals().await?;

        for peripheral in peripherals {
            let Some(properties) = peripheral.properties().await? else {
                continue;
            };

            let Some(candidate) = WhoopScanCandidate::from_properties(peripheral, properties) else {
                continue;
            };

            let key = candidate.identifier.clone();
            match candidates.get_mut(&key) {
                Some(existing) => existing.update_from(candidate),
                None => {
                    println!(
                        "Stage: scan -> candidate '{}' (stable id: {}, RSSI: {})",
                        candidate.title(),
                        candidate.identifier,
                        format_rssi(candidate.rssi)
                    );
                    candidates.insert(key, candidate);
                }
            }
        }

        sleep(SCAN_POLL_INTERVAL).await;
    }

    let _ = adapter.stop_scan().await;

    let mut candidates = candidates.into_values().collect::<Vec<_>>();
    sort_candidates(&mut candidates);
    Ok(candidates)
}

fn print_candidate_list(candidates: &[WhoopScanCandidate]) {
    println!("Found {} WHOOP device(s):", candidates.len());
    for (index, candidate) in candidates.iter().enumerate() {
        candidate.print_summary(index);
    }
}

fn prompt_for_candidate(candidates: &[WhoopScanCandidate]) -> anyhow::Result<WhoopScanCandidate> {
    if candidates.len() == 1 {
        let candidate = candidates[0].clone();
        println!("Selected the only WHOOP device found: {}", candidate.title());
        return Ok(candidate);
    }

    loop {
        print!("Choose a WHOOP device [1-{}]: ", candidates.len());
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let trimmed = input.trim();

        let index = trimmed
            .parse::<usize>()
            .ok()
            .filter(|value| (1..=candidates.len()).contains(value));

        if let Some(index) = index {
            return Ok(candidates[index - 1].clone());
        }

        println!("Please enter a number between 1 and {}.", candidates.len());
    }
}

fn print_candidate_config_hint(candidate: &WhoopScanCandidate) {
    println!();
    println!("Use this device in later commands with:");
    println!("WHOOP=\"{}\"", candidate.config_value());
}

async fn select_whoop_candidate(
    adapter: &Adapter,
    device_id: Option<String>,
    interactive: bool,
) -> anyhow::Result<WhoopScanCandidate> {
    const TARGET_SCAN_TIMEOUT: Duration = Duration::from_secs(12);
    const REQUESTED_SCAN_TIMEOUT: Duration = Duration::from_secs(30);

    if let Some(device_id) = device_id.as_ref() {
        println!("Stage: select -> searching for requested WHOOP {}", device_id);
        let candidates = discover_whoop_candidates(adapter, REQUESTED_SCAN_TIMEOUT).await?;
        println!("Stage: select -> {} candidate(s) discovered", candidates.len());

        if let Some(candidate) = candidates
            .into_iter()
            .find(|candidate| candidate_matches_request(candidate, device_id))
        {
            println!(
                "Stage: select -> matched '{}' ({})",
                candidate.title(),
                candidate.identifier
            );
            info!(
                "Matched WHOOP candidate '{}' (stable id: {}, address: {}, RSSI: {})",
                candidate.title(),
                candidate.identifier,
                candidate.address,
                format_rssi(candidate.rssi)
            );
            return Ok(candidate);
        }

        anyhow::bail!(
            "Timed out after {}s waiting for WHOOP device '{}'",
            REQUESTED_SCAN_TIMEOUT.as_secs(),
            device_id
        );
    }

    println!("Scanning for WHOOP devices...");
    let candidates = discover_whoop_candidates(adapter, TARGET_SCAN_TIMEOUT).await?;
    println!("Stage: select -> {} candidate(s) discovered", candidates.len());

    if candidates.is_empty() {
        anyhow::bail!(
            "No WHOOP devices found after {}s",
            TARGET_SCAN_TIMEOUT.as_secs()
        );
    }

    print_candidate_list(&candidates);

    let candidate = if interactive {
        prompt_for_candidate(&candidates)?
    } else {
        candidates[0].clone()
    };

    print_candidate_config_hint(&candidate);
    Ok(candidate)
}

#[derive(Clone, Copy, Debug)]
pub enum AlarmTime {
    DateTime(NaiveDateTime),
    Time(NaiveTime),
    Minute,
    Minute5,
    Minute10,
    Minute15,
    Minute30,
    Hour,
}

impl FromStr for AlarmTime {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(t) = s.parse() {
            return Ok(Self::DateTime(t));
        }

        if let Ok(t) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
            return Ok(Self::DateTime(t));
        }

        if let Ok(t) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
            return Ok(Self::DateTime(t));
        }

        if let Ok(t) = s.parse() {
            return Ok(Self::Time(t));
        }

        match s {
            "minute" | "1min" | "min" => Ok(Self::Minute),
            "5minute" | "5min" => Ok(Self::Minute5),
            "10minute" | "10min" => Ok(Self::Minute10),
            "15minute" | "15min" => Ok(Self::Minute15),
            "30minute" | "30min" => Ok(Self::Minute30),
            "hour" | "h" => Ok(Self::Hour),
            _ => Err(anyhow!("Invalid alarm time")),
        }
    }
}

impl AlarmTime {
    pub fn unix(self) -> DateTime<Utc> {
        let mut now = Utc::now();
        let timezone_df = Local::now().offset().to_owned();

        match self {
            AlarmTime::DateTime(dt) => dt.and_utc() - timezone_df,
            AlarmTime::Time(t) => {
                let current_time = now.time();
                if current_time > t {
                    now += TimeDelta::days(1);
                }

                now.with_time(t).unwrap() - timezone_df
            }
            _ => {
                let offset = self.offset();
                now + offset
            }
        }
    }

    fn offset(self) -> TimeDelta {
        match self {
            AlarmTime::DateTime(_) => todo!(),
            AlarmTime::Time(_) => todo!(),
            AlarmTime::Minute => TimeDelta::minutes(1),
            AlarmTime::Minute5 => TimeDelta::minutes(5),
            AlarmTime::Minute10 => TimeDelta::minutes(10),
            AlarmTime::Minute15 => TimeDelta::minutes(15),
            AlarmTime::Minute30 => TimeDelta::minutes(30),
            AlarmTime::Hour => TimeDelta::hours(1),
        }
    }
}

#[cfg(target_os = "macos")]
pub fn sanitize_name(name: &str) -> String {
    name.chars()
        .filter(|c| !c.is_control())
        .collect::<String>()
        .trim()
        .to_string()
}

impl OpenWhoopCli {
    async fn run(self) -> anyhow::Result<()> {
        let OpenWhoopCli {
            debug_packets,
            database_url,
            whoop: whoop_filter,
            #[cfg(target_os = "linux")]
            ble_interface,
            subcommand,
        } = self;

        if let OpenWhoopCommand::DownloadFirmware {
            email,
            password,
            device_name,
            maxim,
            nordic,
            output_dir,
        } = &subcommand
        {
            return download_firmware(email, password, device_name, maxim, nordic, output_dir).await;
        }

        let needs_ble = matches!(
            &subcommand,
            OpenWhoopCommand::Scan
                | OpenWhoopCommand::DownloadHistory
                | OpenWhoopCommand::Probe
                | OpenWhoopCommand::SetAlarm { .. }
                | OpenWhoopCommand::Restart
                | OpenWhoopCommand::Erase
                | OpenWhoopCommand::Version
                | OpenWhoopCommand::EnableImu
        );

        let mut adapter = if needs_ble {
            let manager = Manager::new().await?;
            Some(default_adapter_for_cli(
                &manager,
                #[cfg(target_os = "linux")]
                ble_interface.as_ref(),
            )
            .await?)
        } else {
            None
        };

        match subcommand {
            OpenWhoopCommand::Scan => {
                let adapter = adapter.take().expect("BLE adapter should be initialized");
                scan_command(&adapter, None).await?;
            }
            OpenWhoopCommand::DownloadHistory => {
                let adapter = adapter.take().expect("BLE adapter should be initialized");
                if let Some(whoop) = whoop_filter.as_ref() {
                    info!("Scanning for WHOOP device: {whoop}");
                } else {
                    info!("No WHOOP device configured, prompting from nearby WHOOP devices");
                }
                let candidate = select_whoop_candidate(&adapter, whoop_filter.clone(), true).await?;
                let selected_device_id = candidate.config_value();
                info!("Using WHOOP device id {}", selected_device_id);
                let db_handler = DatabaseHandler::new(database_url.clone())
                    .await
                    .with_device_id(Some(selected_device_id));
                let mut whoop =
                    WhoopDevice::new(candidate.peripheral, adapter, db_handler, debug_packets);

                let should_exit = Arc::new(AtomicBool::new(false));

                let se = should_exit.clone();
                ctrlc::set_handler(move || {
                    println!("Received CTRL+C!");
                    se.store(true, Ordering::SeqCst);
                })?;

                info!("Connecting to WHOOP");
                whoop.connect().await?;
                info!("Initializing WHOOP sync mode");
                whoop.initialize().await?;

                info!("Downloading history from WHOOP...");
                let result = whoop.sync_history(should_exit).await;

                info!("Exiting...");
                if let Err(ref e) = result {
                    error!("{}", e);
                } else {
                    info!("WHOOP history download completed");
                }

                loop {
                    if let Ok(true) = whoop.is_connected().await {
                        whoop
                            .send_command(WhoopPacket::exit_high_freq_sync())
                            .await?;
                        break;
                    } else {
                        whoop.connect().await?;
                        sleep(Duration::from_secs(1)).await;
                    }
                }

                if result.is_ok() {
                    whoop.run_post_sync_processing().await?;
                }
            }
            OpenWhoopCommand::Probe => {
                let adapter = adapter.take().expect("BLE adapter should be initialized");
                if let Some(whoop) = whoop_filter.as_ref() {
                    info!("Probing WHOOP device: {whoop}");
                } else {
                    info!("No WHOOP device configured, prompting from nearby WHOOP devices");
                }
                let candidate = select_whoop_candidate(&adapter, whoop_filter.clone(), true).await?;
                let selected_device_id = candidate.config_value();
                info!("Using WHOOP device id {}", selected_device_id);
                let db_handler = DatabaseHandler::new(database_url.clone())
                    .await
                    .with_device_id(Some(selected_device_id));
                let mut whoop =
                    WhoopDevice::new(candidate.peripheral, adapter, db_handler, debug_packets);
                whoop.probe().await?;
            }
            OpenWhoopCommand::ReRun => {
                let db_handler = DatabaseHandler::new(database_url.clone())
                    .await
                    .with_device_id(whoop_filter.clone());
                let mut whoop = OpenWhoop::new(db_handler.clone());
                let mut id = 0;
                loop {
                    let packets = db_handler.get_packets(id).await?;
                    if packets.is_empty() {
                        break;
                    }

                    for packet in packets {
                        id = packet.id;
                        whoop.handle_packet(packet).await?;
                    }

                    println!("{}", id);
                }
            }
            OpenWhoopCommand::DetectEvents => {
                let db_handler = DatabaseHandler::new(database_url.clone())
                    .await
                    .with_device_id(whoop_filter.clone());
                let whoop = OpenWhoop::new(db_handler);
                whoop.detect_sleeps().await?;
                whoop.detect_events().await?;
            }
            OpenWhoopCommand::SleepStats => {
                let db_handler = DatabaseHandler::new(database_url.clone())
                    .await
                    .with_device_id(whoop_filter.clone());
                let whoop = OpenWhoop::new(db_handler);
                let sleep_records = whoop.database.get_sleep_cycles(None).await?;

                if sleep_records.is_empty() {
                    println!("No sleep records found, exiting now");
                    return Ok(());
                }

                let mut last_week = sleep_records
                    .iter()
                    .rev()
                    .take(7)
                    .copied()
                    .collect::<Vec<_>>();

                last_week.reverse();
                let analyzer = SleepConsistencyAnalyzer::new(sleep_records);
                let metrics = analyzer.calculate_consistency_metrics()?;
                println!("All time: \n{}", metrics);
                let analyzer = SleepConsistencyAnalyzer::new(last_week);
                let metrics = analyzer.calculate_consistency_metrics()?;
                println!("\nWeek: \n{}", metrics);
            }
            OpenWhoopCommand::LatestSleep => {
                let db_handler = DatabaseHandler::new(database_url.clone())
                    .await
                    .with_device_id(whoop_filter.clone());
                let whoop = OpenWhoop::new(db_handler);
                let Some(sleep) = whoop.get_latest_sleep().await? else {
                    println!("No sleep records found, exiting now");
                    return Ok(());
                };

                println!("Latest sleep");
                println!("  Date: {}", sleep.id);
                println!("  Start: {}", sleep.start);
                println!("  End: {}", sleep.end);
                println!("  Duration: {}", sleep.duration().format_hm());
                println!("  Score: {:.1}", sleep.score);
                println!(
                    "  Heart rate (min/avg/max): {}/{}/{} bpm",
                    sleep.min_bpm, sleep.avg_bpm, sleep.max_bpm
                );
                println!(
                    "  HRV RMSSD (min/avg/max): {}/{}/{} ms",
                    sleep.min_hrv, sleep.avg_hrv, sleep.max_hrv
                );
            }
            OpenWhoopCommand::ExerciseStats => {
                let db_handler = DatabaseHandler::new(database_url.clone())
                    .await
                    .with_device_id(whoop_filter.clone());
                let whoop = OpenWhoop::new(db_handler);
                let exercises = whoop
                    .database
                    .search_activities(
                        SearchActivityPeriods::default().with_activity(ActivityType::Activity),
                    )
                    .await?;

                if exercises.is_empty() {
                    println!("No activities found, exiting now");
                    return Ok(());
                };

                let last_week = exercises
                    .iter()
                    .rev()
                    .take(7)
                    .copied()
                    .rev()
                    .collect::<Vec<_>>();

                let metrics = ExerciseMetrics::new(exercises)?;
                let last_week = ExerciseMetrics::new(last_week)?;

                println!("All time: \n{}", metrics);
                println!("Last week: \n{}", last_week);
            }
            OpenWhoopCommand::CalculateStress => {
                let db_handler = DatabaseHandler::new(database_url.clone())
                    .await
                    .with_device_id(whoop_filter.clone());
                let whoop = OpenWhoop::new(db_handler);
                whoop.calculate_stress().await?;
            }
            OpenWhoopCommand::CalculateSpo2 => {
                let db_handler = DatabaseHandler::new(database_url.clone())
                    .await
                    .with_device_id(whoop_filter.clone());
                let whoop = OpenWhoop::new(db_handler);
                whoop.calculate_spo2().await?;
            }
            OpenWhoopCommand::CalculateSkinTemp => {
                let db_handler = DatabaseHandler::new(database_url.clone())
                    .await
                    .with_device_id(whoop_filter.clone());
                let whoop = OpenWhoop::new(db_handler);
                whoop.calculate_skin_temp().await?;
            }
            OpenWhoopCommand::SetAlarm { alarm_time } => {
                let adapter = adapter.take().expect("BLE adapter should be initialized");
                let candidate = select_whoop_candidate(&adapter, whoop_filter.clone(), true).await?;
                let db_handler = DatabaseHandler::new(database_url.clone())
                    .await
                    .with_device_id(Some(candidate.config_value()));
                let mut whoop =
                    WhoopDevice::new(candidate.peripheral, adapter, db_handler, debug_packets);
                whoop.connect().await?;

                let time = alarm_time.unix();
                let now = Utc::now();

                if time < now {
                    error!(
                        "Time {} is in past, current time: {}",
                        time.format("%Y-%m-%d %H:%M:%S"),
                        now.format("%Y-%m-%d %H:%M:%S")
                    );
                    return Ok(());
                }

                let packet = WhoopPacket::alarm_time(u32::try_from(time.timestamp())?);
                whoop.send_command(packet).await?;
                let time = time.with_timezone(&Local);

                println!("Alarm time set for: {}", time.format("%Y-%m-%d %H:%M:%S"));
            }
            OpenWhoopCommand::Merge { from } => {
                let db_handler = DatabaseHandler::new(database_url.clone())
                    .await
                    .with_device_id(whoop_filter.clone());
                let from_db = DatabaseHandler::new(from)
                    .await
                    .with_device_id(whoop_filter.clone());

                let mut id = 0;
                loop {
                    let packets = from_db.get_packets(id).await?;
                    if packets.is_empty() {
                        break;
                    }

                    for packets::Model {
                        device_id: _,
                        uuid,
                        bytes,
                        id: c_id,
                    } in packets
                    {
                        id = c_id;
                        db_handler.create_packet(uuid, bytes).await?;
                    }

                    println!("{}", id);
                }
            }
            OpenWhoopCommand::Restart => {
                let adapter = adapter.take().expect("BLE adapter should be initialized");
                let candidate = select_whoop_candidate(&adapter, whoop_filter.clone(), true).await?;
                let db_handler = DatabaseHandler::new(database_url.clone())
                    .await
                    .with_device_id(Some(candidate.config_value()));
                let mut whoop =
                    WhoopDevice::new(candidate.peripheral, adapter, db_handler, debug_packets);
                whoop.connect().await?;
                whoop.send_command(WhoopPacket::restart()).await?;
            }
            OpenWhoopCommand::Erase => {
                let adapter = adapter.take().expect("BLE adapter should be initialized");
                let candidate = select_whoop_candidate(&adapter, whoop_filter.clone(), true).await?;
                let db_handler = DatabaseHandler::new(database_url.clone())
                    .await
                    .with_device_id(Some(candidate.config_value()));
                let mut whoop =
                    WhoopDevice::new(candidate.peripheral, adapter, db_handler, debug_packets);
                whoop.connect().await?;
                whoop.send_command(WhoopPacket::erase()).await?;
                info!("Erase command sent - device will trim all stored history data");
            }
            OpenWhoopCommand::Version => {
                let adapter = adapter.take().expect("BLE adapter should be initialized");
                let candidate = select_whoop_candidate(&adapter, whoop_filter.clone(), true).await?;
                let db_handler = DatabaseHandler::new(database_url.clone())
                    .await
                    .with_device_id(Some(candidate.config_value()));
                let mut whoop = WhoopDevice::new(candidate.peripheral, adapter, db_handler, false);
                whoop.connect().await?;
                whoop.get_version().await?;
            }
            OpenWhoopCommand::EnableImu => {
                let adapter = adapter.take().expect("BLE adapter should be initialized");
                let candidate = select_whoop_candidate(&adapter, whoop_filter.clone(), true).await?;
                let db_handler = DatabaseHandler::new(database_url.clone())
                    .await
                    .with_device_id(Some(candidate.config_value()));
                let mut whoop = WhoopDevice::new(candidate.peripheral, adapter, db_handler, false);
                whoop.connect().await?;
                whoop
                    .send_command(WhoopPacket::toggle_r7_data_collection())
                    .await?;
            }
            OpenWhoopCommand::Sync { remote } => {
                let db_handler = DatabaseHandler::new(database_url.clone()).await;
                let remote_db = DatabaseHandler::new(remote).await;
                let sync = openwhoop::db::sync::DatabaseSync::new(
                    db_handler.connection(),
                    remote_db.connection(),
                );
                sync.run().await?;
            }
            OpenWhoopCommand::Completions { shell } => {
                let mut command = OpenWhoopCli::command();
                let bin_name = command.get_name().to_string();
                generate(shell, &mut command, bin_name, &mut io::stdout());
            }
            OpenWhoopCommand::DownloadFirmware { .. } => {
                unreachable!("handled before BLE/DB init")
            }
        }

        Ok(())
    }
}

#[cfg(target_os = "linux")]
async fn adapter_from_name(manager: &Manager, interface: &str) -> anyhow::Result<Adapter> {
    let adapters = manager.adapters().await?;
    let mut c_adapter = Err(anyhow!("Adapter: `{}` not found", interface));
    for adapter in adapters {
        let name = adapter.adapter_info().await?;
        if name.starts_with(interface) {
            c_adapter = Ok(adapter);
            break;
        }
    }

    c_adapter
}

async fn default_adapter(manager: &Manager) -> anyhow::Result<Adapter> {
    let adapters = manager.adapters().await?;
    adapters
        .into_iter()
        .next()
        .ok_or(anyhow!("No BLE adapters found"))
}

async fn default_adapter_for_cli(
    manager: &Manager,
    #[cfg(target_os = "linux")] ble_interface: Option<&String>,
) -> anyhow::Result<Adapter> {
    #[cfg(target_os = "linux")]
    match ble_interface {
        Some(interface) => adapter_from_name(manager, interface).await,
        None => default_adapter(manager).await,
    }

    #[cfg(target_os = "macos")]
    default_adapter(manager).await
}
