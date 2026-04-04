use sea_orm_migration::prelude::*;
use sea_orm::Statement;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        rename_table(manager, Packets::Table, LegacyPackets::Table).await?;
        create_packets_table(manager).await?;
        copy_rows(
            manager,
            "INSERT INTO packets (id, device_id, uuid, bytes)
             SELECT id, 'legacy', uuid, bytes FROM packets_legacy",
        )
        .await?;
        drop_table(manager, LegacyPackets::Table).await?;

        rename_table(manager, HeartRate::Table, LegacyHeartRate::Table).await?;
        create_heart_rate_table(manager).await?;
        copy_rows(
            manager,
            "INSERT INTO heart_rate (
                id, device_id, bpm, time, rr_intervals, activity, stress, spo2, skin_temp, imu_data, sensor_data, synced
             )
             SELECT
                id, 'legacy', bpm, time, rr_intervals, activity, stress, spo2, skin_temp, imu_data, sensor_data, synced
             FROM heart_rate_legacy",
        )
        .await?;
        drop_table(manager, LegacyHeartRate::Table).await?;

        rename_table(manager, Activities::Table, LegacyActivities::Table).await?;
        rename_table(manager, SleepCycles::Table, LegacySleepCycles::Table).await?;
        create_sleep_cycles_table(manager).await?;
        copy_rows(
            manager,
            "INSERT INTO sleep_cycles (
                id, device_id, sleep_id, start, end, min_bpm, max_bpm, avg_bpm, min_hrv, max_hrv, avg_hrv, score, synced
             )
             SELECT
                id, 'legacy', sleep_id, start, end, min_bpm, max_bpm, avg_bpm, min_hrv, max_hrv, avg_hrv, score, synced
             FROM sleep_cycles_legacy",
        )
        .await?;

        create_activities_table(manager).await?;
        copy_rows(
            manager,
            "INSERT INTO activities (id, device_id, period_id, start, end, activity, synced)
             SELECT id, 'legacy', period_id, start, end, activity, synced FROM activities_legacy",
        )
        .await?;
        drop_table(manager, LegacyActivities::Table).await?;
        drop_table(manager, LegacySleepCycles::Table).await?;

        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        Err(DbErr::Migration(
            "down migration is not supported for device-scoped data".to_string(),
        ))
    }
}

async fn rename_table(
    manager: &SchemaManager<'_>,
    from: impl Iden + 'static,
    to: impl Iden + 'static,
) -> Result<(), DbErr> {
    manager
        .rename_table(Table::rename().table(from, to).to_owned())
        .await
}

async fn drop_table(
    manager: &SchemaManager<'_>,
    table: impl Iden + 'static,
) -> Result<(), DbErr> {
    manager
        .drop_table(Table::drop().table(table).to_owned())
        .await
}

async fn copy_rows(manager: &SchemaManager<'_>, sql: &str) -> Result<(), DbErr> {
    let backend = manager.get_database_backend();
    manager
        .get_connection()
        .execute(Statement::from_string(backend, sql.to_string()))
        .await?;
    Ok(())
}

async fn create_packets_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Packets::Table)
                .col(
                    ColumnDef::new(Packets::Id)
                        .integer()
                        .not_null()
                        .auto_increment()
                        .primary_key(),
                )
                .col(ColumnDef::new(Packets::DeviceId).string().not_null())
                .col(ColumnDef::new(Packets::Uuid).uuid().not_null())
                .col(ColumnDef::new(Packets::Bytes).binary().not_null())
                .to_owned(),
        )
        .await
}

async fn create_heart_rate_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(HeartRate::Table)
                .col(
                    ColumnDef::new(HeartRate::Id)
                        .integer()
                        .not_null()
                        .auto_increment()
                        .primary_key(),
                )
                .col(ColumnDef::new(HeartRate::DeviceId).string().not_null())
                .col(ColumnDef::new(HeartRate::Bpm).small_integer().not_null())
                .col(ColumnDef::new(HeartRate::Time).date_time().not_null())
                .col(ColumnDef::new(HeartRate::RrIntervals).text().not_null())
                .col(ColumnDef::new(HeartRate::Activity).big_integer().null())
                .col(ColumnDef::new(HeartRate::Stress).double().null())
                .col(ColumnDef::new(HeartRate::Spo2).double().null())
                .col(ColumnDef::new(HeartRate::SkinTemp).double().null())
                .col(ColumnDef::new(HeartRate::ImuData).json().null())
                .col(ColumnDef::new(HeartRate::SensorData).json().null())
                .col(
                    ColumnDef::new(HeartRate::Synced)
                        .boolean()
                        .not_null()
                        .default(false),
                )
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .name("idx-heart-rate-device-time-unique")
                .table(HeartRate::Table)
                .col(HeartRate::DeviceId)
                .col(HeartRate::Time)
                .unique()
                .to_owned(),
        )
        .await
}

async fn create_sleep_cycles_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(SleepCycles::Table)
                .col(ColumnDef::new(SleepCycles::Id).uuid().not_null().primary_key())
                .col(ColumnDef::new(SleepCycles::DeviceId).string().not_null())
                .col(ColumnDef::new(SleepCycles::SleepId).date().not_null())
                .col(ColumnDef::new(SleepCycles::Start).date_time().not_null())
                .col(ColumnDef::new(SleepCycles::End).date_time().not_null())
                .col(ColumnDef::new(SleepCycles::MinBpm).small_integer().not_null())
                .col(ColumnDef::new(SleepCycles::MaxBpm).small_integer().not_null())
                .col(ColumnDef::new(SleepCycles::AvgBpm).small_integer().not_null())
                .col(ColumnDef::new(SleepCycles::MinHrv).integer().not_null())
                .col(ColumnDef::new(SleepCycles::MaxHrv).integer().not_null())
                .col(ColumnDef::new(SleepCycles::AvgHrv).integer().not_null())
                .col(ColumnDef::new(SleepCycles::Score).double().null())
                .col(
                    ColumnDef::new(SleepCycles::Synced)
                        .boolean()
                        .not_null()
                        .default(false),
                )
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .name("idx-sleep-cycles-device-sleep-id-unique")
                .table(SleepCycles::Table)
                .col(SleepCycles::DeviceId)
                .col(SleepCycles::SleepId)
                .unique()
                .to_owned(),
        )
        .await
}

async fn create_activities_table(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    manager
        .create_table(
            Table::create()
                .table(Activities::Table)
                .col(
                    ColumnDef::new(Activities::Id)
                        .integer()
                        .not_null()
                        .auto_increment()
                        .primary_key(),
                )
                .col(ColumnDef::new(Activities::DeviceId).string().not_null())
                .col(ColumnDef::new(Activities::PeriodId).date().not_null())
                .col(ColumnDef::new(Activities::Start).date_time().not_null())
                .col(ColumnDef::new(Activities::End).date_time().not_null())
                .col(ColumnDef::new(Activities::Activity).string_len(64).not_null())
                .col(
                    ColumnDef::new(Activities::Synced)
                        .boolean()
                        .not_null()
                        .default(false),
                )
                .to_owned(),
        )
        .await?;

    manager
        .create_index(
            Index::create()
                .name("idx-activities-device-start-unique")
                .table(Activities::Table)
                .col(Activities::DeviceId)
                .col(Activities::Start)
                .unique()
                .to_owned(),
        )
        .await
}

#[derive(Iden)]
enum Packets {
    Table,
    Id,
    DeviceId,
    Uuid,
    Bytes,
}

#[derive(Iden)]
enum LegacyPackets {
    #[iden = "packets_legacy"]
    Table,
}

#[derive(Iden)]
enum HeartRate {
    Table,
    Id,
    DeviceId,
    Bpm,
    Time,
    RrIntervals,
    Activity,
    Stress,
    Spo2,
    SkinTemp,
    ImuData,
    SensorData,
    Synced,
}

#[derive(Iden)]
enum LegacyHeartRate {
    #[iden = "heart_rate_legacy"]
    Table,
}

#[derive(Iden)]
enum SleepCycles {
    Table,
    Id,
    DeviceId,
    SleepId,
    Start,
    End,
    MinBpm,
    MaxBpm,
    AvgBpm,
    MinHrv,
    MaxHrv,
    AvgHrv,
    Score,
    Synced,
}

#[derive(Iden)]
enum LegacySleepCycles {
    #[iden = "sleep_cycles_legacy"]
    Table,
}

#[derive(Iden)]
enum Activities {
    Table,
    Id,
    DeviceId,
    PeriodId,
    Start,
    End,
    Activity,
    Synced,
}

#[derive(Iden)]
enum LegacyActivities {
    #[iden = "activities_legacy"]
    Table,
}
