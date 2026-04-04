use std::str::FromStr;

use openwhoop_entities::activities;
use openwhoop_types::activities::{ActivityPeriod, ActivityType, SearchActivityPeriods};
use sea_orm::{
    ColumnTrait, Condition, EntityTrait, NotSet, QueryFilter, QueryOrder, Set,
    sea_query::OnConflict,
};

use crate::DatabaseHandler;

impl DatabaseHandler {
    pub async fn create_activity(&self, activity: ActivityPeriod) -> anyhow::Result<()> {
        let model = activities::ActiveModel {
            id: NotSet,
            device_id: Set(self.require_device_id()?.to_string()),
            period_id: Set(activity.period_id),
            start: Set(activity.from),
            end: Set(activity.to),
            activity: Set(activity.activity.to_string()),
            synced: NotSet,
        };

        activities::Entity::insert(model)
            .on_conflict(
                OnConflict::columns([activities::Column::DeviceId, activities::Column::Start])
                    .update_column(activities::Column::End)
                    .update_column(activities::Column::Activity)
                    .to_owned(),
            )
            .exec(&self.db)
            .await?;

        Ok(())
    }
    pub async fn search_activities(
        &self,
        options: SearchActivityPeriods,
    ) -> anyhow::Result<Vec<ActivityPeriod>> {
        let activities = activities::Entity::find()
            .filter(search_activity_periods_query(options))
            .filter(self.device_filter(activities::Column::DeviceId))
            .all(&self.db)
            .await?
            .into_iter()
            .map(map_activity_period)
            .collect();

        Ok(activities)
    }

    pub async fn get_latest_activity(&self) -> anyhow::Result<Option<ActivityPeriod>> {
        Ok(activities::Entity::find()
            .filter(self.device_filter(activities::Column::DeviceId))
            .order_by_desc(activities::Column::End)
            .one(&self.db)
            .await?
            .map(map_activity_period))
    }
}

fn map_activity_period(value: activities::Model) -> ActivityPeriod {
    ActivityPeriod {
        period_id: value.period_id,
        from: value.start,
        to: value.end,
        activity: ActivityType::from_str(value.activity.as_str()).unwrap(),
    }
}

fn search_activity_periods_query(query: SearchActivityPeriods) -> Condition {
    Condition::all()
        .add_option(query.from.map(|from| activities::Column::Start.gt(from)))
        .add_option(query.to.map(|to| activities::Column::End.lt(to)))
        .add_option(
            query
                .activity
                .map(|activity| activities::Column::Activity.eq(activity.to_string())),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, Timelike};

    fn make_activity(hour: u32) -> ActivityPeriod {
        let base = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        ActivityPeriod {
            period_id: base,
            from: base.and_hms_opt(hour, 0, 0).unwrap(),
            to: base.and_hms_opt(hour + 1, 0, 0).unwrap(),
            activity: ActivityType::Running,
        }
    }

    #[test]
    fn map_activity_period_converts() {
        let model = activities::Model {
            id: 1,
            device_id: "device-a".to_string(),
            period_id: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            start: NaiveDate::from_ymd_opt(2025, 1, 1)
                .unwrap()
                .and_hms_opt(8, 0, 0)
                .unwrap(),
            end: NaiveDate::from_ymd_opt(2025, 1, 1)
                .unwrap()
                .and_hms_opt(9, 0, 0)
                .unwrap(),
            activity: "Running".to_string(),
            synced: false,
        };
        let period = map_activity_period(model);
        assert!(matches!(period.activity, ActivityType::Running));
    }

    #[test]
    fn search_query_no_filters() {
        let query = SearchActivityPeriods::default();
        let _ = search_activity_periods_query(query);
    }

    #[test]
    fn search_query_with_activity_filter() {
        let query = SearchActivityPeriods::default().with_activity(ActivityType::Running);
        let _ = search_activity_periods_query(query);
    }

    #[tokio::test]
    async fn create_and_search_activities() {
        let db = DatabaseHandler::new("sqlite::memory:")
            .await
            .with_device_id(Some("device-a".to_string()));

        // Must create a sleep cycle first (FK constraint)
        let sleep_date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let sleep = openwhoop_algos::SleepCycle {
            id: sleep_date,
            start: sleep_date.and_hms_opt(22, 0, 0).unwrap(),
            end: NaiveDate::from_ymd_opt(2025, 1, 2)
                .unwrap()
                .and_hms_opt(6, 0, 0)
                .unwrap(),
            min_bpm: 50,
            max_bpm: 70,
            avg_bpm: 60,
            min_hrv: 30,
            max_hrv: 80,
            avg_hrv: 55,
            score: 100.0,
        };
        db.create_sleep(sleep).await.unwrap();

        let activity = make_activity(8);
        db.create_activity(activity).await.unwrap();

        let results = db
            .search_activities(SearchActivityPeriods::default())
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0].activity, ActivityType::Running));
    }

    #[tokio::test]
    async fn get_latest_activity_empty() {
        let db = DatabaseHandler::new("sqlite::memory:")
            .await
            .with_device_id(Some("device-a".to_string()));
        assert!(db.get_latest_activity().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn get_latest_activity_returns_most_recent() {
        let db = DatabaseHandler::new("sqlite::memory:")
            .await
            .with_device_id(Some("device-a".to_string()));

        let sleep_date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        let sleep = openwhoop_algos::SleepCycle {
            id: sleep_date,
            start: sleep_date.and_hms_opt(22, 0, 0).unwrap(),
            end: NaiveDate::from_ymd_opt(2025, 1, 2)
                .unwrap()
                .and_hms_opt(6, 0, 0)
                .unwrap(),
            min_bpm: 50,
            max_bpm: 70,
            avg_bpm: 60,
            min_hrv: 30,
            max_hrv: 80,
            avg_hrv: 55,
            score: 100.0,
        };
        db.create_sleep(sleep).await.unwrap();

        db.create_activity(make_activity(8)).await.unwrap();
        db.create_activity(make_activity(14)).await.unwrap();

        let latest = db.get_latest_activity().await.unwrap().unwrap();
        assert_eq!(latest.from.hour(), 14);
    }
}
