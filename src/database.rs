use crate::chain_api::{Extrinsic, ExtrinsicsPage, Response, RewardSlash, RewardsSlashesPage};
use crate::{Context, Result};
use bson::{doc, from_document, to_bson, to_document, Bson, Document};
use mongodb::{Client, Database as MongoDb};
use serde::Serialize;
use std::borrow::Cow;

const EXTRINSIC_EVENTS_RAW: &'static str = "events_transfer_raw";
const REWARD_SLASH_EVENTS_RAW: &'static str = "events_transfer_raw";

/// Convenience trait. Converts a value to BSON.
trait ToBson {
    fn to_bson(&self) -> Result<Bson>;
    fn to_document(&self) -> Result<Document>;
}

impl<T: Serialize> ToBson for T {
    fn to_bson(&self) -> Result<Bson> {
        Ok(to_bson(self)?)
    }
    fn to_document(&self) -> Result<Document> {
        Ok(to_document(self)?)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ContextData<'a, T: Clone> {
    context: Cow<'a, Context>,
    data: Cow<'a, T>,
}

pub struct Database {
    db: MongoDb,
}

impl Database {
    pub async fn new(uri: &str, db: &str) -> Result<Self> {
        let db = Client::with_uri_str(uri).await?.database(db);

        // Currently, the Rust MongoDb driver does not support indexing
        // natively. This is the current workaround. See
        // https://github.com/mongodb/mongo-rust-driver/pull/188
        db.run_command(
            doc! {
                "createIndexes": EXTRINSIC_EVENTS_RAW.to_bson()?,
                "indexes": [
                    {
                        "key": { "data.extrinsic_hash": 1 },
                        "unique": true
                    },
                ]
            },
            None,
        )
        .await?;

        db.run_command(
            doc! {
                "createIndexes": REWARD_SLASH_EVENTS_RAW.to_bson()?,
                "indexes": [
                    {
                        "key": { "data.extrinsic_hash": 1 },
                        "unique": true
                    },
                ]
            },
            None,
        )
        .await?;

        Ok(Database { db: db })
    }
    pub async fn store_extrinsic_event(
        &self,
        context: &Context,
        data: &Response<ExtrinsicsPage>,
    ) -> Result<usize> {
        let coll = self
            .db
            .collection::<ContextData<Extrinsic>>(EXTRINSIC_EVENTS_RAW);

        // Add the full context to each transfer, so the corresponding account
        // can be identified.
        let extrinsics: Vec<ContextData<Extrinsic>> = data
            .data
            .extrinsics
            .iter()
            .map(|t| ContextData {
                context: Cow::Borrowed(context),
                data: Cow::Borrowed(t),
            })
            .collect();

        // Insert new events. Return count of how many were updates (note that
        // extrinsic hashes have an unique constraint).
        Ok(coll.insert_many(extrinsics, None).await?.inserted_ids.len())
    }
    pub async fn store_reward_slash_event(
        &self,
        context: &Context,
        data: &Response<RewardsSlashesPage>,
    ) -> Result<usize> {
        let coll = self
            .db
            .collection::<ContextData<RewardSlash>>(REWARD_SLASH_EVENTS_RAW);

        // Add the full context to each transfer, so the corresponding account
        // can be identified.
        let reward_slashes: Vec<ContextData<RewardSlash>> = data
            .data
            .list
            .iter()
            .map(|rs| ContextData {
                context: Cow::Borrowed(context),
                data: Cow::Borrowed(rs),
            })
            .collect();

        // Insert new events. Return count of how many were updates (note that
        // extrinsic hashes have an unique constraint).
        Ok(coll
            .insert_many(reward_slashes, None)
            .await?
            .inserted_ids
            .len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Context;
    use crate::chain_api::{ExtrinsicHash, ExtrinsicsPage, Response};
    use rand::{thread_rng, Rng};

    #[tokio::test]
    async fn store_extrinsic_event() {
        let random: u32 = thread_rng().gen_range(u32::MIN..u32::MAX);
        let db = Database::new(
            "mongodb://localhost:27017/",
            &format!("monitoring_test_{}", random),
        )
        .await
        .unwrap();

        let alice = Context::alice();

        // Gen test data
        let mut resp: Response<ExtrinsicsPage> = Default::default();
        resp.data.extrinsics = vec![Default::default(); 10];
        resp.data.extrinsics
            .iter_mut()
            .enumerate()
            .for_each(|(idx, e)| e.extrinsic_hash = idx.to_string().into());

        // New data is inserted
        let count = db.store_extrinsic_event(&alice, &resp).await.unwrap();
        assert_eq!(count, resp.data.extrinsics.len());

        // No new data is inserted (unique constraint)
        let count = db.store_extrinsic_event(&alice, &resp).await.unwrap();
        assert_eq!(count, 0);
    }
}
