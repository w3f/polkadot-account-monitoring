use crate::chain_api::{
    NominationsPage, Response, RewardSlash, RewardsSlashesPage, Transfer, TransfersPage, Validator,
};
use crate::{Context, ContextId, Result};
use bson::{doc, to_bson, to_document, Bson, Document};
use mongodb::options::UpdateOptions;
use mongodb::{Client, Database as MongoDb};
use serde::Serialize;
use std::borrow::Cow;

const COLL_TRANSFER_RAW: &'static str = "raw_transfers";
const COLL_REWARD_SLASH_RAW: &'static str = "raw_rewards_slashes";
const COLL_NOMINATIONS_RAW: &'static str = "raw_rewards_slashes";

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
    context_id: ContextId<'a>,
    data: Cow<'a, T>,
}

#[derive(Clone)]
pub struct Database {
    db: MongoDb,
}

impl Database {
    pub async fn new(uri: &str, db: &str) -> Result<Self> {
        Ok(Database {
            db: Client::with_uri_str(uri).await?.database(db),
        })
    }
    pub async fn store_transfer_event(
        &self,
        context: &Context,
        data: &Response<TransfersPage>,
    ) -> Result<usize> {
        let coll = self
            .db
            .collection::<ContextData<Transfer>>(COLL_TRANSFER_RAW);

        // Add the full context to each transfer, so the corresponding account
        // can be identified.
        let extrinsics: Vec<ContextData<Transfer>> = data
            .data
            .transfers
            .as_ref()
            .ok_or(anyhow!("No transfers found in response body"))?
            .iter()
            .map(|t| ContextData {
                context_id: context.id(),
                data: Cow::Borrowed(t),
            })
            .collect();

        // Insert new entries. Return count of how many were newly inserted.
        let mut count = 0;
        for extrinsic in &extrinsics {
            let res = coll
                .update_one(
                    doc! {
                        "context_id": context.id().to_bson()?,
                        "data.extrinsic_index": extrinsic.data.extrinsic_index.to_bson()?,
                    },
                    doc! {
                        "$setOnInsert": extrinsic.to_bson()?,
                    },
                    {
                        let mut opt = UpdateOptions::default();
                        opt.upsert = Some(true);
                        Some(opt)
                    },
                )
                .await?;

            assert_eq!(res.modified_count, 0);
            res.upserted_id.map(|_| {
                trace!(
                    "Added new transfer to database for {:?}: {:?}",
                    context,
                    extrinsic
                );
                count += 1;
            });
        }

        Ok(count)
    }
    pub async fn store_reward_slash_event(
        &self,
        context: &Context,
        data: &Response<RewardsSlashesPage>,
    ) -> Result<usize> {
        let coll = self
            .db
            .collection::<ContextData<RewardSlash>>(COLL_REWARD_SLASH_RAW);

        // Add the full context to each entry, so the corresponding account
        // can be identified.
        let reward_slashes: Vec<ContextData<RewardSlash>> = data
            .data
            .list
            .as_ref()
            .ok_or(anyhow!("No rewards/slashes found in response body"))?
            .iter()
            .map(|rs| ContextData {
                context_id: context.id(),
                data: Cow::Borrowed(rs),
            })
            .collect();

        // Insert new entries. Return count of how many were newly inserted.
        let mut count = 0;
        for reward_slash in &reward_slashes {
            let res = coll
                .update_one(
                    doc! {
                        "context_id": context.id().to_bson()?,
                        "data.extrinsic_hash": reward_slash.data.extrinsic_hash.to_bson()?,
                    },
                    doc! {
                        "$setOnInsert": reward_slash.to_bson()?,
                    },
                    {
                        let mut opt = UpdateOptions::default();
                        opt.upsert = Some(true);
                        Some(opt)
                    },
                )
                .await?;

            assert_eq!(res.modified_count, 0);
            res.upserted_id.map(|_| {
                trace!(
                    "Added new rewards_slash to database for {:?}: {:?}",
                    context,
                    reward_slash
                );
                count += 1;
            });
        }

        Ok(count)
    }
    pub async fn store_nomination_event(
        &self,
        context: &Context,
        data: &Response<NominationsPage>,
    ) -> Result<usize> {
        let coll = self
            .db
            .collection::<ContextData<Validator>>(COLL_NOMINATIONS_RAW);

        // Add the full context to each entry, so the corresponding account
        // can be identified.
        let validators: Vec<ContextData<Validator>> = data
            .data
            .list
            .as_ref()
            .ok_or(anyhow!("No nominations found in response body"))?
            .iter()
            .map(|v| ContextData {
                context_id: context.id(),
                data: Cow::Borrowed(v),
            })
            .collect();

        // Insert new entries. Return count of how many were newly inserted.
        let mut count = 0;
        for validator in &validators {
            let res = coll
                .update_one(
                    doc! {
                        "context_id": context.id().to_bson()?,
                        "data.validator_stash": validator.data.validator_stash.to_bson()?,
                    },
                    doc! {
                        "$setOnInsert": validator.to_bson()?,
                    },
                    {
                        let mut opt = UpdateOptions::default();
                        opt.upsert = Some(true);
                        Some(opt)
                    },
                )
                .await?;

            assert_eq!(res.modified_count, 0);
            res.upserted_id.map(|_| {
                trace!(
                    "Added new rewards_slash to database for {:?}: {:?}",
                    context,
                    validator
                );
                count += 1;
            });
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain_api::{Response, TransfersPage};
    use crate::tests::db;
    use crate::Context;

    #[tokio::test]
    async fn store_transfer_event() {
        let db = db().await;

        // Must now have an influence on data.
        let alice = Context::alice();
        let bob = Context::bob();

        // Gen test data
        let mut resp: Response<TransfersPage> = Default::default();
        resp.data.transfers = Some(vec![Default::default(); 10]);
        resp.data
            .transfers
            .as_mut()
            .unwrap()
            .iter_mut()
            .enumerate()
            .for_each(|(idx, t)| t.extrinsic_index = idx.to_string().into());

        // New data is inserted
        let count = db.store_transfer_event(&alice, &resp).await.unwrap();
        assert_eq!(count, 10);

        // No new data is inserted
        let count = db.store_transfer_event(&alice, &resp).await.unwrap();
        assert_eq!(count, 0);

        // Gen new test data
        let mut new_resp: Response<TransfersPage> = Default::default();
        new_resp.data.transfers = Some(vec![Default::default(); 15]);
        new_resp
            .data
            .transfers
            .as_mut()
            .unwrap()
            .iter_mut()
            .enumerate()
            .for_each(|(idx, t)| t.extrinsic_index = (idx + 10).to_string().into());

        // New data is inserted
        let count = db.store_transfer_event(&bob, &new_resp).await.unwrap();
        assert_eq!(count, 15);

        // No new data is inserted
        let count = db.store_transfer_event(&bob, &new_resp).await.unwrap();
        assert_eq!(count, 0);

        // Insert previous data (under a new context)
        let count = db.store_transfer_event(&bob, &resp).await.unwrap();
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn store_reward_slash_event() {
        let db = db().await;

        // Must now have an influence on data.
        let alice = Context::alice();
        let bob = Context::bob();

        // Gen test data
        let mut resp: Response<RewardsSlashesPage> = Default::default();
        resp.data.list = Some(vec![Default::default(); 10]);
        resp.data
            .list
            .as_mut()
            .unwrap()
            .iter_mut()
            .enumerate()
            .for_each(|(idx, e)| e.extrinsic_hash = idx.to_string().into());

        // New data is inserted
        let count = db.store_reward_slash_event(&alice, &resp).await.unwrap();
        assert_eq!(count, 10);

        // No new data is inserted
        let count = db.store_reward_slash_event(&alice, &resp).await.unwrap();
        assert_eq!(count, 0);

        // Gen new test data
        let mut new_resp: Response<RewardsSlashesPage> = Default::default();
        new_resp.data.list = Some(vec![Default::default(); 15]);
        new_resp
            .data
            .list
            .as_mut()
            .unwrap()
            .iter_mut()
            .enumerate()
            .for_each(|(idx, e)| e.extrinsic_hash = (idx + 10).to_string().into());

        // New data is inserted
        let count = db.store_reward_slash_event(&bob, &new_resp).await.unwrap();
        assert_eq!(count, 15);

        // No new data is inserted
        let count = db.store_reward_slash_event(&bob, &new_resp).await.unwrap();
        assert_eq!(count, 0);

        // Insert previous data (under a new context)
        let count = db.store_reward_slash_event(&bob, &resp).await.unwrap();
        assert_eq!(count, 10);
    }
}
