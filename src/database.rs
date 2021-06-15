use crate::chain_api::{
    Nomination, NominationsPage, Response, RewardSlash, RewardsSlashesPage, Transfer, TransfersPage,
};
use crate::{BlockNumber, Context, ContextId, Result, Timestamp};
use bson::{doc, to_bson, to_document, Bson, Document};
use futures::StreamExt;
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
    timestamp: Timestamp,
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
                timestamp: Timestamp::now(),
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
                timestamp: Timestamp::now(),
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
            .collection::<ContextData<Nomination>>(COLL_NOMINATIONS_RAW);

        // Add the full context to each entry, so the corresponding account
        // can be identified.
        let validators: Vec<ContextData<Nomination>> = data
            .data
            .list
            .as_ref()
            .ok_or(anyhow!("No nominations found in response body"))?
            .iter()
            .map(|v| ContextData {
                context_id: context.id(),
                timestamp: Timestamp::now(),
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
                        "data.stash_account_display.address": validator.data.stash_account_display.address.to_bson()?,
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
    #[cfg(test)]
    fn reader(&self) -> DatabaseReader {
        DatabaseReader {
            db: self.db.clone(),
        }
    }
}

#[derive(Clone)]
// TODO: Rename
pub struct DatabaseReader {
    db: MongoDb,
}

impl DatabaseReader {
    pub async fn new(uri: &str, db: &str) -> Result<Self> {
        Ok(DatabaseReader {
            db: Client::with_uri_str(uri).await?.database(db),
        })
    }
    pub async fn fetch_transfers<'a>(
        &self,
        contexts: &[&Context],
        from: Timestamp,
        to: Timestamp,
    ) -> Result<Vec<ContextData<'a, Transfer>>> {
        let coll = self
            .db
            .collection::<ContextData<Transfer>>(COLL_TRANSFER_RAW);

        let mut cursor = coll.find(doc!{
            "context_id": {
                "$in": contexts.iter().map(|c| c.id()).collect::<Vec<ContextId>>().to_bson()?,
            },
            "$and": [
                {
                    "data.block_timestamp": {
                        "$gte": from.to_bson()?
                    }
                },
                {
                    "data.block_timestamp": {
                        "$lte": to.to_bson()?
                    }
                }
            ]
        }, None).await?;

        let mut transfers = vec![];
        while let Some(doc) = cursor.next().await {
            transfers.push(doc?);
        }

        Ok(transfers)
    }
    pub async fn fetch_rewards_slashes<'a>(
        &self,
        contexts: &[&Context],
        from: BlockNumber,
        to: BlockNumber,
    ) -> Result<Vec<ContextData<'a, RewardSlash>>> {
        let coll = self
            .db
            .collection::<ContextData<RewardSlash>>(COLL_REWARD_SLASH_RAW);

        let mut cursor = coll.find(doc!{
            "context_id": {
                "$in": contexts.iter().map(|c| c.id()).collect::<Vec<ContextId>>().to_bson()?,
            },
            "$and": [
                {
                    "data.block_num": {
                        "$gte": from.to_bson()?
                    }
                },
                {
                    "data.block_num": {
                        "$lte": to.to_bson()?
                    }
                }
            ]
        }, None).await?;

        let mut rewards_slashes = vec![];
        while let Some(doc) = cursor.next().await {
            rewards_slashes.push(doc?);
        }

        Ok(rewards_slashes)
    }
    pub async fn fetch_nominations<'a>(
        &self,
        contexts: &[Context],
    ) -> Result<Vec<ContextData<'a, Nomination>>> {
        let coll = self
            .db
            .collection::<ContextData<Nomination>>(COLL_NOMINATIONS_RAW);

        let mut cursor = coll.find(doc!{
            "context_id": {
                "$in": contexts.iter().map(|c| c.as_str()).collect::<Vec<&str>>().to_bson()?,
            },
        }, None).await?;

        let mut validators = vec![];
        while let Some(doc) = cursor.next().await {
            validators.push(doc?);
        }

        Ok(validators)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain_api::{Response, TransfersPage};
    use crate::tests::{db, generator};
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

    #[tokio::test]
    async fn store_nomination_event() {
        let db = db().await;

        // Must now have an influence on data.
        let alice = Context::alice();
        let bob = Context::bob();

        // Gen test data
        let mut resp: Response<NominationsPage> = Default::default();
        resp.data.list = Some(vec![Default::default(); 10]);
        resp.data
            .list
            .as_mut()
            .unwrap()
            .iter_mut()
            .enumerate()
            .for_each(|(idx, e)| e.stash_account_display.address = idx.to_string().into());

        // New data is inserted
        let count = db.store_nomination_event(&alice, &resp).await.unwrap();
        assert_eq!(count, 10);

        // No new data is inserted
        let count = db.store_nomination_event(&alice, &resp).await.unwrap();
        assert_eq!(count, 0);

        // Gen new test data
        let mut new_resp: Response<NominationsPage> = Default::default();
        new_resp.data.list = Some(vec![Default::default(); 15]);
        new_resp
            .data
            .list
            .as_mut()
            .unwrap()
            .iter_mut()
            .enumerate()
            .for_each(|(idx, e)| e.stash_account_display.address = (idx + 10).to_string().into());

        // New data is inserted
        let count = db.store_nomination_event(&bob, &new_resp).await.unwrap();
        assert_eq!(count, 15);

        // No new data is inserted
        let count = db.store_nomination_event(&bob, &new_resp).await.unwrap();
        assert_eq!(count, 0);

        // Insert previous data (under a new context)
        let count = db.store_nomination_event(&bob, &resp).await.unwrap();
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn fetch_transfers() {
        let db = db().await;
        let report = db.reader();

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
            .for_each(|(idx, t)| {
                t.block_timestamp = Timestamp::from(idx as u64 * 100);
                t.extrinsic_index = idx.to_string().into();
            });

        // New data is inserted
        let _ = db.store_transfer_event(&alice, &resp).await.unwrap();

        // Fetch data
        let res = report
            .fetch_transfers(&[&alice], Timestamp::from(300), Timestamp::from(800))
            .await
            .unwrap();

        assert_eq!(
            res.iter()
                .map(|c| c.data.clone().into_owned())
                .collect::<Vec<Transfer>>()
                .as_slice(),
            &resp.data.transfers.unwrap()[3..9]
        );

        // Fetch data (invalid)
        let res = report
            .fetch_transfers(&[&bob], Timestamp::from(300), Timestamp::from(800))
            .await
            .unwrap();

        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn fetch_rewards_slashes() {
        let db = db().await;
        let report = db.reader();

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
            .for_each(|(idx, t)| {
                t.block_num = BlockNumber::from(idx as u64 * 100);
                t.extrinsic_hash = idx.to_string().into();
            });

        // New data is inserted
        let _ = db.store_reward_slash_event(&alice, &resp).await.unwrap();

        // Fetch data
        let res = report
            .fetch_rewards_slashes(&[&alice], BlockNumber::from(300), BlockNumber::from(800))
            .await
            .unwrap();

        assert_eq!(
            res.iter()
                .map(|c| c.data.clone().into_owned())
                .collect::<Vec<RewardSlash>>()
                .as_slice(),
            &resp.data.list.unwrap()[3..9]
        );

        // Fetch data (invalid)
        let res = report
            .fetch_rewards_slashes(&[&bob], BlockNumber::from(300), BlockNumber::from(800))
            .await
            .unwrap();

        assert!(res.is_empty());
    }
}
