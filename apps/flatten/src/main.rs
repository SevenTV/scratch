use std::{collections::HashSet, sync::atomic::AtomicUsize};

use bson::to_bson;
use clap::Parser;
use common::{Backend, BackendType, Direction, Query};
use entitlements::data::EdgeKind;
use futures::{stream::FuturesUnordered, StreamExt};
use mongodb::{options::{IndexOptions, InsertManyOptions}, Database, IndexModel};
use serde::{Deserialize, Serialize};
use typesense::{apis::{collections_api::{CreateCollectionError, DeleteCollectionError}, configuration::ApiKey, documents_api::{DeleteDocumentError, ImportDocumentsError}}, models::{CollectionResponse, CollectionSchema, ImportDocumentsImportDocumentsParametersParameter}};
use ulid::Ulid;

const CONCURRENCY: usize = 1000;

#[derive(Debug, clap::Parser)]
struct Args {
    #[clap(long, default_value = "info")]
    loggging: String,
    #[clap(long)]
    load_data: bool,
    #[clap(long)]
    query: Option<Query<EdgeKind>>,
    #[clap(long)]
    query_count: Option<usize>,
    #[clap(long, default_value = "mongo")]
    backend: BackendType,
}

struct TypesenseClient(typesense::apis::configuration::Configuration);  

impl TypesenseClient {
    async fn create_collection(&self, schema: CollectionSchema) -> Result<CollectionResponse, typesense::apis::Error<CreateCollectionError>> {
        typesense::apis::collections_api::create_collection(&self.0, schema).await
    }

    async fn drop_collection(&self, collection_name: &str)-> Result<CollectionResponse, typesense::apis::Error<DeleteCollectionError>> {
        typesense::apis::collections_api::delete_collection(&self.0, collection_name).await
    }

    async fn write_documents<'a, T: serde::Serialize + 'a>(&self, collection_name: &str, action: &str, document: impl IntoIterator<Item = T>) -> Result<String, typesense::apis::Error<ImportDocumentsError>>  {
        let mut body = "".to_string();

        document.into_iter().for_each(|doc| {
            body.push_str(&serde_json::to_string(&doc).unwrap());
            body.push_str("\n");
        });

        typesense::apis::documents_api::import_documents(&self.0, collection_name, body, Some(ImportDocumentsImportDocumentsParametersParameter {
            action: Some(action.to_string()),
            ..Default::default()
        })).await
    }

    async fn delete_document(&self, collection_name: &str, document_id: &str) -> Result<serde_json::Value, typesense::apis::Error<DeleteDocumentError>> {
        typesense::apis::documents_api::delete_document(&self.0, collection_name, document_id).await
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    std::env::set_var(
        "RUST_LOG",
        std::env::var("RUST_LOG").unwrap_or_else(|_| args.loggging.clone()),
    );

    tracing_subscriber::fmt::init();

    tracing::info!("Using MongoDB backend");

    let client = mongodb::Client::with_uri_str("mongodb://localhost:27111")
        .await
        .expect("client");

    let typesense = TypesenseClient(typesense::apis::configuration::Configuration {
        api_key: Some(ApiKey {
            key: "xyz".to_string(),
            prefix: None,
        }),
        base_path: "http://localhost:8108".to_string(),
        ..Default::default()
    });
    
    handle_backend(client.database("7tv_entitlements_graph"), typesense).await;
}

#[derive(Debug, Serialize, Deserialize)]
struct User {
    #[serde(rename = "_id")]
    id: String,
    username: String,
    active_emote_set_id: Option<String>,
    search_index: UserSearchIndex,
}

#[derive(Debug, Serialize, Deserialize)]
struct TypesenseUser {
    id: String,
    role_rank: i32,
    username: String,
    emotes: Vec<String>,
    entitlements: Vec<entitlements::data::EdgeKind>,
}

#[derive(Debug, Serialize, Deserialize)]
struct UserSearchIndex {
    role_rank: i32,
    self_dirty: Option<Ulid>,
    emotes_dirty: Option<Ulid>,
    entitlements_dirty: Option<Ulid>,
    emotes: Vec<String>,
    entitlements_cache_keys: Vec<EntitlementCacheKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EntitlementCacheKey {
    Role {
        id: String,
    },
    Product {
        id: String,
    },
    GiftReward {
        id: String,
    },
    SubscriptionTimeline {
        id: String,
    },
    SubscriptionTimelinePeriod {
        subscription_timeline_id: String,
        period_id: String,
    },
}

impl std::fmt::Display for EntitlementCacheKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EntitlementCacheKey::Role { id } => write!(f, "role:{}", id),
            EntitlementCacheKey::Product { id } => write!(f, "product:{}", id),
            EntitlementCacheKey::GiftReward { id } => write!(f, "gift-reward:{}", id),
            EntitlementCacheKey::SubscriptionTimeline { id } => {
                write!(f, "subscription-timeline:{}", id)
            }
            EntitlementCacheKey::SubscriptionTimelinePeriod {
                subscription_timeline_id,
                period_id,
            } => {
                write!(
                    f,
                    "subscription-timeline-period:{}:{}",
                    subscription_timeline_id, period_id
                )
            }
        }
    }
}

impl std::str::FromStr for EntitlementCacheKey {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        match parts.as_slice() {
            ["role", id] => Ok(EntitlementCacheKey::Role { id: id.to_string() }),
            ["product", id] => Ok(EntitlementCacheKey::Product { id: id.to_string() }),
            ["gift-reward", id] => Ok(EntitlementCacheKey::GiftReward { id: id.to_string() }),
            ["subscription-timeline", id] => {
                Ok(EntitlementCacheKey::SubscriptionTimeline { id: id.to_string() })
            }
            ["subscription-timeline-period", subscription_timeline_id, period_id] => {
                Ok(EntitlementCacheKey::SubscriptionTimelinePeriod {
                    subscription_timeline_id: subscription_timeline_id.to_string(),
                    period_id: period_id.to_string(),
                })
            }
            _ => Err(format!("invalid edge kind: {}", s)),
        }
    }
}

impl Serialize for EntitlementCacheKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for EntitlementCacheKey {
    fn deserialize<D>(deserializer: D) -> Result<EntitlementCacheKey, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct EmoteSetOriginEmote {
    id: String,
    alias: String,
    added_at: chrono::DateTime<chrono::Utc>,
    added_by_id: String,
    set_id: String,
}

#[derive(Debug, Serialize, Deserialize)]

struct EmoteSet {
    #[serde(rename = "_id")]
    id: String,
    name: String,
    origin_config: EmoteSetOriginConfig,
    emotes: Vec<EmoteSetEmote>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
enum EmoteSetOriginError {
    CycleDetected(String),
    MaxDepthExceeded(String),
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct EmoteSetOriginConfig {
    origins: Vec<EmoteSetOrigin>,
    // Specify a limit on the number of emotes imported.
    limit: usize,
    // A list of emotes that are removed from every upstream set before computing the final set.
    purge: Vec<EmoteSetOriginPurge>,
    auto_resync: bool,
    needs_resync: bool,
    // Cycle was detected in the origin configuration, on this emote set id.
    error: Option<EmoteSetOriginError>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
enum EmoteSetOriginPurge {
    Alias(String),
    Id(String),
}

#[derive(Debug, Serialize, Deserialize)]
struct EmoteSetEmote {
    emote_id: String,
    alias: String,
    added_at: chrono::DateTime<chrono::Utc>,
    added_by_id: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    origin_set_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct EmoteSetOrigin {
    id: String,
    limit: Option<EmoteSetLimit>,
    transformations: Vec<EmoteSetOriginTransformation>,
}

#[derive(Debug, Serialize, Deserialize)]
struct EmoteSetLimit {
    count: usize,
    order: LimitOrder,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum EmoteSetOriginTransformation {
    Remove {
        id: String,
    },
    Replace {
        id: String,
        with: String,
    },
    Rename {
        id: String,
        alias: String,
    },
    RenameAlias {
        old: String,
        alias: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LimitOrder {
    AddedAtAcs,
    AddedAtDesc,
    UsageAcs,
    UsageDesc,
}

async fn handle_backend(backend: Database, typesense: TypesenseClient) {
    handle_users(&backend, &typesense).await;
    handle_entitlements(&backend, &typesense).await;
    handle_emote_sets(&backend, &typesense).await;
}

async fn handle_users(backend: &Database, typesense: &TypesenseClient) {
    tracing::info!("inserting users");

    let (entitlement_data, _) = entitlements::binary::load_cache();

    let users = entitlement_data
        .users
        .iter()
        // .filter(|user| user.id == "6054406ab4d31e459fb387b0")
        .map(|user| User {
            id: user.id.to_owned(),
            active_emote_set_id: None,
            username: user.username.to_owned(),
            search_index: UserSearchIndex {
                role_rank: 0,
                self_dirty: None,
                emotes_dirty: None,
                entitlements_dirty: Some(Ulid::new()),
                emotes: vec![],
                entitlements_cache_keys: vec![],
            },
        })
        .collect::<Vec<_>>();

    let user_collection = backend.collection::<User>("users");

    user_collection.drop(None).await.expect("drop users");

    user_collection.create_index(
        IndexModel::builder()
            .keys(mongodb::bson::doc! {
                "search_index.self_dirty": 1
            })
            .build(),
        None,
    ).await.expect("create index");

    user_collection.create_index(
        IndexModel::builder()
            .keys(mongodb::bson::doc! {
                "search_index.emotes_dirty": 1
            })
            .build(),
        None,
    ).await.expect("create index");

    user_collection.create_index(
        IndexModel::builder()
            .keys(mongodb::bson::doc! { 
                "search_index.entitlements_dirty": 1
            })
            .build(),
        None,
    ).await.expect("create index");

    user_collection.create_index(
        IndexModel::builder()
            .keys(mongodb::bson::doc! { 
                "search_index.emotes": 1
            })
            .build(),
        None,
    ).await.expect("create index");

    user_collection.create_index(
        IndexModel::builder()
            .keys(mongodb::bson::doc! { 
                "search_index.entitlements_cache_keys": 1
            })
            .build(),
        None,
    ).await.expect("create index");

    user_collection.insert_many(
        &users,
        Some(InsertManyOptions::builder().ordered(false).build()),
    )
    .await
    .expect("insert users");

    tracing::info!("inserted {} users to mongodb", users.len());

    typesense.drop_collection("users").await.ok();
    
    typesense.create_collection(CollectionSchema {
        name: "users".to_string(),
        fields: vec![
            typesense::models::Field {
                name: "id".to_string(),
                r#type: "string".to_string(),
                ..Default::default()
            },
            typesense::models::Field {
                name: "role_rank".to_string(),
                r#type: "int32".to_string(),
                ..Default::default()
            },
            typesense::models::Field {
                name: "username".to_string(),
                r#type: "string".to_string(),
                ..Default::default()
            },
            typesense::models::Field {
                name: "emotes".to_string(),
                r#type: "string[]".to_string(),
                ..Default::default()
            },
            typesense::models::Field {
                name: "entitlements".to_string(),
                r#type: "string[]".to_string(),
                ..Default::default()
            },
        ],
        default_sorting_field: Some("role_rank".to_string()),
        token_separators: None,
        enable_nested_fields: None,
        symbols_to_index: Some(vec!["_".to_string()])
    }).await.expect("create users collection");
    
    typesense.write_documents("users", "upsert", users.iter().map(|user| TypesenseUser {
        id: user.id.to_owned(),
        role_rank: 0,
        username: user.username.clone(),
        emotes: vec![],
        entitlements: vec![],
    })).await.expect("write users to typesense");
}

#[derive(Debug, Serialize, Deserialize)]
struct UserEmote {
    user_id: String,
    emote_id: String,
}

async fn handle_entitlements(backend: &Database, typesense: &TypesenseClient) {
    let semaphore = tokio::sync::Semaphore::new(CONCURRENCY);

    let completed = AtomicUsize::new(0);

    let user_collection = backend.collection::<User>("users");

    let mut users = user_collection.find(Some(bson::doc! {
        "search_index.entitlements_dirty": { "$ne": None::<i32> },
    }), None).await.expect("find users");

    let mut handles = FuturesUnordered::new();

    let role_ranks = &[
        "6076a99409a4c63a38ebe802",
        "6076a86b09a4c63a38ebe801",
        "60b3f1ea886e63449c5263b1",
        "612c888812a39cc5cdd82ae0",
        "60724f65e93d828bf8858789",
        "631ef5ea03e9beb96f849a7e",
        "6102002eab1aa12bf648cfcd",
        "608831312a61f51b61f2974d",
    ];

    let mut typesense_updates = Vec::new();

    loop {
        tokio::select! {
            Some(user) = users.next() => {
                let user = user.expect("user");
                let backend = &backend;
                let semaphore = &semaphore;
                let completed = &completed;
                let user_collection = &user_collection;

                handles.push(async move {
                    if user.search_index.entitlements_dirty.is_none() {
                        return None;
                    }

                    let _permit = semaphore.acquire().await.unwrap();

                    let edges = backend
                        .traversal(
                            Direction::Outbound,
                            EdgeKind::User {
                                id: user.id.clone(),
                            },
                        )
                        .await
                        .into_iter()
                        .map(|edge| edge.to)
                        .collect::<Vec<_>>();

                    let cache_keys = edges
                        .iter()
                        .cloned()
                        .filter_map(|edge| match edge {
                            EdgeKind::Role { id } => Some(EntitlementCacheKey::Role { id }),
                            EdgeKind::Product { id } => Some(EntitlementCacheKey::Product { id }),
                            EdgeKind::GiftReward { id } => Some(EntitlementCacheKey::GiftReward { id }),
                            EdgeKind::UserSubscriptionTimeline {
                                subscription_timeline_id: id,
                                ..
                            } => Some(EntitlementCacheKey::SubscriptionTimeline { id }),
                            EdgeKind::SubscriptionTimelinePeriod {
                                subscription_timeline_id,
                                period_id,
                            } => Some(EntitlementCacheKey::SubscriptionTimelinePeriod {
                                subscription_timeline_id,
                                period_id,
                            }),
                            _ => None,
                        })
                        .collect::<HashSet<_>>();

                        let role_rank = role_ranks.iter().enumerate().rev().find(|(_, id)| cache_keys.contains(&EntitlementCacheKey::Role { id: id.to_string() })).map(|(idx, _)| idx + 1).unwrap_or(0) as i32;

                        user_collection
                            .update_one(
                                mongodb::bson::doc! { 
                                    "_id": user.id.clone(),
                                },
                                mongodb::bson::doc! { "$set": {
                                    "search_index.role_rank": role_rank,
                                    "search_index.entitlements_dirty": None::<i32>,
                                    "search_index.entitlements_cache_keys": to_bson(&cache_keys).unwrap(),
                                }},
                                None,
                            )
                            .await
                            .expect("update user");

                    let completed = completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if completed % 10000 == 0 {
                        tracing::info!("completed {} users entitlements", completed);
                    }

                    Some(serde_json::json!({
                        "id": user.id.clone(),
                        "role_rank": role_rank,
                        "entitlements": edges,
                    }))
                });
            }
            Some(update) = handles.next() => if let Some(update) = update {
                typesense_updates.push(update);
            },
            else => break,
        }
    }

    tracing::info!("completed {} users", completed.load(std::sync::atomic::Ordering::Relaxed));

    typesense.write_documents("users", "update", typesense_updates).await.expect("write users to typesense");

    tracing::info!("completed {} users typesense update", completed.load(std::sync::atomic::Ordering::Relaxed));
}

async fn handle_emote_sets(backend: &Database, typesense: &TypesenseClient) {
    let (emote_data, _) = emotes::binary::load_cache();

    let emote_sets = emote_data
        .emote_sets
        .iter()
        .map(|emote_set| EmoteSet {
            id: emote_set.id.to_owned(),
            name: emote_set.name.to_owned(),
            origin_config: Default::default(),
            emotes: {
                let mut id_hash_set = HashSet::new();
                let mut alias_hash_set = HashSet::new();

                emote_set.emotes.iter().filter(|emote| {
                    let id = emote.id.to_owned();
                    let alias = emote.name.to_owned();

                    if id_hash_set.contains(&id) || alias_hash_set.contains(&alias) {
                        return false;
                    }

                    id_hash_set.insert(id);
                    alias_hash_set.insert(alias);

                    true
                }).map(|emote| EmoteSetEmote {
                    emote_id: emote.id.to_owned(),
                    alias: emote.name.to_owned(),
                    added_at: chrono::Utc::now(),
                    added_by_id: "system".to_owned(),
                    origin_set_id: None,
                }).collect::<Vec<_>>()
            }
        });

    let emote_set_collection = backend.collection::<EmoteSet>("emote_sets");
    emote_set_collection.drop(None).await.expect("drop emote sets");

    emote_set_collection.create_index(
        IndexModel::builder()
            .keys(mongodb::bson::doc! {
                "_id": 1,
                "emotes.id": 1,
            })
            .options(IndexOptions::builder().unique(true).build())
            .build(),
        None,
    ).await.expect("create index");

    emote_set_collection.create_index(
        IndexModel::builder()
            .keys(mongodb::bson::doc! {
                "_id": 1,
                "emotes.alias": 1,
            })
            .options(IndexOptions::builder().unique(true).build())
            .build(),
        None,
    ).await.expect("create index");

    emote_set_collection.create_index(
        IndexModel::builder()
            .keys(mongodb::bson::doc! {
                "emotes.id": 1
            })
            .build(),
        None,
    ).await.expect("create index");

    emote_set_collection.create_index(
        IndexModel::builder()
            .keys(mongodb::bson::doc! {
                "origin_config.auto_resync": 1,
                "origin_config.origins.id": 1,
            })
            .build(),
        None,
    ).await.expect("create index");

    let r = emote_set_collection.insert_many(
        emote_sets,
        Some(InsertManyOptions::builder().ordered(false).build()),
    ).await.expect("insert emote sets");

    tracing::info!("inserted {} emote sets", r.inserted_ids.len());

    let user_collection = backend.collection::<User>("users");

    let semaphore = tokio::sync::Semaphore::new(CONCURRENCY);
    let completed = AtomicUsize::new(0);

    let futures = emote_data.users.iter().filter_map(|user| {
        let user_collection = &user_collection;
        let emote_set_collection = &emote_set_collection;

        let semaphore = &semaphore;
        let completed = &completed;

        let user_id = user.id.to_owned();
        let emote_set_id = user.active_emote_set_ids.first()?.to_string();

        Some(async move {
            let _permit = semaphore.acquire().await.unwrap();

            let emote_set = emote_set_collection.find_one(Some(bson::doc! {
                "_id": &emote_set_id
            }), None).await.expect("find emote set").expect("emote set");

            let emotes = emote_set.emotes.iter().map(|emote| &emote.emote_id).collect::<Vec<_>>();

            user_collection
                .update_one(
                    mongodb::bson::doc! { 
                        "_id": &user_id,
                    },
                    mongodb::bson::doc! { "$set": {
                        "active_emote_set_id": &emote_set_id,
                        "search_index.emotes_dirty": None::<i32>,
                        "search_index.emotes": &emotes,
                    }},
                    None,
                )
                .await
                .expect("update user");

            let completed = completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if completed % 10000 == 0 {
                tracing::info!("completed {} users emotes", completed);
            }

            serde_json::json!({
                "id": user_id,
                "emotes": &emotes,
            })
        })
    });

    let typesense_updates = futures::future::join_all(futures).await;

    tracing::info!("completed {} users emotes", completed.load(std::sync::atomic::Ordering::Relaxed));

    typesense.write_documents("users", "update", typesense_updates).await.expect("write users to typesense");

    tracing::info!("completed {} users typesense update", completed.load(std::sync::atomic::Ordering::Relaxed));
}
