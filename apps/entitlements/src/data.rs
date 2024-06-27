use std::hash::Hash;

use common::{Backend, Edge, EdgeDestination};
use serde::{Deserialize, Serialize};

use crate::binary::{self, HeapBinaryData};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EdgeKind {
    User {
        id: String,
    },
    Role {
        id: String,
    },
    Badge {
        id: String,
    },
    Paint {
        id: String,
    },
    EmoteSet {
        id: String,
    },
    Product {
        id: String,
    },
    GiftReward {
        id: String,
    },
    UserSubscriptionTimeline {
        subscription_timeline_id: String,
        user_id: String,
    },
    SubscriptionTimelinePeriod {
        subscription_timeline_id: String,
        period_id: String,
    },
}

impl std::fmt::Display for EdgeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EdgeKind::User { id } => write!(f, "user:{}", id),
            EdgeKind::Role { id } => write!(f, "role:{}", id),
            EdgeKind::Badge { id } => write!(f, "badge:{}", id),
            EdgeKind::Paint { id } => write!(f, "paint:{}", id),
            EdgeKind::EmoteSet { id } => write!(f, "emote-set:{}", id),
            EdgeKind::Product { id } => write!(f, "product:{}", id),
            EdgeKind::GiftReward { id } => write!(f, "gift-reward:{}", id),
            EdgeKind::UserSubscriptionTimeline {
                user_id,
                subscription_timeline_id,
            } => {
                write!(
                    f,
                    "user-subscription-timeline:{}:{}",
                    user_id, subscription_timeline_id
                )
            }
            EdgeKind::SubscriptionTimelinePeriod {
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

impl std::str::FromStr for EdgeKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        match parts.as_slice() {
            ["user", id] => Ok(EdgeKind::User { id: id.to_string() }),
            ["role", id] => Ok(EdgeKind::Role { id: id.to_string() }),
            ["badge", id] => Ok(EdgeKind::Badge { id: id.to_string() }),
            ["paint", id] => Ok(EdgeKind::Paint { id: id.to_string() }),
            ["emote-set", id] => Ok(EdgeKind::EmoteSet { id: id.to_string() }),
            ["product", id] => Ok(EdgeKind::Product { id: id.to_string() }),
            ["gift-reward", id] => Ok(EdgeKind::GiftReward { id: id.to_string() }),
            ["user-subscription-timeline", user_id, subscription_timeline_id] => {
                Ok(EdgeKind::UserSubscriptionTimeline {
                    user_id: user_id.to_string(),
                    subscription_timeline_id: subscription_timeline_id.to_string(),
                })
            }
            ["subscription-timeline-period", subscription_timeline_id, period_id] => {
                Ok(EdgeKind::SubscriptionTimelinePeriod {
                    subscription_timeline_id: subscription_timeline_id.to_string(),
                    period_id: period_id.to_string(),
                })
            }
            _ => Err(format!("invalid edge kind: {}", s)),
        }
    }
}

impl Serialize for EdgeKind {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for EdgeKind {
    fn deserialize<D>(deserializer: D) -> Result<EdgeKind, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl common::EdgeKind for EdgeKind {
    fn has_inbound(&self) -> bool {
        matches!(
            self,
            EdgeKind::UserSubscriptionTimeline { .. }
                | EdgeKind::GiftReward { .. }
                | EdgeKind::Product { .. }
                | EdgeKind::SubscriptionTimelinePeriod { .. }
                | EdgeKind::Badge { .. }
                | EdgeKind::Paint { .. }
                | EdgeKind::EmoteSet { .. }
                | EdgeKind::Role { .. }
        )
    }

    fn has_outbound(&self) -> bool {
        matches!(
            self,
            EdgeKind::User { .. }
                | EdgeKind::Role { .. }
                | EdgeKind::Product { .. }
                | EdgeKind::GiftReward { .. }
                | EdgeKind::SubscriptionTimelinePeriod { .. }
                | EdgeKind::UserSubscriptionTimeline { .. }
        )
    }
}

// Create data using JSON.
pub async fn create_data(backend: &impl Backend<EdgeKind>, data: &HeapBinaryData) {
    create_user_edges(backend, data).await;
    create_role_edges(backend, data).await;
    create_group_edges(backend, data).await;
    create_product_edges(backend, data).await;
    create_user_subscription_timelines(backend, data).await;
}

#[tracing::instrument(skip(backend, data))]
async fn create_group_edges(backend: &impl Backend<EdgeKind>, data: &HeapBinaryData) {
    backend
        .insert_edges(
            data.groups
                .iter()
                .filter(|group| !group.edges.is_empty())
                .map(|group| {
                    Edge::new(
                        EdgeKind::SubscriptionTimelinePeriod {
                            subscription_timeline_id: group.product_id.to_owned(),
                            period_id: group.id.to_owned(),
                        },
                        group.edges.iter().map(move |edge| {
                            EdgeDestination::new(
                                match edge.kind {
                                    binary::ArchivedEdgeKind::Role => EdgeKind::Role {
                                        id: edge.id.to_owned(),
                                    },
                                    binary::ArchivedEdgeKind::Badge => EdgeKind::Badge {
                                        id: edge.id.to_owned(),
                                    },
                                    binary::ArchivedEdgeKind::Paint => EdgeKind::Paint {
                                        id: edge.id.to_owned(),
                                    },
                                    binary::ArchivedEdgeKind::EmoteSet => EdgeKind::EmoteSet {
                                        id: edge.id.to_owned(),
                                    },
                                    _ => unreachable!(),
                                },
                                edge.active,
                            )
                        }).collect(),
                    )
                }),
        )
        .await;
}

#[tracing::instrument(skip(backend, data))]
async fn create_product_edges(backend: &impl Backend<EdgeKind>, data: &HeapBinaryData) {
    backend
        .insert_edges(
            data.products
                .iter()
                .filter(|product| product.is_static)
                .map(|product| {
                    Edge::new(
                        EdgeKind::Product {
                            id: product.id.to_owned(),
                        },
                        product.edges.iter().map(move |edge| {
                            EdgeDestination::new(
                                match edge.kind {
                                    binary::ArchivedEdgeKind::Badge => EdgeKind::Badge {
                                        id: edge.id.to_owned(),
                                    },
                                    binary::ArchivedEdgeKind::Paint => EdgeKind::Paint {
                                        id: edge.id.to_owned(),
                                    },
                                    binary::ArchivedEdgeKind::EmoteSet => EdgeKind::EmoteSet {
                                        id: edge.id.to_owned(),
                                    },
                                    binary::ArchivedEdgeKind::Role => EdgeKind::Role {
                                        id: edge.id.to_owned(),
                                    },
                                    _ => unreachable!(),
                                },
                                edge.active,
                            )
                        }).collect(),
                    )
                }),
        )
        .await;
}

#[tracing::instrument(skip(backend, data))]
async fn create_user_edges(backend: &impl Backend<EdgeKind>, data: &HeapBinaryData) {
    backend
        .insert_edges(data.users.iter().map(|user| {
            Edge::new(
                EdgeKind::User {
                    id: user.id.to_owned(),
                },
                user.edges.iter().map(move |edge| {
                    EdgeDestination::new(
                        match edge.kind {
                            binary::ArchivedEdgeKind::Role => EdgeKind::Role {
                                id: edge.id.to_owned(),
                            },
                            binary::ArchivedEdgeKind::Badge => EdgeKind::Badge {
                                id: edge.id.to_owned(),
                            },
                            binary::ArchivedEdgeKind::Paint => EdgeKind::Paint {
                                id: edge.id.to_owned(),
                            },
                            binary::ArchivedEdgeKind::EmoteSet => EdgeKind::EmoteSet {
                                id: edge.id.to_owned(),
                            },
                            binary::ArchivedEdgeKind::Product => EdgeKind::Product {
                                id: edge.id.to_owned(),
                            },
                            binary::ArchivedEdgeKind::UserProduct => {
                                EdgeKind::UserSubscriptionTimeline {
                                    user_id: user.id.to_owned(),
                                    subscription_timeline_id: edge
                                        .id
                                        .strip_prefix(user.id.as_str())
                                        .unwrap()
                                        .strip_prefix(':')
                                        .unwrap()
                                        .to_owned(),
                                }
                            }
                            _ => unreachable!(),
                        },
                        edge.active,
                    )
                }).collect(),
            )
        }))
        .await;
}

#[tracing::instrument(skip(backend, data))]
async fn create_role_edges(backend: &impl Backend<EdgeKind>, data: &HeapBinaryData) {
    backend
        .insert_edges(data.roles.iter().map(|role| {
            Edge::new(
                EdgeKind::Role {
                    id: role.id.to_owned(),
                },
                role.edges.iter().map(move |edge| {
                    EdgeDestination::new(
                        match edge.kind {
                            binary::ArchivedEdgeKind::Badge => EdgeKind::Badge {
                                id: edge.id.to_owned(),
                            },
                            binary::ArchivedEdgeKind::Paint => EdgeKind::Paint {
                                id: edge.id.to_owned(),
                            },
                            binary::ArchivedEdgeKind::EmoteSet => EdgeKind::EmoteSet {
                                id: edge.id.to_owned(),
                            },
                            _ => unreachable!(),
                        },
                        edge.active,
                    )
                }).collect(),
            )
        }))
        .await;
}

#[tracing::instrument(skip(backend, data))]
async fn create_user_subscription_timelines(
    backend: &impl Backend<EdgeKind>,
    data: &HeapBinaryData,
) {
    backend
        .insert_edges(data.user_products.iter().map(|user_product| {
            Edge::new(
                EdgeKind::UserSubscriptionTimeline {
                    subscription_timeline_id: user_product.product_id.to_owned(),
                    user_id: user_product.user_id.to_owned(),
                },    
                user_product.edges.iter().filter_map(move |edge| {
                    if !edge.active {
                        return None;
                    }
                    Some(EdgeDestination::new(
                        match edge.kind {
                            binary::ArchivedEdgeKind::Product => EdgeKind::Product {
                                id: edge.id.to_owned(),
                            },
                            binary::ArchivedEdgeKind::Group => EdgeKind::SubscriptionTimelinePeriod {
                                subscription_timeline_id: user_product.product_id.to_owned(),
                                period_id: edge.id.to_owned(),
                            },
                            _ => unreachable!(),
                        },
                        edge.active,
                    ))
                }).collect(),
            )
        }))
        .await;
}
