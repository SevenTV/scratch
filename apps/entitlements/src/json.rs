use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::binary::{self, BinaryData};

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ObjectId {
    #[serde(rename = "$oid")]
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum EntitlementItemKind {
    Badge,
    Paint,
    EmoteSet,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EdgeKind {
    Role,
    Product,
    Badge,
    Paint,
    EmoteSet,
    Group,
    UserProduct,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ItemKind {
    Badge,
    Paint,
    EmoteSet,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Item {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    pub kind: ItemKind,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Edge {
    pub ref_id: String,
    pub kind: EdgeKind,
    pub active: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct User {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    pub username: String,
    pub role_ids: Vec<ObjectId>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Role {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct EmoteSet {
    #[serde(rename = "_id")]
    pub id: ObjectId,
    pub name: String,
}

#[tracing::instrument]
fn load_file<T>(path: &str) -> Vec<T>
where
    T: for<'de> Deserialize<'de>,
{
    let data = std::fs::read(path).expect("file");
    serde_json::from_slice(&data).expect("json")
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonData {
    pub items: Vec<Item>,
    pub users: Vec<User>,
    pub roles: Vec<Role>,
    pub user_edges: Vec<UserEdges>,
    pub products: Vec<Product>,
    pub groups: Vec<Group>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UserEdges {
    pub id: String,
    pub edges: Vec<Edge>,
    pub products: Vec<UserProduct>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UserProduct {
    pub id: String,
    pub edges: Vec<Edge>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Group {
    pub id: String,
    pub edges: Vec<Edge>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Product {
    pub id: String,
    pub name: String,
    pub dynamic: bool,
    pub edges: Vec<Edge>,
}

#[tracing::instrument]
pub fn load() -> binary::BinaryData {
    let mut items = load_file::<Item>("secret/entitlements/raw/7tv.cosmetics.json");
    tracing::info!("loaded {} cosmetics", items.len());
    let users = load_file::<User>("secret/entitlements/raw/7tv.users.json");
    tracing::info!("loaded {} users", users.len());
    let roles = load_file::<Role>("secret/entitlements/raw/7tv.roles.json");
    tracing::info!("loaded {} roles", roles.len());
    let emote_sets = load_file::<EmoteSet>("secret/entitlements/raw/7tv.emote_sets.json");
    tracing::info!("loaded {} emote sets", emote_sets.len());
    let groups = load_file::<Group>("secret/entitlements/raw/groups.json");
    tracing::info!("loaded {} groups", groups.len());
    let products = load_file::<Product>("secret/entitlements/raw/products.json");
    tracing::info!("loaded {} products", products.len());
    let user_edges = load_file::<UserEdges>("secret/entitlements/raw/users.json");
    tracing::info!("loaded {} user entitlements", user_edges.len());

    items.extend(emote_sets.into_iter().map(|emote_set| Item {
        id: emote_set.id,
        name: emote_set.name,
        kind: ItemKind::EmoteSet,
    }));

    let mut user_edges = user_edges
        .into_iter()
        .map(|user_edges| {
            let edges: Vec<binary::Edge> = user_edges.edges.into_iter().map(Into::into).collect();
            let products: Vec<binary::UserProduct> = user_edges
                .products
                .into_iter()
                .map(|up| binary::UserProduct {
                    id: format!("{}:{}", user_edges.id, up.id),
                    product_id: up.id,
                    user_id: user_edges.id.clone(),
                    edges: up.edges.into_iter().map(Into::into).collect(),
                })
                .collect();

            (user_edges.id, (edges, products))
        })
        .collect::<HashMap<_, _>>();

    let (users, user_products): (Vec<_>, Vec<_>) = users
        .into_iter()
        .map(|user| {
            let mut user = binary::User::from(user);

            let products = if let Some((edges, products)) = user_edges.remove(&user.id) {
                user.edges.extend(edges);
                products
            } else {
                vec![]
            };

            (user, products)
        })
        .unzip();

    BinaryData {
        users,
        user_products: user_products.into_iter().flatten().collect(),
        groups: groups.into_iter().map(Into::into).collect(),
        items: items.into_iter().map(Into::into).collect(),
        products: products.into_iter().map(Into::into).collect(),
        roles: roles.into_iter().map(Into::into).collect(),
    }
}

impl From<EdgeKind> for binary::EdgeKind {
    fn from(value: EdgeKind) -> Self {
        match value {
            EdgeKind::Badge => binary::EdgeKind::Badge,
            EdgeKind::Paint => binary::EdgeKind::Paint,
            EdgeKind::EmoteSet => binary::EdgeKind::EmoteSet,
            EdgeKind::Product => binary::EdgeKind::Product,
            EdgeKind::Group => binary::EdgeKind::Group,
            EdgeKind::Role => binary::EdgeKind::Role,
            EdgeKind::UserProduct => binary::EdgeKind::UserProduct,
        }
    }
}

impl From<User> for binary::User {
    fn from(value: User) -> Self {
        binary::User {
            id: value.id.id,
            username: value.username,
            edges: value
                .role_ids
                .into_iter()
                .map(|id| binary::Edge {
                    kind: binary::EdgeKind::Role,
                    id: id.id,
                    active: true,
                })
                .collect(),
        }
    }
}

impl From<Role> for binary::Role {
    fn from(value: Role) -> Self {
        binary::Role {
            id: value.id.id,
            name: value.name,
            edges: vec![],
        }
    }
}

impl From<Item> for binary::Item {
    fn from(value: Item) -> Self {
        binary::Item {
            id: value.id.id,
            kind: value.kind.into(),
            name: value.name,
        }
    }
}

impl From<ItemKind> for binary::ItemKind {
    fn from(value: ItemKind) -> Self {
        match value {
            ItemKind::Badge => binary::ItemKind::Badge,
            ItemKind::Paint => binary::ItemKind::Paint,
            ItemKind::EmoteSet => binary::ItemKind::EmoteSet,
        }
    }
}

impl From<Edge> for binary::Edge {
    fn from(value: Edge) -> Self {
        binary::Edge {
            kind: value.kind.into(),
            id: value.ref_id,
            active: value.active,
        }
    }
}

impl From<Product> for binary::Product {
    fn from(value: Product) -> Self {
        binary::Product {
            id: value.id,
            name: value.name,
            is_static: !value.dynamic,
            edges: value.edges.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<Group> for binary::Group {
    fn from(value: Group) -> Self {
        binary::Group {
            id: value.id,
            product_id: "?".to_string(),
            edges: value.edges.into_iter().map(Into::into).collect(),
        }
    }
}
