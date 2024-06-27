use rkyv::{Archive, Archived, Deserialize, Serialize};

use crate::{json, transform::transform_data};

#[derive(
    Debug, Archive, Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Copy,
)]
#[archive(check_bytes)]
#[archive_attr(repr(u8))]
#[archive_attr(derive(Debug))]
pub enum ItemKind {
    Badge,
    Paint,
    EmoteSet,
}

#[derive(
    Debug, Archive, Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Copy,
)]
#[archive(check_bytes)]
#[archive_attr(repr(u8))]
#[archive_attr(derive(Debug))]
pub enum EdgeKind {
    Badge,
    Paint,
    EmoteSet,
    Role,
    Product,
    UserProduct,
    Group,
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(repr(C))]
#[archive_attr(derive(Debug))]
pub struct Product {
    pub id: String,
    pub name: String,
    pub is_static: bool,
    pub edges: Vec<Edge>,
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(repr(C))]
#[archive_attr(derive(Debug))]
pub struct Item {
    pub id: String,
    pub kind: ItemKind,
    pub name: String,
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(repr(C))]
#[archive_attr(derive(Debug))]
pub struct Group {
    pub id: String,
    pub product_id: String,
    pub edges: Vec<Edge>,
}

#[derive(Debug, Archive, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[archive(check_bytes)]
#[archive_attr(repr(C))]
#[archive_attr(derive(Debug))]
pub struct Edge {
    pub id: String,
    pub kind: EdgeKind,
    pub active: bool,
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(repr(C))]
#[archive_attr(derive(Debug))]
pub struct User {
    pub id: String,
    pub username: String,
    pub edges: Vec<Edge>,
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(repr(C))]
#[archive_attr(derive(Debug))]
pub struct UserProduct {
    pub id: String,
    pub user_id: String,
    pub product_id: String,
    pub edges: Vec<Edge>,
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(repr(C))]
pub struct Role {
    pub id: String,
    pub name: String,
    pub edges: Vec<Edge>,
}

#[derive(Debug, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
#[archive_attr(repr(C))]
pub struct BinaryData {
    pub users: Vec<User>,
    pub roles: Vec<Role>,
    pub items: Vec<Item>,
    pub products: Vec<Product>,
    pub user_products: Vec<UserProduct>,
    pub groups: Vec<Group>,
}

#[tracing::instrument]
pub fn load_cache() -> (HeapBinaryData, bool) {
    if let Some(data) = load() {
        return (data, true);
    }

    let mut data = json::load();
    transform_data(&mut data);
    save(&data);

    (load().expect("loaded"), false)
}

pub fn save(data: &BinaryData) {
    let bytes = rkyv::to_bytes::<_, 4096>(data).expect("serialized");
    std::fs::write("secret/entitlements/7tv.entitlements.bin", &bytes).expect("saved");
}

fn load() -> Option<HeapBinaryData> {
    let data = std::fs::read("secret/entitlements/7tv.entitlements.bin").ok()?;

    Some(HeapBinaryData::new(data))
}

pub struct HeapBinaryData {
    buffer: Vec<u8>,
}

impl HeapBinaryData {
    fn new(buffer: Vec<u8>) -> Self {
        rkyv::check_archived_root::<BinaryData>(&buffer).expect("checked");

        Self { buffer }
    }
}

impl std::ops::Deref for HeapBinaryData {
    type Target = Archived<BinaryData>;

    fn deref(&self) -> &Self::Target {
        unsafe { rkyv::archived_root::<BinaryData>(&self.buffer) }
    }
}
