use std::{fmt::Debug, hash::Hash, str::FromStr};

use serde::{Deserialize, Serialize};

mod mongo;

#[derive(Debug, Clone)]
pub enum Query<K> {
    Inbound(K),
    Outbound(K),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendType {
    Mongo,
}

impl FromStr for BackendType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "mongo" => Ok(BackendType::Mongo),
            _ => Err("invalid backend".to_string()),
        }
    }
}

impl<K: FromStr> FromStr for Query<K> {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts = s.splitn(2, ':').collect::<Vec<_>>();

        if parts.len() != 2 {
            return Err("invalid query".to_string());
        }

        let kind = parts[0];
        let edge_kind = parts[1].parse().map_err(|_| "invalid edge kind")?;

        match kind {
            "inbound" => Ok(Query::Inbound(edge_kind)),
            "outbound" => Ok(Query::Outbound(edge_kind)),
            _ => Err("invalid query".to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Inbound,
    Outbound,
}

pub trait EdgeKind:
    Send
    + Sync
    + Clone
    + Sized
    + PartialEq
    + Eq
    + Hash
    + Serialize
    + for<'de> Deserialize<'de>
    + Debug
    + 'static
{
    fn has_inbound(&self) -> bool;
    fn has_outbound(&self) -> bool;
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct EdgeDestination<K> {
    pub to: K,
    pub active: bool,
}

impl<K> EdgeDestination<K> {
    pub fn new(to: K, active: bool) -> Self {
        Self { to, active }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Edge<K> {
    #[serde(rename = "_id")]
    pub from: K,
    #[serde(default = "Vec::new")]
    pub destinations: Vec<EdgeDestination<K>>,
}

impl<K: Hash> Edge<K> {
    pub fn new(from: K, destinations: Vec<EdgeDestination<K>>) -> Self {
        Self { from, destinations }
    }

    pub fn from(&self) -> &K {
        &self.from
    }

    pub fn destinations(&self) -> &[EdgeDestination<K>] {
        &self.destinations
    }
}

impl Direction {
    pub fn edge_next<K: EdgeKind>(&self, edge: &Edge<K>) -> Vec<K> {
        match self {
            Direction::Inbound if edge.from().has_inbound() => vec![edge.from().clone()],
            Direction::Outbound => edge.destinations().iter().filter_map(|dest| {
                if dest.active && dest.to.has_outbound() {
                    Some(dest.to.clone())
                } else {
                    None
                }
            }).collect(),
            _ => Vec::new(),
        }
    }
}

pub trait Backend<K: EdgeKind>: Send + Sync {
    type Client;

    fn client(&self) -> &Self::Client;

    fn insert_edges<I: IntoIterator<Item = Edge<K>> + Send>(
        &self,
        edges: I,
    ) -> impl std::future::Future<Output = ()> + Send
    where
        I::IntoIter: Send;

    fn create_schema(&self) -> impl std::future::Future<Output = ()> + Send;

    fn drop_schema(&self) -> impl std::future::Future<Output = ()> + Send;

    fn traversal(
        &self,
        direction: Direction,
        start_node: K,
    ) -> impl std::future::Future<Output = Vec<Edge<K>>> + Send {
        async move {
            let mut visited = fnv::FnvHashSet::default();
            self.traversal_filter(direction, start_node, |kind| visited.insert(kind.clone()))
                .await
        }
    }

    #[tracing::instrument(skip(self, filter))]
    fn traversal_filter(
        &self,
        direction: Direction,
        start_node: K,
        mut filter: impl FnMut(&K) -> bool + Send,
    ) -> impl std::future::Future<Output = Vec<Edge<K>>> + Send {
        async move {
            let start = std::time::Instant::now();

            let mut total_edges = vec![];
            let mut iteration = 0;

            let mut next_edges = vec![];
            if filter(&start_node) {
                next_edges.push(start_node);
            }

            while !next_edges.is_empty() {
                let start = std::time::Instant::now();
                let new_edges = self.fetch_edges(direction, &next_edges).await;

                next_edges.clear();

                for edge in &new_edges {
                    direction.edge_next(edge).into_iter().for_each(|kind| {
                        if filter(&kind) {
                            next_edges.push(kind);
                        }
                    });
                }

                tracing::info!(
                    "[{iteration}] current edges: {} - {:?}",
                    new_edges.len(),
                    start.elapsed()
                );
                iteration += 1;
                total_edges.extend(new_edges);
            }

            tracing::info!("total edges: {} - {:?}", total_edges.len(), start.elapsed());

            total_edges
        }
    }

    fn fetch_edges(
        &self,
        direction: Direction,
        start_nodes: &[K],
    ) -> impl std::future::Future<Output = Vec<Edge<K>>> + Send;
}
