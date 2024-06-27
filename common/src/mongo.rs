use futures::TryStreamExt;
use mongodb::options::{FindOptions, InsertManyOptions};

use crate::{Direction, Edge, EdgeDestination, EdgeKind};

const BATCH_SIZE: usize = 1000;

impl<K: EdgeKind> crate::Backend<K> for mongodb::Database {
    type Client = Self;

    fn client(&self) -> &Self::Client {
        self
    }

    async fn create_schema(&self) {
        self.create_collection("edges", None)
            .await
            .expect("collection");

        self.collection::<()>("edges")
            .create_index(
                mongodb::IndexModel::builder()
                    .keys(bson::doc! { "destinations": 1 })
                    .build(),
                None,
            )
            .await
            .expect("index");
    }

    async fn drop_schema(&self) {
        self.collection::<()>("edges")
            .drop(None)
            .await
            .expect("drop collection");
    }

    async fn insert_edges<I: IntoIterator<Item = Edge<K>> + Send>(&self, edges: I)
    where
        I::IntoIter: Send,
    {
        let data = edges.into_iter().collect::<Vec<_>>();
        let total_count = data.len();

        let chunks = data.chunks(BATCH_SIZE);

        let mut inserted = 0;
        let collection = self.collection::<Edge<K>>("edges");

        for chunk in chunks {
            let r = collection
                .insert_many(
                    chunk,
                    Some(InsertManyOptions::builder().ordered(false).build()),
                )
                .await
                .expect("insert");

            inserted += r.inserted_ids.len();

            // Print every 1%
            tracing::info!("inserted: {}/{}", inserted, total_count);
        }

        tracing::info!("done!");
    }

    async fn fetch_edges(&self, direction: Direction, start_nodes: &[K]) -> Vec<Edge<K>> {
        futures::future::join_all(start_nodes.chunks(1000).map(|start_nodes| async move {
            let query = match direction {
                Direction::Inbound => bson::doc! {
                    "destinations": {
                        "$in": bson::to_bson(&start_nodes.into_iter().map(|kind| EdgeDestination::new(kind, true)).collect::<Vec<_>>()).unwrap(),
                    }
                },
                Direction::Outbound => bson::doc! {
                    "_id": {
                        "$in": bson::to_bson(&start_nodes).unwrap(),
                    }
                },
            };

            let projection = match direction {
                Direction::Inbound => bson::doc! {
                    "destinations": 0,
                },
                Direction::Outbound => bson::doc! {},
            };

            let collection = self.collection::<Edge<K>>("edges");
            let options = Some(
                FindOptions::builder()
                    .batch_size(BATCH_SIZE as u32)
                    .projection(projection)
                    .build(),
            );

            collection
                .find(query, options)
                .await
                .expect("find")
                .try_collect::<Vec<_>>()
                .await
                .expect("collect")
        }))
        .await
        .into_iter()
        .flatten()
        .collect()
    }
}
