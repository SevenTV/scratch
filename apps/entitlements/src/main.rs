use std::collections::HashSet;

use clap::Parser;
use common::{Backend, BackendType, Direction, Query};
use entitlements::{
    binary,
    data::{self, EdgeKind},
};

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

#[tokio::main]
async fn main() {
    let args = Args::parse();

    std::env::set_var(
        "RUST_LOG",
        std::env::var("RUST_LOG").unwrap_or_else(|_| args.loggging.clone()),
    );

    tracing_subscriber::fmt::init();

    if args.backend == BackendType::Mongo {
        tracing::info!("Using MongoDB backend");
        mongo(args).await;
    }
}

async fn mongo(args: Args) {
    let client = mongodb::Client::with_uri_str("mongodb://localhost:27111")
        .await
        .expect("client");

    handle_backend(args, client.database("7tv_entitlements_graph")).await;
}

async fn handle_backend(args: Args, backend: impl Backend<EdgeKind>) {
    if args.load_data {
        tracing::info!("Loading data...");

        let (data, _) = binary::load_cache();

        tracing::info!("Data loaded!");

        tracing::info!("Creating database...");
        backend.drop_schema().await;
        tracing::info!("Creating schema...");
        backend.create_schema().await;

        tracing::info!("Inserting data...");
        data::create_data(&backend, &data).await;
        tracing::info!("Data inserted!");
    }

    if let Some(query) = &args.query {
        tracing::info!("Running query...");

        for _ in 0..args.query_count.unwrap_or(1) {
            let (direction, start_node) = match query {
                Query::Inbound(start_node) => (Direction::Inbound, start_node.clone()),
                Query::Outbound(start_node) => (Direction::Outbound, start_node.clone()),
            };

            let edges = backend.traversal(direction, start_node).await;

            let true_edges = edges.iter().flat_map(|edge| {
                match direction {
                    Direction::Inbound => vec![edge.from().clone()],
                    Direction::Outbound => edge
                        .destinations()
                        .iter()
                        .filter_map(|dest| {
                            if dest.active {
                                Some(dest.to.clone())
                            } else {
                                None
                            }
                        })
                        .collect(),
                }
            }).collect::<HashSet<_>>();

            tracing::info!("Query result: {}", true_edges.len());
        }

        tracing::info!("Query complete!");
    }
}
