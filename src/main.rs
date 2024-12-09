mod api;
mod querygen;
mod schema;
mod unitycatalog;
use api::Table;
use clap::Parser;
use futures::{self, stream, StreamExt};
use log::{debug, info};
use std::collections::HashSet;
use std::path::PathBuf;
use tokio;
use unitycatalog::UnityCatalog;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    config_path: PathBuf,
    #[arg(long)]
    num_request: Option<usize>,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();
    debug!("Parsing config");
    let config = schema::load_config(&args.config_path.to_str().unwrap()).unwrap();
    let mut catalogs: HashSet<&String> = HashSet::new();
    config.catalogs.iter().for_each(|c| {
        catalogs.insert(&c.catalog);
        c.pinned_catalogs.iter().for_each(|c| {
            catalogs.insert(c);
        });
    });
    let mut tables: Vec<Table> = Vec::new();
    info!("Host is {}", config.host);

    for catalog in catalogs {
        let client = api::APIClient::new(&config.host, &config.pat);
        let res_schemas = client.collect_schemas(&catalog).await.unwrap();
        let futures = res_schemas
            .iter()
            .map(|s| client.collect_tables(&catalog, &s.name))
            .collect::<Vec<_>>();
        let results: Vec<Table> = stream::iter(futures)
            .buffer_unordered(args.num_request.unwrap_or(1))
            .filter_map(|res| async move { res.ok() })
            .collect::<Vec<Vec<Table>>>()
            .await
            .into_iter()
            .flatten()
            .collect();
        tables.extend(results);
    }
    let uc = UnityCatalog::from_records(tables);
    println!();
    println!();

    let queries = config
        .generation_config
        .generate_queries(&uc, config.catalogs);
    queries.iter().for_each(|q| println!("{}", q));
}
