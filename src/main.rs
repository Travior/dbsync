mod api;
mod schema;
use api::{FetchJob};
use clap::Parser;
use futures::stream::StreamExt;
use log::debug;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use tokio::{self};
use unitycatalog::{Catalog, DiffTree, Schema, Table, UnityCatalog, UnityCatalogElement};
use chrono::DateTime;
mod querygen;

mod unitycatalog;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    config_path: PathBuf,
    #[arg(long)]
    num_request: Option<usize>,
}

async fn process_job(
    job: FetchJob,
    client: &api::Client,
) -> (Vec<FetchJob>, Vec<UnityCatalogElement>) {
    let mut new_jobs = Vec::new();
    let mut new_elements = Vec::new();

    match job {
        FetchJob::FetchAllCatalogs(j) => {
            debug!("Fetching all catalogs");
            if let Ok(children) = j.get_children(client).await {
                new_jobs.extend(children.iter().map(|catalog| FetchJob::FetchCatalog(api::FetchCatalog {
                    catalog_name: catalog.name.clone(),
                })));
                new_elements.extend(children.iter().map(|catalog| {
                    UnityCatalogElement::Catalog(Catalog {
                        name: catalog.name.clone(),
                        schemas: HashMap::new(),
                    })
                }));
            }
        }
        FetchJob::FetchCatalog(c) => {
            debug!("Fetching catalog {}", c.catalog_name);
            if let Ok(children) = c.get_children(client).await {
                new_jobs.extend(children.iter().map(|schema| {
                    FetchJob::FetchSchema(api::FetchSchema {
                        catalog_name: schema.catalog_name.clone(),
                        schema_name: schema.name.clone(),
                    })
                }));
                new_elements.extend(children.iter().map(|schema| {
                    UnityCatalogElement::Schema(Schema {
                        _name: schema.name.clone(),
                        _catalog_name: schema.catalog_name.clone(),
                        tables: HashMap::new(),
                    })
                }));
            }
        }
        FetchJob::FetchSchema(s) => {
            let catalog_name = s.catalog_name.clone();
            let schema_name = s.schema_name.clone();
            debug!("Fetching schema {}.{}", catalog_name, schema_name);
            if let Ok(children) = s.get_children(client).await {
                debug!(
                    "Found {} tables in {}.{}",
                    children.len(),
                    catalog_name,
                    schema_name
                );
                new_elements.extend(children.into_iter().map(|table| {
                    UnityCatalogElement::Table(Table {
                        name: table.name,
                        schema_name: table.schema_name,
                        catalog_name: table.catalog_name,
                        table_type: table.table_type,
                        data_source_format: table.data_source_format,
                        updated_at: DateTime::from_timestamp_millis(table.updated_at).unwrap(),
                        _updated_by: table.updated_by,
                        _properties: table.properties,
                    })
                }));
            }
        }
    }
  (new_jobs, new_elements)
}

fn create_job_future(
    job: FetchJob,
    client: &api::Client,
) -> Pin<Box<dyn Future<Output = (Vec<FetchJob>, Vec<UnityCatalogElement>)> + Send + '_>> {
    Box::pin(process_job(job, client))
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();
    debug!("Parsing config");
    let config = schema::load_config(&args.config_path.to_str().unwrap()).unwrap();
    let mut catalogs: HashSet<String> = HashSet::new();
    let mut unity_catalog = UnityCatalog::new();
    config.catalogs.iter().for_each(|c| {
        catalogs.insert(c.catalog.clone());
        c.pinned_catalogs.iter().for_each(|c| {
            catalogs.insert(c.clone());
        });
    });
    debug!("Catalogs: {:?}", catalogs);

    let client = api::Client::new(&config.host, &config.pat);

    // Create a queue for pending jobs
    let mut job_queue: VecDeque<FetchJob> = catalogs
        .iter()
        .map(|c| {
            unity_catalog.insert_assume_ordered(UnityCatalogElement::Catalog(Catalog {
                name: c.clone(),
                schemas: HashMap::new(),
            }));
            FetchJob::FetchCatalog(api::FetchCatalog {
                catalog_name: c.clone(),
            })
        })
        .collect();

    // let mut job_queue: VecDeque<FetchJob> = VecDeque::new();
    // job_queue.push_back(FetchJob::FetchAllCatalogs(FetchAllCatalogs {}));

    let mut active_futures = futures::stream::FuturesUnordered::new();

    while active_futures.len() < 10 && !job_queue.is_empty() {
        if let Some(job) = job_queue.pop_front() {
            active_futures.push(create_job_future(job, &client));
        }
    }

    while !active_futures.is_empty() {
        if let Some((new_jobs, uc_elements)) = active_futures.next().await {
            uc_elements.into_iter().for_each(|element| {
                unity_catalog.insert_assume_ordered(element);
            });
            job_queue.extend(new_jobs);
            while active_futures.len() < 10 && !job_queue.is_empty() {
                if let Some(job) = job_queue.pop_front() {
                    active_futures.push(create_job_future(job, &client));
                }
            }
        }
    }

    for catalog in config.catalogs {
        for pinned_catalog in catalog.pinned_catalogs {
            let mut queries = VecDeque::new();
            let cat = unity_catalog.catalogs.get(&catalog.catalog).unwrap();
            let pinned_cat = unity_catalog.catalogs.get(&pinned_catalog).unwrap();
            let diff = cat.diff(Some(pinned_cat)).unwrap();
            let query = querygen::Query::from_diff_node(&diff, &pinned_catalog);
            queries.push_back(query);

            while !queries.is_empty() {
                let query = queries.pop_front().unwrap();
                if let Some(query_string) = query.query {
                    println!("{}", query_string);
                }
                queries.extend(query.children);
            }
            
        }
    }
}
