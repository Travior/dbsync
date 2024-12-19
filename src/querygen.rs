use std::collections::HashMap;

use crate::schema::SyncEntry;
use crate::unitycatalog::{Catalog, Schema, Table, UnityCatalog};
use chrono::TimeDelta;
use log::{info, warn};
use serde::{self, Deserialize, Deserializer};

fn hour_to_duration<'de, D>(deserializer: D) -> Result<chrono::TimeDelta, D::Error>
where
    D: Deserializer<'de>,
{
    let hours: i64 = Deserialize::deserialize(deserializer)?;
    Ok(TimeDelta::seconds(hours * 3600))
}

#[derive(Debug, Deserialize)]
pub struct QueryGen {
    #[serde(deserialize_with = "hour_to_duration")]
    max_staleness_duration_hours: TimeDelta,
    deep_clone_non_managed: bool,
    create_schema_if_missing: bool,
}

impl QueryGen {
    fn generate_query_from_table_comparison(&self, parent: &Table, child: &Catalog) -> Vec<String> {
        let mut queries = Vec::<String>::new();

        if !child.schemas.contains_key(&parent.schema_name) {
            if self.create_schema_if_missing {
                queries.push(format!(
                    "CREATE SCHEMA {}.{};",
                    child.name, parent.schema_name
                ));
            } else {
                warn!(
                    "Schema {}.{} does not exist in child catalog. Skipping...",
                    child.name, parent.schema_name
                );
                return queries;
            }
        }
        let binding = Schema {
            _name: parent.schema_name.to_string(),
            tables: HashMap::new(),
        };
        let schema = child.schemas.get(&parent.schema_name).unwrap_or(&binding);

        let mut clone_type: String = "SHALLOW".to_string();
        match parent {
            &Table { ref table_type, .. }
                if table_type != "MANAGED" && self.deep_clone_non_managed =>
            {
                clone_type = String::from("DEEP")
            }
            &Table {
                ref data_source_format,
                ..
            } if data_source_format.as_ref().is_none_or(|f| f != "DELTA")
                && self.deep_clone_non_managed =>
            {
                clone_type = String::from("DEEP")
            }
            _ => {}
        }

        let table = schema.tables.get(&parent.name);
        match table {
            Some(table) => {
                if parent.updated_at - table.updated_at > self.max_staleness_duration_hours {
                    info!("Table {} is stale. Recreating...", table.to_path());
                    queries.push(format!(
                        "CREATE OR REPLACE TABLE {} {} CLONE {};",
                        table.to_path(),
                        clone_type,
                        parent.to_path()
                    ));
                    return queries;
                }
            }
            None => {
                info!(
                    "Table {}.{}.{} does not exist. Creating...",
                    child.name, parent.schema_name, parent.name
                );
                queries.push(format!(
                    "CREATE TABLE {}.{}.{} {} CLONE {}.{}.{};",
                    child.name,
                    parent.schema_name,
                    parent.name,
                    clone_type,
                    parent.catalog_name,
                    parent.schema_name,
                    parent.name
                ));
            }
        }
        return queries;
    }

    pub fn generate_queries(&self, uc: &UnityCatalog, syncs: Vec<SyncEntry>) -> Vec<String> {
        let queries: Vec<String> = syncs
            .iter()
            .map(|s| {
                let catalog = uc.catalogs.get(&s.catalog).unwrap();
                let queries_for_catalog: Vec<String> = s
                    .pinned_catalogs
                    .iter()
                    .map(|p| {
                        let pinned_catalog = uc.catalogs.get(p).unwrap();
                        let queries_for_pinned_catalog: Vec<String> = catalog
                            .iter_tables()
                            .map(|t| self.generate_query_from_table_comparison(t, pinned_catalog))
                            .flatten()
                            .collect();
                        return queries_for_pinned_catalog;
                    })
                    .flatten()
                    .collect();
                return queries_for_catalog;
            })
            .flatten()
            .collect();
        return queries;
    }
}
