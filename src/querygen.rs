use crate::schema::SyncEntry;
use crate::unitycatalog::{Catalog, Table, UnityCatalog};
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
}

impl QueryGen {
    fn generate_query_from_table_comparison(
        &self,
        parent: &Table,
        child: &Catalog,
    ) -> Option<String> {
        if parent.table_type != "MANAGED" {
            warn!("Table {} is not MANAGED (type={}). Only managed tables support SHALLOW CLONE. Skipping...",
            parent.to_path(), parent.table_type);
            return None;
        }
        if parent
            .data_source_format
            .as_ref()
            .is_none_or(|f| f != "DELTA")
        {
            warn!("Table {} is not of data source format DELTA (format={}). Only DELTA tables support SHALLOW CLONE. Skipping...",
            parent.to_path(), parent.data_source_format.as_ref().unwrap_or(&String::from("N/A")));
            return None;
        }

        let schema = child.schemas.get(&parent.schema_name);
        match schema {
            Some(schema) => {
                let table = schema.tables.get(&parent.name);
                match table {
                    Some(table) => {
                        if parent.updated_at - table.updated_at > self.max_staleness_duration_hours
                        {
                            info!("Table {} is stale. Recreating...", table.to_path());
                            return Some(format!(
                                "CREATE OR REPLACE TABLE {} SHALLOW CLONE {};",
                                table.to_path(),
                                parent.to_path()
                            ));
                        }
                        return None;
                    }
                    None => {
                        info!(
                            "Table {}.{}.{} does not exist. Creating...",
                            child.name, parent.schema_name, parent.name
                        );
                        Some(format!(
                            "CREATE TABLE {}.{}.{} SHALLOW CLONE {}.{}.{};",
                            child.name,
                            parent.schema_name,
                            parent.name,
                            parent.catalog_name,
                            parent.schema_name,
                            parent.name
                        ))
                    }
                }
            }
            None => {
                warn!(
                    "Schema {}.{} doesn't exist. Skipping...",
                    child.name, parent.schema_name
                );
                return None;
            }
        }
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
                            .filter_map(|t| {
                                self.generate_query_from_table_comparison(t, pinned_catalog)
                            })
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
