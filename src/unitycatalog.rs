use chrono::DateTime;
use chrono::Utc;
use log::info;
use std::collections::HashMap;
use std::iter::Iterator;

use crate::api::Table as TableData;

pub struct Table {
    pub name: String,
    pub schema_name: String,
    pub catalog_name: String,
    pub table_type: String,
    pub data_source_format: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub _updated_by: String,
    pub _properties: HashMap<String, String>,
}

pub struct Schema {
    pub _name: String,
    pub tables: HashMap<String, Table>,
}

pub struct Catalog {
    pub name: String,
    pub schemas: HashMap<String, Schema>,
}
pub struct UnityCatalog {
    pub catalogs: HashMap<String, Catalog>,
}

impl Table {
    pub fn to_path(&self) -> String {
        return format!("{}.{}.{}", self.catalog_name, self.schema_name, self.name);
    }
}

impl Catalog {
    pub fn iter_tables(&self) -> impl Iterator<Item = &Table> {
        return self.schemas.values().map(|s| s.tables.values()).flatten();
    }
}

impl UnityCatalog {
    pub fn from_records(records: Vec<TableData>) -> Self {
        info!("Creating unity_catalog from {} records", records.len());
        let mut catalog = UnityCatalog {
            catalogs: HashMap::new(),
        };
        for r in records {
            catalog
                .catalogs
                .entry(r.catalog_name.clone())
                .or_insert(Catalog {
                    name: r.catalog_name.clone(),
                    schemas: HashMap::new(),
                })
                .schemas
                .entry(r.schema_name.clone())
                .or_insert(Schema {
                    _name: r.schema_name.clone(),
                    tables: HashMap::new(),
                })
                .tables
                .entry(r.name.clone())
                .or_insert(Table {
                    name: r.name,
                    schema_name: r.schema_name,
                    catalog_name: r.catalog_name,
                    table_type: r.table_type,
                    data_source_format: r.data_source_format,
                    updated_at: DateTime::from_timestamp_millis(r.updated_at).unwrap_or(Utc::now()),
                    _updated_by: r.updated_by,
                    _properties: r.properties,
                });
        }
        return catalog;
    }
}
