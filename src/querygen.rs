use crate::unitycatalog::{DiffNode, Operation};

pub struct Query {
    pub query: Option<String>,
    pub is_fast: bool,
    pub children: Vec<Query>,
}

impl Query {
    pub fn from_diff_node(diff_node: &DiffNode, target_catalog: &str) -> Self {
        match &diff_node.operation {
            None => {
                Self {
                    query: None,
                    is_fast: false,
                    children: diff_node.children.iter().map(|child| Self::from_diff_node(child, target_catalog)).collect(),
                }
            }
            Some(operation) => {
                match operation {
                    Operation::CreateCatalog(_catalog) => {
                        Self {
                            query: Some(format!("CREATE CATALOG {}", target_catalog)),
                            is_fast: true,
                            children: diff_node.children.iter().map(|child| Self::from_diff_node(child, target_catalog)).collect(),
                        }
                    }
                    Operation::CreateSchema(schema) => {
                        Self {
                            query: Some(format!("CREATE SCHEMA {}.{}", target_catalog, schema._name)),
                            is_fast: true,
                            children: diff_node.children.iter().map(|child| Self::from_diff_node(child, target_catalog)).collect(),
                        }
                    }
                    Operation::DropCatalog(catalog) => {
                        Self {
                            query: Some(format!("DROP CATALOG {} CASCADE", catalog.name)),
                            is_fast: true,
                            children: vec![], // no children because delete gets cascaded
                        }
                    }
                    Operation::DropSchema(schema) => {
                        Self {
                            query: Some(format!("DROP SCHEMA {}.{} CASCADE", target_catalog, schema._name)),
                            is_fast: true,
                            children: vec![], // no children because delete gets cascaded
                        }
                    }
                    Operation::DropTable(table) => {
                        Self {
                            query: Some(format!("DROP TABLE {}.{}.{}", target_catalog, table.schema_name, table.name)),
                            is_fast: true,
                            children: vec![], // no children because delete table is always leaf node
                        }
                    }
                    Operation::CloneTable { source, target } => {
                        let mut queries = vec![];
                        
                        // If there's an existing table to replace, drop it first
                        if let Some(existing_table) = target {
                            queries.push(format!("DROP TABLE {}.{}.{}", target_catalog, existing_table.schema_name, existing_table.name));
                        }
                        
                        // Create the clone
                        queries.push(format!("CREATE TABLE {}.{}.{} SHALLOW CLONE {}.{}.{}", 
                            target_catalog, source.schema_name, source.name, 
                            source.catalog_name, source.schema_name, source.name));
                        
                        Self {
                            query: Some(queries.join(";\n")),
                            is_fast: true,
                            children: vec![],
                        }
                    }
                }
            }
        }
    }
}