use chrono::Utc;
use chrono::{DateTime, Duration};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

#[derive(Debug)]
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

#[derive(Debug)]
pub struct Schema {
    pub _name: String,
    pub _catalog_name: String,
    pub tables: HashMap<String, Table>,
}

#[derive(Debug)]
pub struct Catalog {
    pub name: String,
    pub schemas: HashMap<String, Schema>,
}

pub enum UnityCatalogElement {
    Catalog(Catalog),
    Schema(Schema),
    Table(Table),
}
pub struct UnityCatalog {
    pub catalogs: HashMap<String, Catalog>,
}

impl UnityCatalog {
    pub fn new() -> Self {
        Self {
            catalogs: HashMap::new(),
        }
    }

    pub fn insert_assume_ordered(&mut self, element: UnityCatalogElement) {
        match element {
            UnityCatalogElement::Catalog(catalog) => {
                self.catalogs.insert(catalog.name.clone(), catalog);
            }
            UnityCatalogElement::Schema(schema) => {
                self.catalogs
                    .get_mut(&schema._catalog_name)
                    .unwrap()
                    .schemas
                    .insert(schema._name.clone(), schema);
            }
            UnityCatalogElement::Table(table) => {
                self.catalogs
                    .get_mut(&table.catalog_name)
                    .unwrap()
                    .schemas
                    .get_mut(&table.schema_name)
                    .unwrap()
                    .tables
                    .insert(table.name.clone(), table);
            }
        }
    }
}

#[derive(Debug)]
pub enum Object<'a> {
    Catalog(&'a Catalog),
    Schema(&'a Schema),
    Table(&'a Table),
}

impl<'a> Object<'a> {
    pub fn to_path(&self) -> String {
        match self {
            Object::Catalog(catalog) => catalog.name.clone(),
            Object::Schema(schema) => format!("{}.{}", schema._catalog_name, schema._name),
            Object::Table(table) => format!("{}.{}.{}", table.catalog_name, table.schema_name, table.name),
        }
    }
}

#[derive(Debug)]
pub enum Operation<'a> {
    CreateCatalog(&'a Catalog),
    CreateSchema(&'a Schema),
    DropCatalog(&'a Catalog),
    DropSchema(&'a Schema),
    DropTable(&'a Table),
    CloneTable {
        source: &'a Table,
        target: Option<&'a Table>, // None for new table, Some for existing table to replace
    },
}

pub struct DiffNode<'a> {
    pub operation: Option<Operation<'a>>,
    pub children: Vec<DiffNode<'a>>,
}

impl<'a> DiffNode<'a> {
    fn format_tree(&self, f: &mut std::fmt::Formatter<'_>, indent: usize) -> std::fmt::Result {
        for _ in 0..indent {
            write!(f, "│ ")?;
        }
        match &self.operation {
            Some(operation) => {
                writeln!(f, "├─ {:?}", operation)?;
            }
            None => {
                writeln!(f, "├─ <noop>")?;
            }
        }
        for child in &self.children {
            child.format_tree(f, indent + 1)?;
        }
        Ok(())
    }
}

impl<'a> Debug for DiffNode<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.format_tree(f, 0)
    }
}

pub trait DiffTree<'a> {
    fn diff(&'a self, other: Option<&'a Self>) -> Option<DiffNode<'a>>;
}

impl<'a> DiffTree<'a> for Catalog {
    fn diff(&'a self, other: Option<&'a Catalog>) -> Option<DiffNode<'a>> {
        match other {
            Some(other) => {
                let mut children = vec![];
                let a_keys = self.schemas.keys().collect::<HashSet<&String>>();
                let b_keys = other.schemas.keys().collect::<HashSet<&String>>();

                let only_a: Vec<&Schema> = a_keys
                    .difference(&b_keys)
                    .map(|k| self.schemas.get(k.as_str()).unwrap())
                    .collect();
                let only_b: Vec<&Schema> = b_keys
                    .difference(&a_keys)
                    .map(|k| other.schemas.get(k.as_str()).unwrap())
                    .collect();
                let shared: Vec<&Schema> = a_keys
                    .intersection(&b_keys)
                    .map(|k| self.schemas.get(k.as_str()).unwrap())
                    .collect();


                children.extend(
                    shared
                        .iter()
                        .filter_map(|s| s.diff(other.schemas.get(s._name.as_str())))
                );

                children.extend(only_a.iter().filter_map(|s| s.diff(None)));
                children.extend(only_b.iter().map(|s| DiffNode {
                    operation: Some(Operation::DropSchema(s)),
                    children: vec![],
                }));

                if children.len() > 0 {
                    return Some(DiffNode {
                        operation: None,
                        children: children,
                    });
                }
                None
            }
            None => {
                return Some(DiffNode {
                    operation: Some(Operation::CreateCatalog(self)),
                    children: self
                        .schemas
                        .iter()
                        .filter_map(|(_k, v)| v.diff(None))
                        .collect(),
                });
            }
        }
    }
}

impl<'a> DiffTree<'a> for Schema {
    fn diff(&'a self, other: Option<&'a Schema>) -> Option<DiffNode<'a>> {
        match other {
            Some(other) => {
                let mut children = vec![];
                let a_keys = self.tables.keys().collect::<HashSet<&String>>();
                let b_keys = other.tables.keys().collect::<HashSet<&String>>();

                let only_a: Vec<&Table> = a_keys
                    .difference(&b_keys)
                    .map(|k| self.tables.get(k.as_str()).unwrap())
                    .collect();
                let only_b: Vec<&Table> = b_keys
                    .difference(&a_keys)
                    .map(|k| other.tables.get(k.as_str()).unwrap())
                    .collect();
                let shared: Vec<&Table> = a_keys
                    .intersection(&b_keys)
                    .map(|k| self.tables.get(k.as_str()).unwrap())
                    .collect();

                children.extend(
                    shared
                        .iter()
                        .filter_map(|s| s.diff(other.tables.get(s.name.as_str())))
                );

                children.extend(only_a.iter().filter_map(|s| s.diff(None)));
                children.extend(only_b.iter().map(|t| DiffNode {
                    operation: Some(Operation::DropTable(t)),
                    children: vec![],
                }));

                if children.len() > 0 {
                    return Some(DiffNode {
                        operation: None,
                        children: children,
                    });
                }
                None
            }
            None => {
                return Some(DiffNode {
                    operation: Some(Operation::CreateSchema(self)),
                    children: self
                        .tables
                        .iter()
                        .filter_map(|(_k, v)| v.diff(None))
                        .collect(),
                });
            }
        }
    }
}

impl<'a> DiffTree<'a> for Table {
    fn diff(&'a self, other: Option<&'a Table>) -> Option<DiffNode<'a>> {
        match other {
            Some(other) => {
                if self.updated_at - other.updated_at > Duration::days(1) {
                    // TODO: Make duration configurable
                    Some(DiffNode {
                        operation: Some(Operation::CloneTable {
                            source: self,
                            target: Some(other),
                        }),
                        children: vec![],
                    })
                } else {
                    None
                }
            }
            None => {
                return Some(DiffNode {
                    operation: Some(Operation::CloneTable {
                        source: self,
                        target: None,
                    }),
                    children: vec![],
                });
            }
        }
    }
}
