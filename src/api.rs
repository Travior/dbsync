use anyhow::Result;
use reqwest_middleware::{
    reqwest::{self},
    ClientBuilder, ClientWithMiddleware,
};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value;
use std::collections::HashMap;


#[derive(Debug, Deserialize)]
pub struct Catalog {
    pub name: String,
}


#[derive(Deserialize, Debug)]
pub struct GetCatalogResponse {
    pub catalogs: Vec<Catalog>,
}


#[derive(Debug, Deserialize)]
pub struct Schema {
    pub name: String,
    pub catalog_name: String,
}
#[derive(Deserialize, Debug)]
pub struct GetSchemaResponse {
    pub schemas: Vec<Schema>,
}

#[derive(Deserialize, Debug)]
pub struct Table {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub table_type: String,
    pub data_source_format: Option<String>,
    pub updated_at: i64,
    pub updated_by: String,
    pub properties: HashMap<String, String>,
}

#[derive(Deserialize, Debug)]
pub struct GetTableResponse {
    tables: Option<Vec<Table>>,
}


#[derive(Clone)]
pub struct Client {
    host: String,
    client: ClientWithMiddleware,
    pat: String,
}

impl Client {
    pub fn new(host: &str, pat: &str) -> Self {
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
        let client = ClientBuilder::new(reqwest::Client::new())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        Self {
            host: host.to_string(),
            client: client,
            pat: pat.to_string(),
        }
    }
    async fn get<T>(&self, endpoint: &str, query: &[(&str, &str)]) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
    {
        let mut data: Vec<T> = Vec::new();
        let mut next_page: Option<String> = None;
        let req_template = self
            .client
            .get(format!("https://{}/{}", self.host, endpoint))
            .bearer_auth(&self.pat)
            .query(query);
        loop {
            let mut request = req_template.try_clone().unwrap();
            if let Some(token) = &next_page {
                request = request.query(&[("page_token", token)]);
            }
            let response = request.send().await?.error_for_status()?;
            let npt = response.text().await?;
            let response_parsed: T = serde_json::from_str(&npt)?;
            data.push(response_parsed);

            let response_npt: Value = serde_json::from_str(&npt)?;
            if let Some(token) = response_npt.get("next_page_token") {
                next_page = Some(token.to_string());
            } else {
                break;
            }
        }
        Ok(data)
    }
}

pub struct FetchAllCatalogs {}

pub struct FetchCatalog {
    pub catalog_name: String,
}

pub struct FetchSchema {
    pub catalog_name: String,
    pub schema_name: String,
}


pub enum FetchJob {
    FetchAllCatalogs(FetchAllCatalogs),
    FetchCatalog(FetchCatalog),
    FetchSchema(FetchSchema),
}

impl FetchAllCatalogs {
    pub async fn get_children(self, client: &Client) -> Result<Vec<Catalog>> {
        let catalogs = client
            .get::<GetCatalogResponse>("api/2.1/unity-catalog/catalogs", &[]).await?;
        Ok(catalogs.into_iter().map(|c| c.catalogs).flatten().collect())
    }
}

impl FetchCatalog {
    pub async fn get_children(&self, client: &Client) -> Result<Vec<Schema>> {
        let schemas = client
            .get::<GetSchemaResponse>(
                "api/2.1/unity-catalog/schemas",
                &[("catalog_name", &self.catalog_name)],
            )
            .await?;
        Ok(schemas.into_iter().map(|s| s.schemas).flatten().collect())
    }
}

impl FetchSchema {
    pub async fn get_children(&self, client: &Client) -> Result<Vec<Table>> {
        let tables = client
            .get::<GetTableResponse>(
                "api/2.1/unity-catalog/tables",
                &[("catalog_name", &self.catalog_name), ("schema_name", &self.schema_name)],
            )
            .await?;
        Ok(tables.into_iter().map(|t| t.tables.unwrap_or_default()).flatten().collect())
    }
}

