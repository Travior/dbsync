use log::info;
use reqwest_middleware::{
    reqwest::{self},
    ClientBuilder, ClientWithMiddleware,
};
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Deserialize, Debug)]
pub struct Schema {
    pub name: String,
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

pub struct APIClient {
    host: String,
    client: ClientWithMiddleware,
    pat: String,
}

impl APIClient {
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

    async fn paginate_request<T>(
        &self,
        endpoint: &str,
        query: &[(&str, &str)],
    ) -> Result<Vec<T>, Box<dyn std::error::Error>>
    where
        T: for<'de> Deserialize<'de>,
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

    pub async fn collect_schemas(
        &self,
        catalog: &str,
    ) -> Result<Vec<Schema>, Box<dyn std::error::Error>> {
        info!("Fetching schemas for path {}", catalog);
        let data: Vec<GetSchemaResponse> = self
            .paginate_request(
                "api/2.1/unity-catalog/schemas",
                &[("catalog_name", catalog)],
            )
            .await?;
        Ok(data.into_iter().map(|i| i.schemas).flatten().collect())
    }

    pub async fn collect_tables(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<Vec<Table>, Box<dyn std::error::Error>> {
        info!("Fetching tables for path {}.{}", catalog, schema);
        let data: Vec<GetTableResponse> = self
            .paginate_request(
                "api/2.1/unity-catalog/tables",
                &[("catalog_name", catalog), ("schema_name", schema)],
            )
            .await?;

        Ok(data
            .into_iter()
            .map(|i| i.tables.unwrap_or_default())
            .flatten()
            .collect())
    }
}
