use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{
        collect, stream::RecordBatchStreamAdapter, streaming::PartitionStream, ExecutionPlan,
    },
    prelude::SessionConfig,
};
use leptos::prelude::*;
use leptos::wasm_bindgen::{JsCast, JsValue};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use wasm_bindgen_futures::JsFuture;
use web_sys::{js_sys, Headers, Request, RequestInit, RequestMode, Response};

use crate::ParquetInfo;

use futures::StreamExt;
#[derive(Debug)]
struct DummyStreamPartition {
    schema: SchemaRef,
    bytes: Bytes,
}

impl PartitionStream for DummyStreamPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let parquet_builder = ParquetRecordBatchReaderBuilder::try_new(self.bytes.clone()).unwrap();
        let reader = parquet_builder.build().unwrap();
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::iter(reader)
                .map(|batch| batch.map_err(|e| DataFusionError::ArrowError(e, None))),
        ))
    }
}

pub(crate) async fn execute_query_inner(
    table_name: &str,
    parquet_info: ParquetInfo,
    data: Bytes,
    query: &str,
) -> Result<(Vec<RecordBatch>, Arc<dyn ExecutionPlan>), DataFusionError> {
    web_sys::console::log_1(&table_name.into());
    let mut config = SessionConfig::new();
    config.options_mut().sql_parser.dialect = "PostgreSQL".to_string();

    let ctx = datafusion::prelude::SessionContext::new_with_config(config);

    let schema = parquet_info.schema.clone();

    let streaming_table = datafusion::datasource::streaming::StreamingTable::try_new(
        schema.clone(),
        vec![Arc::new(DummyStreamPartition {
            schema: schema.clone(),
            bytes: data.clone(),
        })],
    )?;

    ctx.register_table(table_name, Arc::new(streaming_table))?;

    let plan = ctx.sql(&query).await?;

    let (state, plan) = plan.into_parts();
    let plan = state.optimize(&plan)?;

    web_sys::console::log_1(&plan.display_indent().to_string().into());

    let physical_plan = state.create_physical_plan(&plan).await?;

    let results = collect(physical_plan.clone(), ctx.task_ctx().clone()).await?;
    Ok((results, physical_plan))
}

#[component]
pub fn QueryInput(
    user_input: ReadSignal<String>,
    set_user_input: WriteSignal<String>,
) -> impl IntoView {
    let (api_key, _) = signal({
        let window = web_sys::window().unwrap();
        window
            .local_storage()
            .unwrap()
            .unwrap()
            .get_item("claude_api_key")
            .unwrap()
            .unwrap_or_default()
    });

    Effect::new(move |_| {
        if let Some(window) = web_sys::window() {
            if let Ok(Some(storage)) = window.local_storage() {
                let _ = storage.set_item("claude_api_key", &api_key.get());
            }
        }
    });

    let (input_value, set_input_value) = signal(user_input.get_untracked());

    Effect::new(move |_| {
        set_input_value.set(user_input.get());
    });

    let key_down = move |ev: web_sys::KeyboardEvent| {
        if ev.key() == "Enter" {
            let input = input_value.get();
            set_user_input.set(input.clone());
        }
    };

    let button_press = move |_ev: web_sys::MouseEvent| {
        let input = input_value.get();
        set_user_input.set(input.clone());
    };

    view! {
        <div class="flex gap-2 items-center flex-col relative">
            <div class="w-full flex gap-2 items-center">
                <input
                    type="text"
                    on:input=move |ev| set_input_value(event_target_value(&ev))
                    prop:value=input_value
                    on:keydown=key_down
                    class="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
                <button
                    on:click=button_press
                    class="px-4 py-2 bg-green-500 text-white rounded-md hover:bg-green-600 whitespace-nowrap"
                >
                    "Run Query"
                </button>
            </div>
        </div>
    }
}

pub(crate) async fn user_input_to_sql(
    input: &str,
    schema: &SchemaRef,
    file_name: &str,
    api_key: &str,
) -> Result<String, String> {
    // if the input seems to be a SQL query, return it as is
    if input.starts_with("select") || input.starts_with("SELECT") {
        return Ok(input.to_string());
    }

    // otherwise, treat it as some natural language

    let schema_str = schema_to_brief_str(schema);
    web_sys::console::log_1(&format!("Processing user input: {}", input).into());

    let prompt = format!(
        "Generate a SQL query to answer the following question: {}. You should generate PostgreSQL SQL dialect, all field names and table names should be double quoted, and the output SQL should be executable, be careful about the available columns. The table name is: {}, the schema of the table is: {}.  ",
        input, file_name, schema_str
    );
    web_sys::console::log_1(&prompt.clone().into());

    let sql = match generate_sql_via_claude(&prompt, api_key).await {
        Ok(response) => response,
        Err(e) => {
            web_sys::console::log_1(&e.clone().into());
            let claude_error = format!("Failed to generate SQL through Claude: {}", e);
            return Err(claude_error);
        }
    };
    web_sys::console::log_1(&sql.clone().into());
    Ok(sql)
}

fn schema_to_brief_str(schema: &SchemaRef) -> String {
    let fields = schema.fields();
    let field_strs = fields
        .iter()
        .map(|field| format!("{}: {}", field.name(), field.data_type()));
    field_strs.collect::<Vec<_>>().join(", ")
}

// Asynchronous function to call the Claude API
async fn generate_sql_via_claude(prompt: &str, api_key: &str) -> Result<String, String> {
    let url = "https://api.anthropic.com/v1/messages";

    let payload = json!({
        "model": "claude-3-haiku-20240307",
        "max_tokens": 1024,
        "messages": [{
            "role": "user",
            "content": prompt
        }],
        "system": "You are a SQL query generator. You should only respond with the generated SQL query. Do not include any explanation, JSON wrapping, or additional text."
    });

    let opts = RequestInit::new();
    opts.set_method("POST");
    opts.set_mode(RequestMode::Cors);

    // Update headers according to docs
    let headers = Headers::new().map_err(|e| format!("Failed to create headers: {:?}", e))?;
    headers
        .set("content-type", "application/json")
        .map_err(|e| format!("Failed to set Content-Type: {:?}", e))?;
    headers
        .set("anthropic-version", "2023-06-01")
        .map_err(|e| format!("Failed to set Anthropic version: {:?}", e))?;
    headers
        .set("x-api-key", &api_key)
        .map_err(|e| format!("Failed to set API key: {:?}", e))?;
    headers
        .set("anthropic-dangerous-direct-browser-access", "true")
        .map_err(|e| format!("Failed to set browser access header: {:?}", e))?;
    opts.set_headers(&headers);

    // Set body
    let body =
        serde_json::to_string(&payload).map_err(|e| format!("JSON serialization error: {}", e))?;
    opts.set_body(&JsValue::from_str(&body));

    // Create Request
    let request = Request::new_with_str_and_init(&url, &opts)
        .map_err(|e| format!("Request creation failed: {:?}", e))?;

    // Send the request
    let window = web_sys::window().ok_or("No global `window` exists")?;
    let response_value = JsFuture::from(window.fetch_with_request(&request))
        .await
        .map_err(|e| format!("Fetch error: {:?}", e))?;

    // Convert the response to a WebSys Response object
    let response: Response = response_value
        .dyn_into()
        .map_err(|e| format!("Response casting failed: {:?}", e))?;

    if !response.ok() {
        return Err(format!(
            "Network response was not ok: {}",
            response.status()
        ));
    }

    // Parse the JSON response
    let json = JsFuture::from(
        response
            .json()
            .map_err(|e| format!("Failed to parse JSON: {:?}", e))?,
    )
    .await
    .map_err(|e| format!("JSON parsing error: {:?}", e))?;

    // Simplified response parsing
    let json_value: serde_json::Value = serde_json::from_str(
        &js_sys::JSON::stringify(&json)
            .map_err(|e| format!("Failed to stringify JSON: {:?}", e))?
            .as_string()
            .ok_or("Failed to convert to string")?,
    )
    .map_err(|e| format!("Failed to parse JSON value: {:?}", e))?;

    // Extract the SQL directly from the content
    let sql = json_value
        .get("content")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("text"))
        .and_then(|t| t.as_str())
        .ok_or("Failed to extract SQL from response")?
        .trim()
        .to_string();

    Ok(sql)
}
