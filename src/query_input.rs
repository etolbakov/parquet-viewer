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
use leptos::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use wasm_bindgen::{JsCast, JsValue};
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
    sql_query: ReadSignal<String>,
    set_sql_query: WriteSignal<String>,
    file_name: ReadSignal<String>,
    execute_query: Arc<dyn Fn(String)>,
    schema: SchemaRef,
) -> impl IntoView {
    let key_down_schema = schema.clone();
    let key_down_exec = execute_query.clone();
    let file_name_s = file_name.get_untracked();
    let key_down = move |ev: web_sys::KeyboardEvent| {
        if ev.key() == "Enter" {
            let input = sql_query.get_untracked();
            process_user_input(
                input,
                key_down_schema.clone(),
                file_name_s.clone(),
                key_down_exec.clone(),
                set_sql_query.clone(),
            );
        }
    };

    let key_down_exec = execute_query.clone();
    let button_press_schema = schema.clone();
    let file_name_s = file_name.get_untracked();
    let button_press = move |_ev: web_sys::MouseEvent| {
        let input = sql_query.get_untracked();
        process_user_input(
            input,
            button_press_schema.clone(),
            file_name_s.clone(),
            key_down_exec.clone(),
            set_sql_query.clone(),
        );
    };

    let default_query = format!("select * from \"{}\" limit 10", file_name.get_untracked());

    view! {
        <div class="flex gap-2 items-center">
            <input
                type="text"
                placeholder=default_query
                on:input=move |ev| {
                    let value = event_target_value(&ev);
                    set_sql_query(value);
                }
                on:input=move |ev| set_sql_query(event_target_value(&ev))
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
    }
}

fn process_user_input(
    input: String,
    schema: SchemaRef,
    file_name: String,
    exec: Arc<dyn Fn(String)>,
    set_sql_query: WriteSignal<String>,
) {
    // if the input seems to be a SQL query, return it as is
    if input.starts_with("select") || input.starts_with("SELECT") {
        exec(input.clone());
        set_sql_query(input);
        return;
    }

    // otherwise, treat it as some natural language

    let schema_str = schema_to_brief_str(schema);
    web_sys::console::log_1(&format!("Processing user input: {}", input).into());

    let prompt = format!(
        "Generate a SQL query to answer the following question: {}. You should generate PostgreSQL SQL dialect, all field names should be double quoted, and the output SQL should be executable, be careful about the available columns. The table name is: {}, the schema of the table is: {}.  ",
        input, file_name, schema_str
    );
    web_sys::console::log_1(&prompt.clone().into());

    spawn_local({
        let prompt = prompt.clone();
        async move {
            let sql = match generate_sql_via_gemini(prompt).await {
                Ok(response) => response,
                Err(e) => {
                    web_sys::console::log_1(&e.into());
                    return;
                }
            };
            web_sys::console::log_1(&sql.clone().into());
            set_sql_query(sql.clone());
            exec(sql);
        }
    });
}

fn schema_to_brief_str(schema: SchemaRef) -> String {
    let fields = schema.fields();
    let field_strs = fields
        .iter()
        .map(|field| format!("{}: {}", field.name(), field.data_type()));
    field_strs.collect::<Vec<_>>().join(", ")
}

// Asynchronous function to call the Gemini API
async fn generate_sql_via_gemini(prompt: String) -> Result<String, String> {
    // this is free tier key, who cares
    let default_key = "AIzaSyDSEI9ixzvFYQx-e82poEtz8e0bM4omB0Q";

    // Define the API endpoint
    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={}",
        default_key
    );

    // Build the JSON payload
    let payload = json!({
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": prompt
                    }
                ]
            }
        ],
        "generationConfig": {
            "temperature": 1,
            "topK": 40,
            "topP": 0.95,
            "maxOutputTokens": 8192,
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string"
                    }
                }
            }
        }
    });

    // Initialize Request
    let opts = RequestInit::new();
    opts.set_method("POST");
    opts.set_mode(RequestMode::Cors);

    // Set headers
    let headers = Headers::new().map_err(|e| format!("Failed to create headers: {:?}", e))?;
    headers
        .set("Content-Type", "application/json")
        .map_err(|e| format!("Failed to set Content-Type: {:?}", e))?;
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

    // Parse the response to extract just the SQL query
    let json_value: serde_json::Value = serde_json::from_str(
        &js_sys::JSON::stringify(&json)
            .map_err(|e| format!("Failed to stringify JSON: {:?}", e))?
            .as_string()
            .ok_or("Failed to convert to string")?,
    )
    .map_err(|e| format!("Failed to parse JSON value: {:?}", e))?;

    // Navigate the JSON structure to extract the SQL
    let sql = json_value
        .get("candidates")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("content"))
        .and_then(|c| c.get("parts"))
        .and_then(|p| p.get(0))
        .and_then(|p| p.get("text"))
        .and_then(|t| t.as_str())
        .ok_or("Failed to extract SQL from response")?;

    // Parse the inner JSON string to get the final SQL
    let sql_obj: serde_json::Value =
        serde_json::from_str(sql).map_err(|e| format!("Failed to parse SQL JSON: {:?}", e))?;

    let final_sql = sql_obj
        .get("sql")
        .and_then(|s| s.as_str())
        .ok_or("Failed to extract SQL field")?
        .to_string();

    Ok(final_sql)
}
