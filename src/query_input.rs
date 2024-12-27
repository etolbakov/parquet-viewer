use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::{
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    physical_plan::{collect, ExecutionPlan},
    prelude::{ParquetReadOptions, SessionConfig},
};
use leptos::{logging, prelude::*};
use leptos::{
    reactive::wrappers::write::SignalSetter,
    wasm_bindgen::{JsCast, JsValue},
};
use serde_json::json;
use wasm_bindgen_futures::JsFuture;
use web_sys::{js_sys, Headers, Request, RequestInit, RequestMode, Response};

use crate::INMEMORY_STORE;

pub(crate) async fn execute_query_inner(
    table_name: &str,
    query: &str,
) -> Result<(Vec<RecordBatch>, Arc<dyn ExecutionPlan>), DataFusionError> {
    let mut config = SessionConfig::new();
    config.options_mut().sql_parser.dialect = "PostgreSQL".to_string();

    let ctx = datafusion::prelude::SessionContext::new_with_config(config);

    let object_store_url = ObjectStoreUrl::parse("mem://").unwrap();
    let object_store = INMEMORY_STORE.clone();
    ctx.register_object_store(object_store_url.as_ref(), object_store);
    ctx.register_parquet(
        table_name,
        &format!("mem:///{}.parquet", table_name),
        ParquetReadOptions::default(),
    )
    .await?;

    let plan = ctx.sql(query).await?;

    let (state, plan) = plan.into_parts();
    let plan = state.optimize(&plan)?;

    logging::log!("{}", &plan.display_indent());

    let physical_plan = state.create_physical_plan(&plan).await?;

    let results = collect(physical_plan.clone(), ctx.task_ctx().clone()).await?;
    Ok((results, physical_plan))
}

#[component]
pub fn QueryInput(
    user_input: Memo<Option<String>>,
    set_user_input: SignalSetter<Option<String>>,
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
                    on:input=move |ev| set_input_value(Some(event_target_value(&ev)))
                    prop:value=input_value
                    on:keydown=key_down
                    class="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
                <div class="flex items-center gap-1">
                    <button
                        on:click=button_press
                        class="px-4 py-2 bg-green-500 text-white rounded-md hover:bg-green-600 whitespace-nowrap"
                    >
                        "Run Query"
                    </button>
                    <div class="relative group">
                        <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 text-gray-500 hover:text-gray-700 cursor-help" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                        </svg>
                        <div class="absolute bottom-full right-0 mb-2 w-64 p-2 bg-gray-800 text-white text-xs rounded shadow-lg opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none">
                            "Query starts with 'SELECT' run as SQL, otherwise it is a question to be answered by AI generated SQL" 
                        </div>
                    </div>
                </div>
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
    logging::log!("Processing user input: {}", input);

    let prompt = format!(
        "Generate a SQL query to answer the following question: {}. You should generate PostgreSQL SQL dialect, all field names and table names should be double quoted, and the output SQL should be executable, be careful about the available columns. The table name is: {}, the schema of the table is: {}.  ",
        input, file_name, schema_str
    );
    logging::log!("{}", prompt);

    let sql = match generate_sql_via_claude(&prompt, api_key).await {
        Ok(response) => response,
        Err(e) => {
            logging::log!("{}", e);
            let claude_error = format!("Failed to generate SQL through Claude: {}", e);
            return Err(claude_error);
        }
    };
    logging::log!("{}", sql);
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
        .set("x-api-key", api_key)
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
    let request = Request::new_with_str_and_init(url, &opts)
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
