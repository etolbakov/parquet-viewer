mod schema;
use codee::string::FromToStringCodec;
use datafusion::physical_plan::ExecutionPlan;
use file_reader::{get_stored_value, FileReader};
use leptos_use::{
    use_interval_fn, use_timestamp, use_websocket_with_options, ReconnectLimit,
    UseWebSocketOptions, UseWebSocketReturn,
};
use opendal::{services::Http, Operator};
use query_results::QueryResults;
use schema::SchemaSection;

mod file_reader;
mod query_results;
mod row_group;

mod metadata;
use metadata::MetadataSection;

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use leptos::{logging, prelude::*};
use parquet::{
    arrow::parquet_to_arrow_schema,
    errors::ParquetError,
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};

mod query_input;
use query_input::{execute_query_inner, QueryInput};

mod settings;
use settings::{Settings, ANTHROPIC_API_KEY, WS_ENDPOINT_KEY};

use std::fmt::Display;

#[derive(Debug, Clone, PartialEq)]
struct ParquetInfo {
    file_size: u64,
    uncompressed_size: u64,
    compression_ratio: f64,
    row_group_count: u64,
    row_count: u64,
    columns: u64,
    has_row_group_stats: bool,
    has_column_index: bool,
    has_page_index: bool,
    has_bloom_filter: bool,
    schema: SchemaRef,
    metadata: Arc<ParquetMetaData>,
    metadata_len: u64,
}

impl ParquetInfo {
    fn from_metadata(metadata: ParquetMetaData, metadata_len: u64) -> Result<Self, ParquetError> {
        let compressed_size = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.compressed_size())
            .sum::<i64>() as u64;
        let uncompressed_size = metadata
            .row_groups()
            .iter()
            .map(|rg| rg.total_byte_size())
            .sum::<i64>() as u64;

        let schema = parquet_to_arrow_schema(
            metadata.file_metadata().schema_descr(),
            metadata.file_metadata().key_value_metadata(),
        )?;
        let first_row_group = metadata.row_groups().first();
        let first_column = first_row_group.and_then(|rg| rg.columns().first());

        Ok(Self {
            file_size: compressed_size,
            uncompressed_size,
            compression_ratio: compressed_size as f64 / uncompressed_size as f64,
            row_group_count: metadata.num_row_groups() as u64,
            row_count: metadata.file_metadata().num_rows() as u64,
            columns: schema.fields.len() as u64,
            has_row_group_stats: first_column
                .map(|c| c.statistics().is_some())
                .unwrap_or(false),
            has_column_index: metadata.column_index().is_some(),
            has_page_index: metadata.offset_index().is_some(),
            has_bloom_filter: first_column
                .map(|c| c.bloom_filter_offset().is_some())
                .unwrap_or(false),
            schema: Arc::new(schema),
            metadata: Arc::new(metadata),
            metadata_len,
        })
    }
}

fn get_parquet_info(bytes: Bytes) -> Result<ParquetInfo, ParquetError> {
    let mut footer = [0_u8; 8];
    footer.copy_from_slice(&bytes[bytes.len() - 8..]);
    let metadata_len = ParquetMetaDataReader::decode_footer(&footer)?;

    let mut metadata_reader = ParquetMetaDataReader::new()
        .with_page_indexes(true)
        .with_column_indexes(true)
        .with_offset_indexes(true);
    metadata_reader.try_parse(&bytes)?;
    let metadata = metadata_reader.finish()?;

    let parquet_info = ParquetInfo::from_metadata(metadata, metadata_len as u64)?;

    Ok(parquet_info)
}

fn format_rows(rows: u64) -> String {
    let mut result = rows.to_string();
    let mut i = result.len();
    while i > 3 {
        i -= 3;
        result.insert(i, ',');
    }
    result
}

impl std::fmt::Display for ParquetInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "File Size: {} MB\nRow Groups: {}\nTotal Rows: {}\nColumns: {}\nFeatures: {}{}{}{}",
            self.file_size as f64 / 1_048_576.0, // Convert bytes to MB
            self.row_group_count,
            self.row_count,
            self.columns,
            if self.has_row_group_stats {
                "✓ Statistics "
            } else {
                "✗ Statistics "
            },
            if self.has_column_index {
                "✓ Column Index "
            } else {
                "✗ Column Index "
            },
            if self.has_page_index {
                "✓ Page Index "
            } else {
                "✗ Page Index "
            },
            if self.has_bloom_filter {
                "✓ Bloom Filter"
            } else {
                "✗ Bloom Filter"
            },
        )
    }
}

async fn execute_query_async(
    query: String,
    bytes: Bytes,
    table_name: String,
    parquet_info: ParquetInfo,
) -> Result<(Vec<arrow::array::RecordBatch>, Arc<dyn ExecutionPlan>), String> {
    let (results, physical_plan) = execute_query_inner(&table_name, parquet_info, bytes, &query)
        .await
        .map_err(|e| format!("Failed to execute query: {}", e))?;

    Ok((results, physical_plan))
}

#[derive(Clone)]
pub struct WebsocketContext {
    pub message: Signal<Option<String>>,
    send: Arc<dyn Fn(&String) + Send + Sync>,
}

impl WebsocketContext {
    pub fn new(message: Signal<Option<String>>, send: Arc<dyn Fn(&String) + Send + Sync>) -> Self {
        Self { message, send }
    }

    pub fn send(&self, message: &str) {
        (self.send)(&message.to_string())
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum WebSocketMessage {
    Sql {
        query: String,
    },
    ParquetFile {
        file_name: String,
        server_address: String,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
struct AckMessage {
    message_type: String,
}

impl AckMessage {
    fn new() -> Self {
        Self {
            message_type: "ack".to_string(),
        }
    }

    fn new_json() -> String {
        serde_json::to_string(&Self::new()).unwrap()
    }
}

#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    pub last_message_time: Option<f64>,
    pub display_time: RwSignal<String>,
}

impl ConnectionInfo {
    pub fn new() -> Self {
        Self {
            last_message_time: None,
            display_time: RwSignal::new("never".to_string()),
        }
    }
}

impl Default for ConnectionInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for ConnectionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_time.get())
    }
}

#[component]
fn App() -> impl IntoView {
    let (error_message, set_error_message) = signal(Option::<String>::None);
    let (file_bytes, set_file_bytes) = signal(None::<Bytes>);
    let (user_input, set_user_input) = signal(String::new());
    let (sql_query, set_sql_query) = signal(String::new());
    let (query_result, set_query_result) = signal(Vec::<arrow::array::RecordBatch>::new());
    let (file_name, set_file_name) = signal(String::from("uploaded"));
    let (physical_plan, set_physical_plan) = signal(None::<Arc<dyn ExecutionPlan>>);
    let (show_settings, set_show_settings) = signal(false);
    let (connection_info, set_connection_info) = signal(ConnectionInfo::new());
    let api_key = get_stored_value(ANTHROPIC_API_KEY, "");

    let parquet_info = Memo::new(move |_| {
        file_bytes
            .get()
            .and_then(|bytes| get_parquet_info(bytes.clone()).ok())
    });

    let ws_url = get_stored_value(WS_ENDPOINT_KEY, "ws://localhost:12306");
    let UseWebSocketReturn { message, send, .. } =
        use_websocket_with_options::<String, String, FromToStringCodec>(
            &ws_url,
            UseWebSocketOptions::default()
                .reconnect_limit(ReconnectLimit::Infinite)
                .reconnect_interval(1000),
        );

    let send = Arc::new(send);
    Effect::watch(
        message,
        move |message, _, _| {
            if let Some(message) = message {
                set_connection_info.update(|info| {
                    info.last_message_time = Some(use_timestamp()());
                    info.display_time.set("0s ago".to_string());
                });

                let message = serde_json::from_str::<WebSocketMessage>(message).unwrap();
                match message {
                    WebSocketMessage::Sql { query } => {
                        // Send acknowledgment
                        let ack_json = AckMessage::new_json();
                        send(&ack_json);
                        set_user_input.set(query.clone());
                    }
                    WebSocketMessage::ParquetFile {
                        file_name,
                        server_address,
                    } => {
                        logging::log!(
                            "Received file: {}, server_address: {}",
                            file_name,
                            server_address
                        );
                        let builder = Http::default().endpoint(&server_address);
                        let Ok(op) = Operator::new(builder) else {
                            set_error_message.set(Some("Failed to create HTTP operator".into()));
                            return;
                        };
                        let op = op.finish();
                        let send_inner = send.clone();
                        leptos::task::spawn_local(async move {
                            loop {
                                match op.read(&file_name).await {
                                    Ok(bs) => {
                                        send_inner(&AckMessage::new_json());
                                        set_file_bytes.set(Some(bs.to_bytes()));
                                        set_file_name.set(file_name.clone());
                                        logging::log!("read file success");
                                        return;
                                    }
                                    Err(e) => {
                                        logging::log!("read file failed: {}", e);
                                    }
                                }
                            }
                        });
                    }
                }
            }
        },
        true,
    );

    Effect::watch(
        parquet_info,
        move |info, _, _| {
            if let Some(info) = info {
                logging::log!("{}", info.to_string());
                let default_query =
                    format!("select * from \"{}\" limit 10", file_name.get_untracked());
                set_user_input.set(default_query);
            }
        },
        true,
    );

    Effect::watch(
        user_input,
        move |user_input, _, _| {
            let user_input = user_input.clone();
            let api_key = api_key.clone();
            leptos::task::spawn_local(async move {
                let Some(parquet_info) = parquet_info() else {
                    return;
                };
                let sql = match query_input::user_input_to_sql(
                    &user_input,
                    &parquet_info.schema,
                    &file_name(),
                    &api_key,
                )
                .await
                {
                    Ok(response) => response,
                    Err(e) => {
                        logging::log!("{}", e);
                        set_error_message.set(Some(e));
                        return;
                    }
                };
                logging::log!("{}", sql);
                set_sql_query.set(sql);
            });
        },
        true,
    );

    Effect::watch(
        sql_query,
        move |query, _, _| {
            let bytes_opt = file_bytes.get();
            let table_name = file_name.get();
            set_error_message.set(None);

            if query.trim().is_empty() {
                return;
            }

            if let Some(bytes) = bytes_opt {
                let parquet_info = match parquet_info() {
                    Some(content) => content,
                    None => {
                        set_error_message.set(Some("Failed to get file schema".into()));
                        return;
                    }
                };

                let query = query.clone();

                leptos::task::spawn_local(async move {
                    match execute_query_async(query.clone(), bytes, table_name, parquet_info).await
                    {
                        Ok((results, physical_plan)) => {
                            set_physical_plan.set(Some(physical_plan));
                            set_query_result.set(results);
                        }
                        Err(e) => set_error_message.set(Some(e)),
                    }
                });
            } else {
                set_error_message.set(Some("No Parquet file loaded.".into()));
            }
        },
        true,
    );

    // Set up the interval in the component
    use_interval_fn(
        move || {
            set_connection_info.update(|info| {
                if let Some(last_time) = info.last_message_time {
                    let current = use_timestamp()();
                    let seconds = ((current - last_time) / 1000.0).round() as i64;
                    info.display_time.set(format!("{}s ago", seconds));
                }
            });
        },
        1000,
    );

    view! {
        <div class="container mx-auto px-4 py-8 max-w-6xl">
            <h1 class="text-3xl font-bold mb-8 flex items-center justify-between">
                <span>"Parquet Viewer"</span>
                <div class="flex items-center gap-4">
                    <button
                        on:click=move |_| set_show_settings.set(true)
                        class="text-gray-600 hover:text-gray-800"
                        title="Settings"
                    >
                        <svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path
                                stroke-linecap="round"
                                stroke-linejoin="round"
                                stroke-width="2"
                                d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z"
                            ></path>
                            <path
                                stroke-linecap="round"
                                stroke-linejoin="round"
                                stroke-width="2"
                                d="M15 12a3 3 0 11-6 0 3 3 0 016 0z"
                            ></path>
                        </svg>
                    </button>
                    <a
                        href="https://github.com/XiangpengHao/parquet-viewer"
                        target="_blank"
                        class="text-gray-600 hover:text-gray-800"
                    >
                        <svg height="24" width="24" viewBox="0 0 16 16">
                            <path
                                fill="currentColor"
                                d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"
                            ></path>
                        </svg>
                    </a>
                </div>
            </h1>
            <div class="space-y-6">
                <FileReader
                    set_error_message=set_error_message
                    set_file_bytes=set_file_bytes
                    set_file_name=set_file_name
                />

                <div class="border-t border-gray-300 my-4"></div>

                {move || {
                    error_message
                        .get()
                        .map(|msg| {
                            view! {
                                <div class="bg-red-50 border-l-4 border-red-500 p-4 my-4">
                                    <div class="text-red-700">{msg}</div>
                                    <div class="mt-2 text-sm text-gray-600">
                                        "Tips:" <ul class="list-disc ml-6 mt-2 space-y-1">
                                            <li>"Make sure the URL has CORS enabled."</li>
                                            <li>
                                                "If query with natural language, make sure to set the Anthropic API key."
                                            </li>
                                            <li>
                                                "I usually download the file and use the file picker above."
                                            </li>
                                        </ul>
                                    </div>
                                </div>
                            }
                        })
                }}
                <div class="mt-4">
                    {move || {
                        file_bytes
                            .get()
                            .map(|_| {
                                match parquet_info() {
                                    Some(info) => {
                                        if info.row_group_count > 0 {
                                            view! {
                                                <QueryInput
                                                    user_input=user_input
                                                    set_user_input=set_user_input
                                                />
                                            }
                                                .into_any()
                                        } else {
                                            ().into_any()
                                        }
                                    }
                                    None => ().into_any(),
                                }
                            })
                    }}
                </div>

                {move || {
                    let result = query_result.get();
                    if result.is_empty() {
                        ().into_any()
                    } else {
                        let physical_plan = physical_plan.get().unwrap();
                        view! {
                            <QueryResults
                                sql_query=sql_query.get()
                                query_result=result
                                physical_plan=physical_plan
                            />
                        }
                            .into_any()
                    }
                }}

                <div class="mt-8">
                    {move || {
                        let info = parquet_info();
                        match info {
                            Some(info) => {
                                view! {
                                    <div class="space-y-6">
                                        <div class="w-full">
                                            <MetadataSection parquet_info=info.clone() />
                                        </div>
                                        <div class="w-full">
                                            <SchemaSection parquet_info=info.clone() />
                                        </div>
                                    </div>
                                }
                                    .into_any()
                            }
                            None => {
                                view! {
                                    <div class="text-center text-gray-500 py-8">
                                        "No file selected"
                                    </div>
                                }
                                    .into_any()
                            }
                        }
                    }}
                </div>

            </div>
            <Settings
                show=show_settings
                set_show=set_show_settings
                connection_info=connection_info
            />
        </div>
    }
}

fn main() {
    console_error_panic_hook::set_once();
    mount_to_body(|| view! { <App /> })
}
