mod schema;
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{
        collect, stream::RecordBatchStreamAdapter, streaming::PartitionStream, ExecutionPlan,
    },
};
use query_results::QueryResults;
use schema::SchemaSection;

mod query_results;
mod row_group;

mod info;
use info::InfoSection;

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use leptos::*;
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, parquet_to_arrow_schema},
    errors::ParquetError,
    file::{
        metadata::{ParquetMetaData, ParquetMetaDataReader},
        statistics::Statistics,
    },
};
use wasm_bindgen::{prelude::Closure, JsCast};
use wasm_bindgen_futures::JsFuture;
use web_sys::js_sys;

#[derive(Debug, Clone)]
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

        Ok(Self {
            file_size: compressed_size,
            uncompressed_size,
            compression_ratio: compressed_size as f64 / uncompressed_size as f64,
            row_group_count: metadata.num_row_groups() as u64,
            row_count: metadata.file_metadata().num_rows() as u64,
            columns: schema.fields.len() as u64,
            has_row_group_stats: metadata.row_group(0).column(0).statistics().is_some(),
            has_column_index: metadata.column_index().is_some(),
            has_page_index: metadata.offset_index().is_some(),
            has_bloom_filter: metadata
                .row_group(0)
                .column(0)
                .bloom_filter_offset()
                .is_some(),
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

fn stats_to_string(stats: Option<Statistics>) -> String {
    match stats {
        Some(stats) => {
            let mut parts = Vec::new();
            match &stats {
                Statistics::Int32(s) => {
                    if let Some(min) = s.min_opt() {
                        parts.push(format!("min: {}", min));
                    }
                    if let Some(max) = s.max_opt() {
                        parts.push(format!("max: {}", max));
                    }
                }
                Statistics::Int64(s) => {
                    if let Some(min) = s.min_opt() {
                        parts.push(format!("min: {}", min));
                    }
                    if let Some(max) = s.max_opt() {
                        parts.push(format!("max: {}", max));
                    }
                }
                Statistics::Int96(s) => {
                    if let Some(min) = s.min_opt() {
                        parts.push(format!("min: {}", min));
                    }
                    if let Some(max) = s.max_opt() {
                        parts.push(format!("max: {}", max));
                    }
                }
                Statistics::Boolean(s) => {
                    if let Some(min) = s.min_opt() {
                        parts.push(format!("min: {}", min));
                    }
                    if let Some(max) = s.max_opt() {
                        parts.push(format!("max: {}", max));
                    }
                }
                Statistics::Float(s) => {
                    if let Some(min) = s.min_opt() {
                        parts.push(format!("min: {:.2}", min));
                    }
                    if let Some(max) = s.max_opt() {
                        parts.push(format!("max: {:.2}", max));
                    }
                }
                Statistics::Double(s) => {
                    if let Some(min) = s.min_opt() {
                        parts.push(format!("min: {:.2}", min));
                    }
                    if let Some(max) = s.max_opt() {
                        parts.push(format!("max: {:.2}", max));
                    }
                }
                Statistics::ByteArray(s) => {
                    s.min_opt()
                        .and_then(|min| min.as_utf8().ok())
                        .map(|min_utf8| parts.push(format!("min: {:?}", min_utf8)));
                    s.max_opt()
                        .and_then(|max| max.as_utf8().ok())
                        .map(|max_utf8| parts.push(format!("max: {:?}", max_utf8)));
                }
                Statistics::FixedLenByteArray(s) => {
                    s.min_opt()
                        .and_then(|min| min.as_utf8().ok())
                        .map(|min_utf8| parts.push(format!("min: {:?}", min_utf8)));
                    s.max_opt()
                        .and_then(|max| max.as_utf8().ok())
                        .map(|max_utf8| parts.push(format!("max: {:?}", max_utf8)));
                }
            }

            if let Some(null_count) = stats.null_count_opt() {
                parts.push(format!("nulls: {}", format_rows(null_count as u64)));
            }

            if let Some(distinct_count) = stats.distinct_count_opt() {
                parts.push(format!("distinct: {}", format_rows(distinct_count as u64)));
            }

            if parts.is_empty() {
                "✗".to_string()
            } else {
                parts.join(" / ")
            }
        }
        None => "✗".to_string(),
    }
}

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

#[component]
fn App() -> impl IntoView {
    let default_url = "https://raw.githubusercontent.com/RobinL/iris_parquet/main/gridwatch/gridwatch_2023-01-08.parquet";
    let (url, set_url) = create_signal(default_url.to_string());
    let (file_content, set_file_content) = create_signal(None::<ParquetInfo>);
    let (error_message, set_error_message) = create_signal(Option::<String>::None);
    let (file_bytes, set_file_bytes) = create_signal(None::<Bytes>);
    let (sql_query, set_sql_query) = create_signal(String::new());
    let (query_result, set_query_result) = create_signal(Vec::<arrow::array::RecordBatch>::new());
    let (file_name, set_file_name) = create_signal(String::from("uploaded"));
    let (physical_plan, set_physical_plan) = create_signal(None::<Arc<dyn ExecutionPlan>>);

    let execute_query = move || {
        let query = sql_query.get();
        let bytes_opt = file_bytes.get();
        let table_name = file_name.get();
        set_error_message.set(None); // Clear any previous error messages

        if query.trim().is_empty() {
            set_error_message.set(Some("Please enter a SQL query.".into()));
            return;
        }
        if let Some(bytes) = bytes_opt {
            wasm_bindgen_futures::spawn_local(async move {
                web_sys::console::log_1(&table_name.clone().into());
                let ctx = datafusion::prelude::SessionContext::new();

                let schema = match file_content.get_untracked() {
                    Some(content) => content.schema.clone(),
                    None => {
                        set_error_message.set(Some("Failed to get file schema".into()));
                        return;
                    }
                };

                let streaming_table =
                    match datafusion::datasource::streaming::StreamingTable::try_new(
                        schema.clone(),
                        vec![Arc::new(DummyStreamPartition {
                            schema: schema.clone(),
                            bytes: bytes.clone(),
                        })],
                    ) {
                        Ok(table) => table,
                        Err(e) => {
                            set_error_message
                                .set(Some(format!("Failed to create streaming table: {}", e)));
                            return;
                        }
                    };

                if let Err(e) = ctx.register_table(table_name.clone(), Arc::new(streaming_table)) {
                    set_error_message.set(Some(format!(
                        "Failed to register table '{}': {}",
                        table_name, e
                    )));
                    return;
                }

                let plan = match ctx.sql(&query).await {
                    Ok(plan) => plan,
                    Err(e) => {
                        set_error_message.set(Some(format!("SQL error: {}", e)));
                        return;
                    }
                };

                let (state, plan) = plan.into_parts();
                let plan = match state.optimize(&plan) {
                    Ok(plan) => plan,
                    Err(e) => {
                        set_error_message.set(Some(format!("Failed to optimize query: {}", e)));
                        return;
                    }
                };

                web_sys::console::log_1(&plan.display_indent().to_string().into());

                let physical_plan = match state.create_physical_plan(&plan).await {
                    Ok(plan) => plan,
                    Err(e) => {
                        set_error_message
                            .set(Some(format!("Failed to create execution plan: {}", e)));
                        return;
                    }
                };

                set_physical_plan.set(Some(physical_plan.clone()));

                match collect(physical_plan, ctx.task_ctx().clone()).await {
                    Ok(results) => {
                        set_error_message.set(None); // Clear error message on success
                        set_query_result.set(results);
                    }
                    Err(e) => {
                        set_error_message.set(Some(format!("Failed to execute query: {}", e)));
                    }
                };
            });
        } else {
            set_error_message.set(Some("No Parquet file loaded.".into()));
        };
    };

    let on_file_select = move |ev: web_sys::Event| {
        let input: web_sys::HtmlInputElement = event_target(&ev);
        let files = input.files().unwrap();
        let file = files.get(0).unwrap();

        let file_reader = web_sys::FileReader::new().unwrap();
        let file_content_setter = set_file_content.clone();
        let file_reader_clone = file_reader.clone();

        let onload = Closure::wrap(Box::new(move |_: web_sys::Event| {
            let result = file_reader_clone.result().unwrap();
            let array_buffer = result.dyn_into::<js_sys::ArrayBuffer>().unwrap();
            let uint8_array = js_sys::Uint8Array::new(&array_buffer);
            let bytes = bytes::Bytes::from(uint8_array.to_vec());
            let parquet_info = get_parquet_info(bytes.clone());

            match parquet_info {
                Ok(info) => {
                    web_sys::console::log_1(&info.to_string().into());
                    file_content_setter.set(Some(info));
                    set_file_bytes.set(Some(bytes.clone()));
                    let default_query =
                        format!("select * from \"{}\" limit 10", file_name.get_untracked());
                    set_sql_query.set(default_query);
                    execute_query();
                }
                Err(_e) => {
                    file_content_setter.set(None);
                }
            }
        }) as Box<dyn FnMut(_)>);

        file_reader.set_onload(Some(onload.as_ref().unchecked_ref()));
        file_reader.read_as_array_buffer(&file).unwrap();
        onload.forget();
        let table_name = file
            .name()
            .strip_suffix(".parquet")
            .unwrap_or(&file.name())
            .to_string();
        set_file_name.set(table_name);
    };

    let on_url_submit = move |ev: web_sys::SubmitEvent| {
        ev.prevent_default();
        let url_str = url.get();
        set_error_message.set(None); // Clear previous errors

        let table_name = url_str
            .split('/')
            .last()
            .unwrap_or("uploaded.parquet")
            .strip_suffix(".parquet")
            .unwrap_or("uploaded")
            .to_string();
        set_file_name.set(table_name.clone());

        wasm_bindgen_futures::spawn_local(async move {
            let opts = web_sys::RequestInit::new();
            opts.set_method("GET");

            let headers = web_sys::Headers::new().unwrap();
            headers.append("Accept", "*/*").unwrap();
            opts.set_headers(&headers);

            let request = web_sys::Request::new_with_str_and_init(&url_str, &opts).unwrap();

            let window = web_sys::window().unwrap();
            let resp = match JsFuture::from(window.fetch_with_request(&request)).await {
                Ok(resp) => resp,
                Err(_) => {
                    set_error_message.set(Some("Failed to fetch the file. This might be due to CORS restrictions. Try using a direct link from S3 or a server that allows CORS.".into()));
                    return;
                }
            };

            let resp: web_sys::Response = resp.dyn_into().unwrap();
            if !resp.ok() {
                let status = resp.status();
                let error_msg = match status {
                    0 => "Network error: The server might be blocking CORS requests.".to_string(),
                    403 => "Access denied: The file is not publicly accessible.".to_string(),
                    404 => "File not found: Please check if the URL is correct.".to_string(),
                    _ => format!(
                        "Server error (status {}): Please try a different source.",
                        status
                    ),
                };
                set_error_message.set(Some(error_msg));
                return;
            }

            let array_buffer = JsFuture::from(resp.array_buffer().unwrap()).await.unwrap();
            let uint8_array = js_sys::Uint8Array::new(&array_buffer);
            let bytes = bytes::Bytes::from(uint8_array.to_vec());

            let parquet_info = get_parquet_info(bytes.clone());
            match parquet_info {
                Ok(info) => {
                    set_file_content.set(Some(info.clone()));
                    set_file_bytes.set(Some(bytes.clone()));
                    let default_query = format!("select * from \"{}\" limit 10", table_name);
                    set_sql_query.set(default_query);
                    execute_query();
                }
                Err(_) => {
                    set_file_content.set(None);
                }
            }
        });
    };

    let run_query = move |_ev: web_sys::MouseEvent| {
        execute_query();
    };

    view! {
        <div class="container mx-auto px-4 py-8 max-w-6xl">
            <h1 class="text-3xl font-bold mb-8 flex items-center justify-between">
                <span>"Parquet Viewer"</span>
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
            </h1>
            <div class="space-y-6">
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4 items-center">
                    <div class="border-2 border-dashed border-gray-300 rounded-lg p-6 text-center space-y-4">
                        <div>
                            <input
                                type="file"
                                accept=".parquet"
                                on:change=on_file_select
                                id="file-input"
                            />
                        </div>
                        <div>
                            <label for="file-input" class="cursor-pointer text-gray-600">
                                "Drop Parquet file or click to browse"
                            </label>
                        </div>
                    </div>
                    <div class="flex items-center space-x-4">
                        <div class="text-gray-500">"OR"</div>
                        <form on:submit=on_url_submit class="flex-1">
                            <div class="flex space-x-2">
                                <input
                                    type="url"
                                    placeholder="Enter Parquet file URL"
                                    on:focus=move |ev| {
                                        let input: web_sys::HtmlInputElement = event_target(&ev);
                                        input.select();
                                    }
                                    on:input=move |ev| {
                                        set_url.set(event_target_value(&ev));
                                    }
                                    prop:value=url
                                    class="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                />
                                <button
                                    type="submit"
                                    class="px-4 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600"
                                >
                                    "Load from URL"
                                </button>
                            </div>
                        </form>
                    </div>
                </div>
                <div class="border-t border-gray-300 my-4"></div>

                {move || {
                    error_message
                        .get()
                        .map(|msg| {
                            view! {
                                <div class="bg-red-50 border-l-4 border-red-500 p-4 my-4">
                                    <div class="text-red-700">{msg}</div>
                                    <div class="mt-2 text-sm text-gray-600">
                                        "Tip: Try these sources:"
                                        <ul class="list-disc ml-6 mt-2 space-y-1">
                                            <li>
                                                "Grid watch data: "
                                                <code class="bg-gray-100 px-1 rounded">
                                                    "https://raw.githubusercontent.com/RobinL/iris_parquet/main/gridwatch/gridwatch_2023-01-08.parquet"
                                                </code>
                                            </li>
                                            <li>"Your own S3 bucket with CORS enabled"</li>
                                            <li>
                                                "Or download the file and use the file picker above"
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
                                view! {
                                    <div class="flex gap-2 items-center">
                                        <input
                                            type="text"
                                            placeholder=move || {
                                                format!("select * from \"{}\" limit 10", file_name.get())
                                            }
                                            prop:value=sql_query
                                            on:input=move |ev| set_sql_query(event_target_value(&ev))
                                            on:dblclick=move |_| {
                                                if sql_query.get().trim().is_empty() {
                                                    set_sql_query(format!("select * from \"{}\" limit 10", file_name.get()))
                                                }
                                            }
                                            on:keydown=move |ev| {
                                                if ev.key() == "Enter" {
                                                    execute_query();
                                                }
                                            }
                                            class="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                        />
                                        <button
                                            on:click=run_query
                                            class="px-4 py-2 bg-green-500 text-white rounded-md hover:bg-green-600 whitespace-nowrap"
                                        >
                                            "Run Query"
                                        </button>
                                    </div>
                                }
                            })
                    }}
                </div>

                {move || {
                    let result = query_result.get();
                    if result.is_empty() {
                        return view! {
                        }.into_view();
                    } else {
                        let physical_plan = physical_plan.get().unwrap();
                        view! {
                            <QueryResults query_result=result physical_plan=physical_plan />
                        }
                        .into_view()
                    }
                }}

                <div class="mt-8">
                    {move || {
                        let info = file_content.get();
                        match info {
                            Some(info) => {
                                view! {
                                    <div class="flex gap-6">
                                        <div class="w-96 flex-none">
                                            <InfoSection parquet_info=info.clone() />
                                        </div>
                                        <div class="w-96 flex-1">
                                            <SchemaSection parquet_info=info.clone() />
                                        </div>
                                    </div>
                                }
                                    .into_view()
                            }
                            None => {
                                view! {
                                    <div class="text-center text-gray-500 py-8">
                                        "No file selected"
                                    </div>
                                }
                                    .into_view()
                            }
                        }
                    }}
                </div>

            </div>
        </div>
    }
}

fn main() {
    console_error_panic_hook::set_once();
    mount_to_body(|| view! { <App /> })
}
