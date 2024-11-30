mod schema;
use datafusion::physical_plan::ExecutionPlan;
use opendal::{services::S3, Operator};
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
    arrow::parquet_to_arrow_schema,
    errors::ParquetError,
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};
use wasm_bindgen::{prelude::Closure, JsCast};
use wasm_bindgen_futures::JsFuture;
use web_sys::js_sys;

mod query_input;
use query_input::{execute_query_inner, QueryInput};

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

async fn fetch_parquet_from_url(url_str: String) -> Result<Bytes, String> {
    let opts = web_sys::RequestInit::new();
    opts.set_method("GET");

    let headers = web_sys::Headers::new().map_err(|_| "Failed to create headers")?;
    headers
        .append("Accept", "*/*")
        .map_err(|_| "Failed to set headers")?;
    opts.set_headers(&headers);

    let request = web_sys::Request::new_with_str_and_init(&url_str, &opts)
        .map_err(|_| "Failed to create request")?;

    let window = web_sys::window().ok_or("Failed to get window")?;
    let resp = JsFuture::from(window.fetch_with_request(&request))
        .await
        .map_err(|_| "Failed to fetch the file. This might be due to CORS restrictions. Try using a direct link from S3 or a server that allows CORS.")?;

    let resp: web_sys::Response = resp.dyn_into().map_err(|_| "Failed to convert response")?;

    if !resp.ok() {
        let status = resp.status();
        let error_msg = match status {
            0 => "Network error: The server might be blocking CORS requests.",
            403 => "Access denied: The file is not publicly accessible.",
            404 => "File not found: Please check if the URL is correct.",
            _ => {
                return Err(format!(
                    "Server error (status {}): Please try a different source.",
                    status
                ))
            }
        };
        return Err(error_msg.to_string());
    }

    let array_buffer = JsFuture::from(
        resp.array_buffer()
            .map_err(|_| "Failed to get array buffer")?,
    )
    .await
    .map_err(|_| "Failed to convert array buffer")?;

    let uint8_array = js_sys::Uint8Array::new(&array_buffer);
    Ok(Bytes::from(uint8_array.to_vec()))
}

async fn execute_query_async(
    query: String,
    bytes: Bytes,
    table_name: String,
    parquet_info: ParquetInfo,
) -> Result<(Vec<arrow::array::RecordBatch>, Arc<dyn ExecutionPlan>), String> {
    web_sys::console::log_1(&table_name.clone().into());

    let (results, physical_plan) = execute_query_inner(&table_name, parquet_info, bytes, &query)
        .await
        .map_err(|e| format!("Failed to execute query: {}", e))?;

    Ok((results, physical_plan))
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
    let (active_tab, set_active_tab) = create_signal("file");
    let (s3_endpoint, set_s3_endpoint) = create_signal(String::from("https://s3.amazonaws.com"));
    let (s3_access_key_id, set_s3_access_key_id) = create_signal(String::new());
    let (s3_secret_key, set_s3_secret_key) = create_signal(String::new());
    let (s3_bucket, set_s3_bucket) = create_signal(String::new());
    let (s3_region, set_s3_region) = create_signal(String::from("us-east-1"));
    let (s3_file_path, set_s3_file_path) = create_signal(String::new());

    let execute_query = move |query: String| {
        let bytes_opt = file_bytes.get();
        let table_name = file_name.get();
        set_error_message.set(None);

        if query.trim().is_empty() {
            set_error_message.set(Some("Please enter a SQL query.".into()));
            return;
        }

        if let Some(bytes) = bytes_opt {
            let parquet_info = match file_content.get_untracked() {
                Some(content) => content,
                None => {
                    set_error_message.set(Some("Failed to get file schema".into()));
                    return;
                }
            };

            wasm_bindgen_futures::spawn_local(async move {
                match execute_query_async(query, bytes, table_name, parquet_info).await {
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
    };

    let on_bytes_load =
        move |bytes: Bytes, file_content_setter: WriteSignal<Option<ParquetInfo>>| {
            let parquet_info = get_parquet_info(bytes.clone());

            match parquet_info {
                Ok(info) => {
                    web_sys::console::log_1(&info.to_string().into());
                    file_content_setter.set(Some(info));
                    set_file_bytes.set(Some(bytes.clone()));
                    let default_query =
                        format!("select * from \"{}\" limit 10", file_name.get_untracked());
                    set_sql_query.set(default_query.clone());
                    execute_query(default_query);
                }
                Err(_e) => {
                    file_content_setter.set(None);
                }
            }
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
            on_bytes_load(bytes, file_content_setter);
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
        set_error_message.set(None);

        let table_name = url_str
            .split('/')
            .last()
            .unwrap_or("uploaded.parquet")
            .strip_suffix(".parquet")
            .unwrap_or("uploaded")
            .to_string();
        set_file_name.set(table_name);

        wasm_bindgen_futures::spawn_local(async move {
            match fetch_parquet_from_url(url_str).await {
                Ok(bytes) => on_bytes_load(bytes, set_file_content),
                Err(error) => set_error_message.set(Some(error)),
            }
        });
    };

    let on_s3_submit = move |ev: web_sys::SubmitEvent| {
        ev.prevent_default();
        set_error_message.set(None);

        let endpoint = s3_endpoint.get();
        let access_key_id = s3_access_key_id.get();
        let secret_key = s3_secret_key.get();
        let bucket = s3_bucket.get();
        let region = s3_region.get();

        // Validate inputs
        if endpoint.is_empty() || bucket.is_empty() || s3_file_path.get().is_empty() {
            set_error_message.set(Some("All fields except region are required".into()));
            return;
        }

        wasm_bindgen_futures::spawn_local(async move {
            let cfg = S3::default()
                .endpoint(&endpoint)
                .access_key_id(&access_key_id)
                .secret_access_key(&secret_key)
                .bucket(&bucket)
                .region(&region);

            match Operator::new(cfg) {
                Ok(op) => {
                    let operator = op.finish();
                    match operator.read(&s3_file_path.get()).await {
                        Ok(bs) => {
                            let bytes = Bytes::from(bs.to_vec());
                            on_bytes_load(bytes, set_file_content);
                        }
                        Err(e) => {
                            set_error_message.set(Some(format!("Failed to read from S3: {}", e)));
                        }
                    }
                }
                Err(e) => {
                    set_error_message.set(Some(format!("Failed to create S3 operator: {}", e)));
                }
            }
        });
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
                <div class="border-b border-gray-200">
                    <nav class="-mb-px flex space-x-8">
                        <button
                            class=move || {
                                let base = "py-2 px-1 border-b-2 font-medium text-sm";
                                if active_tab.get() == "file" {
                                    format!("{} border-blue-500 text-blue-600", base)
                                } else {
                                    format!("{} border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300", base)
                                }
                            }
                            on:click=move |_| set_active_tab.set("file")
                        >
                            "Local file"
                        </button>
                        <button
                            class=move || {
                                let base = "py-2 px-1 border-b-2 font-medium text-sm";
                                if active_tab.get() == "url" {
                                    format!("{} border-blue-500 text-blue-600", base)
                                } else {
                                    format!("{} border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300", base)
                                }
                            }
                            on:click=move |_| set_active_tab.set("url")
                        >
                            "Load from URL"
                        </button>
                        <button
                            class=move || {
                                let base = "py-2 px-1 border-b-2 font-medium text-sm";
                                if active_tab.get() == "s3" {
                                    format!("{} border-blue-500 text-blue-600", base)
                                } else {
                                    format!("{} border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300", base)
                                }
                            }
                            on:click=move |_| set_active_tab.set("s3")
                        >
                            "From S3"
                        </button>
                    </nav>
                </div>

                {move || match active_tab.get() {
                    "file" => view! {
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
                    }.into_view(),
                    "url" => view! {
                        <form on:submit=on_url_submit class="w-full">
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
                    }.into_view(),
                    "s3" => view! {
                        <form on:submit=on_s3_submit class="space-y-4 w-full">
                            <div class="flex flex-wrap gap-4">
                                <div class="flex-1 min-w-[250px]">
                                    <label class="block text-sm font-medium text-gray-700 mb-1">
                                        "S3 Endpoint"
                                    </label>
                                    <input
                                        type="text"
                                        placeholder="https://s3.amazonaws.com"
                                        on:input=move |ev| set_s3_endpoint.set(event_target_value(&ev))
                                        prop:value=s3_endpoint
                                        class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                    />
                                </div>
                                <div class="flex-1 min-w-[250px]">
                                    <label class="block text-sm font-medium text-gray-700 mb-1">
                                        "Access Key ID"
                                    </label>
                                    <input
                                        type="text"
                                        on:input=move |ev| set_s3_access_key_id.set(event_target_value(&ev))
                                        prop:value=s3_access_key_id
                                        class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                    />
                                </div>
                                <div class="flex-1 min-w-[250px]">
                                    <label class="block text-sm font-medium text-gray-700 mb-1">
                                        "Secret Access Key"
                                    </label>
                                    <input
                                        type="password"
                                        on:input=move |ev| set_s3_secret_key.set(event_target_value(&ev))
                                        prop:value=s3_secret_key
                                        class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                    />
                                </div>
                                <div class="flex-1 min-w-[250px]">
                                    <label class="block text-sm font-medium text-gray-700 mb-1">
                                        "Bucket"
                                    </label>
                                    <input
                                        type="text"
                                        on:input=move |ev| set_s3_bucket.set(event_target_value(&ev))
                                        prop:value=s3_bucket
                                        class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                    />
                                </div>
                                <div class="flex-1 min-w-[250px]">
                                    <label class="block text-sm font-medium text-gray-700 mb-1">
                                        "Region"
                                    </label>
                                    <input
                                        type="text"
                                        placeholder="us-east-1"
                                        on:input=move |ev| set_s3_region.set(event_target_value(&ev))
                                        prop:value=s3_region
                                        class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                    />
                                </div>
                                <div class="flex-1 min-w-[250px]">
                                    <label class="block text-sm font-medium text-gray-700 mb-1">
                                        "File Path"
                                    </label>
                                    <input
                                        type="text"
                                        placeholder="path/to/file.parquet"
                                        on:input=move |ev| set_s3_file_path.set(event_target_value(&ev))
                                        prop:value=s3_file_path
                                        class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                    />
                                </div>
                            </div>
                            <div>
                                <button
                                    type="submit"
                                    class="w-full px-4 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600"
                                >
                                    "Read from S3"
                                </button>
                            </div>
                        </form>
                    }.into_view(),
                    _ => view! {}.into_view()
                }}

                <div class="border-t border-gray-300 my-4"></div>

                {move || {
                    error_message
                        .get()
                        .map(|msg| {
                            view! {
                                <div class="bg-red-50 border-l-4 border-red-500 p-4 my-4">
                                    <div class="text-red-700">{msg}</div>
                                    <div class="mt-2 text-sm text-gray-600">
                                        "Tips:"
                                        <ul class="list-disc ml-6 mt-2 space-y-1">
                                            <li>"Make sure the URL has CORS enabled."</li>
                                            <li>"If query with natural language, make sure to set the Gemini API key (free tier is enough)."</li>
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
                                match file_content.get_untracked() {
                                    Some(info) => {
                                        view! {
                                            <QueryInput
                                                sql_query=sql_query
                                                set_sql_query=set_sql_query
                                                file_name=file_name
                                                execute_query=Arc::new(execute_query)
                                                schema=info.schema
                                                error_message=set_error_message
                                            />
                                        }
                                    },
                                    None => view! {}.into_view(),
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
                            <QueryResults sql_query=sql_query.get_untracked() query_result=result physical_plan=physical_plan />
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
