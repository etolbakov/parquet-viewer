mod schema;
use datafusion::physical_plan::ExecutionPlan;
use file_reader::FileReader;
use query_results::QueryResults;
use schema::SchemaSection;

mod file_reader;
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
        let first_row_group = metadata.row_groups().first();
        let first_column = first_row_group.map(|rg| rg.columns().first()).flatten();

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
    web_sys::console::log_1(&table_name.clone().into());

    let (results, physical_plan) = execute_query_inner(&table_name, parquet_info, bytes, &query)
        .await
        .map_err(|e| format!("Failed to execute query: {}", e))?;

    Ok((results, physical_plan))
}

#[component]
fn App() -> impl IntoView {
    let (file_content, set_file_content) = create_signal(None::<ParquetInfo>);
    let (error_message, set_error_message) = create_signal(Option::<String>::None);
    let (file_bytes, set_file_bytes) = create_signal(None::<Bytes>);
    let (user_query, set_user_query) = create_signal(String::new());
    let (sql_query, set_sql_query) = create_signal(String::new());
    let (query_result, set_query_result) = create_signal(Vec::<arrow::array::RecordBatch>::new());
    let (file_name, set_file_name) = create_signal(String::from("uploaded"));
    let (physical_plan, set_physical_plan) = create_signal(None::<Arc<dyn ExecutionPlan>>);

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
                match execute_query_async(query.clone(), bytes, table_name, parquet_info).await {
                    Ok((results, physical_plan)) => {
                        set_physical_plan.set(Some(physical_plan));
                        set_query_result.set(results);
                        set_sql_query.set(query);
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
                    set_user_query.set(default_query.clone());
                    set_sql_query.set(default_query.clone());
                    execute_query(default_query);
                }
                Err(_e) => {
                    file_content_setter.set(None);
                }
            }
        };

    create_effect(move |_| {
        if let Some(bytes) = file_bytes.get() {
            on_bytes_load(bytes, set_file_content);
        }
    });

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
                                match file_content.get_untracked() {
                                    Some(info) => {
                                        if info.row_group_count > 0 {
                                            view! {
                                                <QueryInput
                                                user_query=user_query
                                                set_user_query=set_user_query
                                                file_name=file_name
                                                execute_query=Arc::new(execute_query)
                                                    schema=info.schema
                                                    error_message=set_error_message
                                                />
                                            }
                                        } else {
                                            view! {}.into_view()
                                        }
                                    }
                                    None => view! {}.into_view(),
                                }
                            })
                    }}
                </div>

                {move || {
                    let result = query_result.get();
                    if result.is_empty() {
                        return view! {}.into_view();
                    } else {
                        let physical_plan = physical_plan.get().unwrap();
                        view! {
                            <QueryResults
                                sql_query=sql_query.get()
                                set_user_query=set_user_query
                                query_result=result
                                physical_plan=physical_plan
                            />
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
