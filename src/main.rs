mod schema;
use datafusion::{
    datasource::MemTable,
    execution::object_store::ObjectStoreUrl,
    physical_plan::ExecutionPlan,
    prelude::{SessionConfig, SessionContext},
};
use leptos_router::{
    components::Router,
    hooks::{query_signal, use_query_map},
};
use object_store::path::Path;
use parquet_reader::{ParquetInfo, ParquetReader, INMEMORY_STORE};

use query_results::{export_to_csv_inner, export_to_parquet_inner, QueryResult, QueryResultView};
use schema::SchemaSection;

mod parquet_reader;
mod query_results;
mod row_group_column;

mod metadata;
mod object_store_cache;
use metadata::MetadataSection;

use std::{sync::Arc, sync::LazyLock};

use arrow::datatypes::SchemaRef;
use leptos::{logging, prelude::*};
use parquet::{
    arrow::{
        async_reader::{AsyncFileReader, ParquetObjectReader},
        parquet_to_arrow_schema,
    },
    errors::ParquetError,
    file::metadata::ParquetMetaData,
};

mod query_input;
use query_input::{execute_query_inner, QueryInput};

mod settings;
use settings::Settings;

pub(crate) static SESSION_CTX: LazyLock<Arc<SessionContext>> = LazyLock::new(|| {
    let mut config = SessionConfig::new();
    config.options_mut().sql_parser.dialect = "PostgreSQL".to_string();
    config.options_mut().execution.parquet.pushdown_filters = true;
    let ctx = Arc::new(SessionContext::new_with_config(config));
    let object_store_url = ObjectStoreUrl::parse("mem://").unwrap();
    let object_store = INMEMORY_STORE.clone();
    ctx.register_object_store(object_store_url.as_ref(), object_store);
    ctx
});

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ParquetFileReader {
    parquet_table: ParquetTable,
    display_info: DisplayInfo,
}

impl ParquetFileReader {
    pub fn new(table: ParquetTable) -> Result<Self> {
        let metadata = table.metadata.clone();
        let size = metadata.memory_size();

        let parquet_info = DisplayInfo::from_metadata(metadata, size as u64)?;

        Ok(Self {
            parquet_table: table,
            display_info: parquet_info,
        })
    }

    fn info(&self) -> &DisplayInfo {
        &self.display_info
    }

    fn table_name(&self) -> &str {
        &self.parquet_table.table_name
    }
}

#[derive(Debug, Clone, PartialEq)]
struct DisplayInfo {
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

impl DisplayInfo {
    fn from_metadata(
        metadata: Arc<ParquetMetaData>,
        metadata_len: u64,
    ) -> Result<Self, ParquetError> {
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

        let has_column_index = metadata
            .column_index()
            .and_then(|ci| ci.first().map(|c| !c.is_empty()))
            .unwrap_or(false);
        let has_page_index = metadata
            .offset_index()
            .and_then(|ci| ci.first().map(|c| !c.is_empty()))
            .unwrap_or(false);

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
            has_column_index,
            has_page_index,
            has_bloom_filter: first_column
                .map(|c| c.bloom_filter_offset().is_some())
                .unwrap_or(false),
            schema: Arc::new(schema),
            metadata,
            metadata_len,
        })
    }
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

impl std::fmt::Display for DisplayInfo {
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
    query: &str,
) -> Result<(Vec<arrow::array::RecordBatch>, Arc<dyn ExecutionPlan>), String> {
    let (results, physical_plan) = execute_query_inner(query)
        .await
        .map_err(|e| format!("Failed to execute query: {}", e))?;

    Ok((results, physical_plan))
}

#[derive(Debug, Clone)]
struct ParquetTable {
    reader: ParquetObjectReader,
    metadata: Arc<ParquetMetaData>,
    table_name: String,
}

impl PartialEq for ParquetTable {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
    }
}

#[component]
fn App() -> impl IntoView {
    let (error_message, set_error_message) = signal(Option::<String>::None);
    let (parquet_table, set_parquet_table) = signal(None::<ParquetTable>);
    let (user_input, set_user_input) = query_signal::<String>("query");

    let export_to = use_query_map().with(|map| map.get("export").map(|v| v.to_string()));

    let (sql_query, set_sql_query) = signal(String::new());
    let (query_results, set_query_results) = signal(Vec::<QueryResult>::new());

    let (show_settings, set_show_settings) = signal(false);

    let parquet_file_reader = Memo::new(move |_| {
        parquet_table
            .get()
            .and_then(|table| ParquetFileReader::new(table).ok())
    });

    let (force_update_user_input, set_force_update_user_input) = signal(false);

    let toggle_display = move |id: usize| {
        set_query_results.update(|r| {
            r.iter_mut()
                .find(|r| r.id() == id)
                .unwrap()
                .toggle_display();
        });
    };

    Effect::watch(
        parquet_file_reader,
        move |reader, old_reader, _| {
            let Some(reader) = reader else { return };

            match old_reader.flatten() {
                Some(old_reader) => {
                    if old_reader.table_name() != reader.table_name() {
                        let default_query =
                            format!("select * from \"{}\" limit 10", reader.table_name());
                        set_user_input.set(Some(default_query));
                    }
                }
                None => match user_input.get() {
                    Some(user_input) => {
                        set_user_input.set(Some(user_input));
                        set_force_update_user_input.set(true);
                    }
                    None => {
                        logging::log!("{}", reader.info().to_string());
                        let default_query =
                            format!("select * from \"{}\" limit 10", reader.table_name());
                        set_user_input.set(Some(default_query));
                    }
                },
            }
        },
        true,
    );

    Effect::watch(
        move || (force_update_user_input.get(), user_input.get()),
        move |(_, user_input_str), _, _| {
            let Some(user_input_str) = user_input_str else {
                return;
            };

            let user_input = user_input_str.clone();
            leptos::task::spawn_local(async move {
                let Some(parquet_reader) = parquet_file_reader.get() else {
                    return;
                };
                let sql = match query_input::user_input_to_sql(&user_input, &parquet_reader).await {
                    Ok(response) => response,
                    Err(e) => {
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
            let bytes_opt = parquet_table.get();
            set_error_message.set(None);

            if query.trim().is_empty() {
                return;
            }

            if let Some(_parquet_table) = bytes_opt {
                let query = query.clone();
                let export_to = export_to.clone();

                leptos::task::spawn_local(async move {
                    match execute_query_async(&query).await {
                        Ok((results, physical_plan)) => {
                            if let Some(export_to) = export_to {
                                if export_to == "csv" {
                                    export_to_csv_inner(&results);
                                } else if export_to == "parquet" {
                                    export_to_parquet_inner(&results);
                                }
                            }

                            set_query_results.update(|r| {
                                let id = r.len();
                                if let Some(first_batch) = results.first() {
                                    let schema = first_batch.schema();
                                    let mem_table =
                                        MemTable::try_new(schema, vec![results.clone()]).unwrap();
                                    SESSION_CTX
                                        .as_ref()
                                        .register_table(format!("view_{}", id), Arc::new(mem_table))
                                        .unwrap();
                                }
                                r.push(QueryResult::new(
                                    id,
                                    query,
                                    Arc::new(results),
                                    physical_plan,
                                ));
                            });
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

    let on_parquet_read = move |parquet_info: ParquetInfo| {
        leptos::task::spawn_local(async move {
            let meta = parquet_info
                .object_store
                .head(&Path::parse(&parquet_info.path).unwrap())
                .await
                .unwrap();
            let mut reader = ParquetObjectReader::new(parquet_info.object_store.clone(), meta)
                .with_preload_column_index(true)
                .with_preload_offset_index(true);
            let metadata = reader.get_metadata().await.unwrap();

            let table_path = parquet_info.table_path();

            let ctx = SESSION_CTX.as_ref();
            if ctx
                .runtime_env()
                .object_store(&parquet_info.object_store_url)
                .is_err()
            {
                logging::log!(
                    "Object store {} not found, registering",
                    parquet_info.object_store_url
                );
                ctx.register_object_store(
                    parquet_info.object_store_url.as_ref(),
                    parquet_info.object_store,
                );
            } else {
                logging::log!(
                    "Object store {} found, using existing store",
                    parquet_info.object_store_url
                );
            }
            ctx.register_parquet(&parquet_info.table_name, &table_path, Default::default())
                .await
                .unwrap();
            set_parquet_table.set(Some(ParquetTable {
                reader,
                table_name: parquet_info.table_name,
                metadata,
            }));
        });
    };

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
                <ParquetReader
                    set_error_message=set_error_message
                    read_call_back=on_parquet_read
                />

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
                                                "If query with natural language, make sure to set the Claude API key."
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

                <div class="border-t border-gray-300 my-4"></div>

                <div class="mt-4">
                    {move || {
                        parquet_table
                            .get()
                            .map(|_| {
                                match parquet_file_reader() {
                                    Some(info) => {
                                        if info.info().row_group_count > 0 {
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

                <div class="space-y-4">
                    <For
                        each=move || query_results.get().into_iter().filter(|r| r.display()).rev()
                        key=|result| result.id()
                        children=move |result| {
                            view! {
                                <div class="transform transition-all duration-300 ease-out animate-slide-in">
                                    <QueryResultView result=result toggle_display=toggle_display />
                                </div>
                            }
                        }
                    />
                </div>

                <div class="border-t border-gray-300 my-4"></div>

                <div class="mt-8">
                    {move || {
                        let info = parquet_file_reader();
                        match info {
                            Some(info) => {
                                view! {
                                    <div class="space-y-6">
                                        <div class="w-full">
                                            <MetadataSection parquet_reader=info.clone() />
                                        </div>
                                        <div class="w-full">
                                            <SchemaSection parquet_info=info.info().clone() />
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
            <Settings show=show_settings set_show=set_show_settings />
        </div>
    }
}

fn main() {
    console_error_panic_hook::set_once();
    mount_to_body(|| {
        view! {
            <Router>
                <App />
            </Router>
        }
    })
}
