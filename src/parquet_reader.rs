use std::sync::{Arc, LazyLock};

use datafusion::execution::object_store::ObjectStoreUrl;
use leptos::logging::log;
use leptos::prelude::*;
use leptos::wasm_bindgen::{prelude::Closure, JsCast};
use leptos_router::hooks::{query_signal, use_query_map};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use object_store_opendal::OpendalStore;
use opendal::{services::Http, services::S3, Operator};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use url::Url;
use web_sys::js_sys;

use crate::object_store_cache::ObjectStoreCache;
use crate::{ParquetTable, SESSION_CTX};

pub(crate) static INMEMORY_STORE: LazyLock<Arc<InMemory>> =
    LazyLock::new(|| Arc::new(InMemory::new()));

const S3_ENDPOINT_KEY: &str = "s3_endpoint";
const S3_ACCESS_KEY_ID_KEY: &str = "s3_access_key_id";
const S3_SECRET_KEY_KEY: &str = "s3_secret_key";
const S3_BUCKET_KEY: &str = "s3_bucket";
const S3_REGION_KEY: &str = "s3_region";
const S3_FILE_PATH_KEY: &str = "s3_file_path";

pub(crate) fn get_stored_value(key: &str, default: &str) -> String {
    let window = web_sys::window().unwrap();
    let storage = window.local_storage().unwrap().unwrap();
    storage
        .get_item(key)
        .unwrap()
        .unwrap_or_else(|| default.to_string())
}

fn save_to_storage(key: &str, value: &str) {
    if let Some(window) = web_sys::window() {
        if let Ok(Some(storage)) = window.local_storage() {
            let _ = storage.set_item(key, value);
        }
    }
}

const DEFAULT_URL: &str = "https://raw.githubusercontent.com/RobinL/iris_parquet/main/gridwatch/gridwatch_2023-01-08.parquet";

#[component]
pub fn ParquetReader(
    set_error_message: WriteSignal<Option<String>>,
    set_parquet_table: WriteSignal<Option<ParquetTable>>,
) -> impl IntoView {
    let default_tab = {
        let query = use_query_map();
        let url = query.get().get("url");
        if url.is_some() {
            "url"
        } else {
            "file"
        }
    };
    let (active_tab, set_active_tab) = signal(default_tab.to_string());

    let set_active_tab_fn = move |tab: &str| {
        if active_tab.get() != tab {
            set_active_tab.set(tab.to_string());
        }
    };

    view! {
        <div class="bg-white rounded-lg border border-gray-300 p-3">
            <div class="border-b border-gray-200 mb-4">
                <nav class="-mb-px flex space-x-8">
                    <button
                        class=move || {
                            let base = "py-2 px-1 border-b-2 font-medium text-sm";
                            if active_tab.get() == "file" {
                                return format!("{} border-green-500 text-green-600", base);
                            }
                            format!(
                                "{} border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300",
                                base,
                            )
                        }
                        on:click=move |_| set_active_tab_fn("file")
                    >
                        "From file"
                    </button>
                    <button
                        class=move || {
                            let base = "py-2 px-1 border-b-2 font-medium text-sm";
                            if active_tab.get() == "url" {
                                return format!("{} border-green-500 text-green-600", base);
                            }
                            format!(
                                "{} border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300",
                                base,
                            )
                        }
                        on:click=move |_| set_active_tab_fn("url")
                    >
                        "From URL"
                    </button>
                    <button
                        class=move || {
                            let base = "py-2 px-1 border-b-2 font-medium text-sm";
                            if active_tab.get() == "s3" {
                                return format!("{} border-green-500 text-green-600", base);
                            }
                            format!(
                                "{} border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300",
                                base,
                            )
                        }
                        on:click=move |_| set_active_tab_fn("s3")
                    >
                        "From S3"
                    </button>
                </nav>
            </div>

            {move || {
                match active_tab.get().as_str() {
                    "file" => {
                        view! {
                            <FileReader
                                _set_error_message=set_error_message
                                set_parquet_table=set_parquet_table
                            />
                        }
                            .into_any()
                    }
                    "url" => {
                        view! {
                            <UrlReader
                                set_error_message=set_error_message
                                set_parquet_table=set_parquet_table
                            />
                        }
                            .into_any()
                    }
                    "s3" => {
                        view! {
                            <S3Reader
                                set_error_message=set_error_message
                                set_parquet_table=set_parquet_table
                            />
                        }
                            .into_any()
                    }
                    _ => ().into_any(),
                }
            }}
        </div>
    }
}

#[component]
fn FileReader(
    _set_error_message: WriteSignal<Option<String>>,
    set_parquet_table: WriteSignal<Option<ParquetTable>>,
) -> impl IntoView {
    let on_file_select = move |ev: web_sys::Event| {
        let input: web_sys::HtmlInputElement = event_target(&ev);
        let files = input.files().unwrap();
        let file = files.get(0).unwrap();

        let file_reader = web_sys::FileReader::new().unwrap();
        let file_reader_clone = file_reader.clone();

        let table_name = file.name();

        let onload = Closure::wrap(Box::new(move |_: web_sys::Event| {
            let table_name = table_name.clone();
            let result = file_reader_clone.result().unwrap();
            let array_buffer = result.dyn_into::<js_sys::ArrayBuffer>().unwrap();
            let uint8_array = js_sys::Uint8Array::new(&array_buffer);
            let bytes = bytes::Bytes::from(uint8_array.to_vec());
            leptos::task::spawn_local(async move {
                let ctx = SESSION_CTX.as_ref();
                let object_store = INMEMORY_STORE.clone();
                let path = Path::parse(&table_name).unwrap();
                let payload = PutPayload::from_bytes(bytes.clone());
                object_store.put(&path, payload).await.unwrap();
                let meta = object_store
                    .head(&Path::parse(&table_name).unwrap())
                    .await
                    .unwrap();
                let mut reader = ParquetObjectReader::new(object_store, meta)
                    .with_preload_column_index(true)
                    .with_preload_offset_index(true);
                let metadata = reader.get_metadata().await.unwrap();
                ctx.register_parquet(
                    &table_name,
                    &format!("mem:///{}", table_name),
                    Default::default(),
                )
                .await
                .unwrap();
                set_parquet_table.set(Some(ParquetTable {
                    reader,
                    table_name,
                    metadata,
                }));
            });
        }) as Box<dyn FnMut(_)>);

        file_reader.set_onload(Some(onload.as_ref().unchecked_ref()));
        file_reader.read_as_array_buffer(&file).unwrap();
        onload.forget();
    };

    view! {
        <div class="border-2 border-dashed border-gray-300 rounded-lg p-6 text-center space-y-4">
            <div>
                <input type="file" accept=".parquet" on:change=on_file_select id="file-input" />
            </div>
            <div>
                <label for="file-input" class="cursor-pointer text-gray-600">
                    "Drop Parquet file or click to browse"
                </label>
            </div>
        </div>
    }
}

#[component]
fn UrlReader(
    set_error_message: WriteSignal<Option<String>>,
    set_parquet_table: WriteSignal<Option<ParquetTable>>,
) -> impl IntoView {
    let (url_query, set_url_query) = query_signal::<String>("url");
    let default_url = {
        if let Some(url) = url_query.get() {
            url
        } else {
            DEFAULT_URL.to_string()
        }
    };

    let (url, set_url) = signal(default_url);

    let on_url_submit = move || {
        let url_str = url.get();
        set_url_query.set(Some(url_str.clone()));
        set_error_message.set(None);

        let Ok(url) = Url::parse(&url_str) else {
            set_error_message.set(Some(format!("Invalid URL: {}", url_str)));
            return;
        };
        let endpoint = format!(
            "{}://{}{}",
            url.scheme(),
            url.host_str().unwrap(),
            url.port().map_or("".to_string(), |p| format!(":{}", p))
        );
        let path = url.path().to_string();

        let table_name = path
            .split('/')
            .last()
            .unwrap_or("uploaded.parquet")
            .to_string();

        leptos::task::spawn_local(async move {
            let builder = Http::default().endpoint(&endpoint);
            let Ok(op) = Operator::new(builder) else {
                set_error_message.set(Some("Failed to create HTTP operator".into()));
                return;
            };
            let op = op.finish();
            let object_store = Arc::new(ObjectStoreCache::new(OpendalStore::new(op)));
            let meta = object_store
                .head(&Path::parse(&path).unwrap())
                .await
                .unwrap();
            let mut reader = ParquetObjectReader::new(object_store.clone(), meta)
                .with_preload_column_index(true)
                .with_preload_offset_index(true);
            let metadata = reader.get_metadata().await.unwrap();
            let object_store_url = ObjectStoreUrl::parse(&endpoint).unwrap();
            let ctx = SESSION_CTX.as_ref();
            ctx.register_object_store(object_store_url.as_ref(), object_store);
            log!("table_name: {}, url_str: {}", table_name, url_str);
            ctx.register_parquet(&table_name, &url_str, Default::default())
                .await
                .unwrap();

            set_parquet_table.set(Some(ParquetTable {
                reader,
                table_name,
                metadata,
            }));
        });
    };

    match url_query.get() {
        Some(url) => {
            // user provided an url, set it and run it.
            set_url.set(url);
            on_url_submit();
        }
        None => set_url.set(DEFAULT_URL.to_string()),
    }

    view! {
        <div class="h-full flex items-center">
            <form
                on:submit=move |ev| {
                    ev.prevent_default();
                    on_url_submit();
                }
                class="w-full"
            >
                <div class="flex space-x-2">
                    <input
                        type="url"
                        placeholder="Enter Parquet file URL"
                        on:input=move |ev| {
                            set_url.set(event_target_value(&ev));
                        }
                        prop:value=url
                        class="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                    />
                    <button
                        type="submit"
                        class="px-4 py-2 border border-green-500 text-green-500 rounded-md hover:bg-green-50"
                    >
                        "Read URL"
                    </button>
                </div>
            </form>
        </div>
    }
}

#[component]
fn S3Reader(
    set_error_message: WriteSignal<Option<String>>,
    set_parquet_table: WriteSignal<Option<ParquetTable>>,
) -> impl IntoView {
    let (s3_bucket, set_s3_bucket) = signal(get_stored_value(S3_BUCKET_KEY, ""));
    let (s3_region, set_s3_region) = signal(get_stored_value(S3_REGION_KEY, "us-east-1"));
    let (s3_file_path, set_s3_file_path) = signal(get_stored_value(S3_FILE_PATH_KEY, ""));

    let on_s3_bucket_change = move |ev| {
        let value = event_target_value(&ev);
        save_to_storage(S3_BUCKET_KEY, &value);
        set_s3_bucket.set(value);
    };

    let on_s3_region_change = move |ev| {
        let value = event_target_value(&ev);
        save_to_storage(S3_REGION_KEY, &value);
        set_s3_region.set(value);
    };

    let on_s3_file_path_change = move |ev| {
        let value = event_target_value(&ev);
        save_to_storage(S3_FILE_PATH_KEY, &value);
        set_s3_file_path.set(value);
    };

    let on_s3_submit = move || {
        set_error_message.set(None);

        let endpoint = get_stored_value(S3_ENDPOINT_KEY, "https://s3.amazonaws.com");
        let access_key_id = get_stored_value(S3_ACCESS_KEY_ID_KEY, "");
        let secret_key = get_stored_value(S3_SECRET_KEY_KEY, "");

        let bucket = s3_bucket.get();
        let region = s3_region.get();

        // Validate inputs
        if endpoint.is_empty() || bucket.is_empty() || s3_file_path.get().is_empty() {
            set_error_message.set(Some("All fields except region are required".into()));
            return;
        }
        let file_name = s3_file_path
            .get()
            .split('/')
            .last()
            .unwrap_or("uploaded.parquet")
            .to_string();

        leptos::task::spawn_local(async move {
            let cfg = S3::default()
                .endpoint(&endpoint)
                .access_key_id(&access_key_id)
                .secret_access_key(&secret_key)
                .bucket(&bucket)
                .region(&region);

            let path = format!("s3://{}", bucket);

            let op = Operator::new(cfg).unwrap().finish();
            let object_store = Arc::new(OpendalStore::new(op));
            let meta = object_store
                .head(&Path::parse(&s3_file_path.get()).unwrap())
                .await
                .unwrap();
            let mut reader = ParquetObjectReader::new(object_store.clone(), meta)
                .with_preload_column_index(true)
                .with_preload_offset_index(true);
            let metadata = reader.get_metadata().await.unwrap();
            let object_store_url = ObjectStoreUrl::parse(&path).unwrap();
            let ctx = SESSION_CTX.as_ref();
            ctx.register_object_store(object_store_url.as_ref(), object_store);

            ctx.register_parquet(
                &file_name,
                &format!("s3://{}/{}", bucket, s3_file_path.get()),
                Default::default(),
            )
            .await
            .unwrap();

            set_parquet_table.set(Some(ParquetTable {
                reader,
                table_name: file_name,
                metadata,
            }));
        });
    };

    view! {
        <div>
            <form
                on:submit=move |ev| {
                    ev.prevent_default();
                    on_s3_submit();
                }
                class="space-y-4 w-full"
            >
                <div class="flex flex-wrap gap-4">
                    <div class="flex-1 min-w-[200px] max-w-[200px]">
                        <label class="block text-sm font-medium text-gray-700 mb-1">"Bucket"</label>
                        <input
                            type="text"
                            on:input=on_s3_bucket_change
                            prop:value=s3_bucket
                            class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                        />
                    </div>
                    <div class="flex-1 min-w-[150px] max-w-[150px]">
                        <label class="block text-sm font-medium text-gray-700 mb-1">"Region"</label>
                        <input
                            type="text"
                            on:input=on_s3_region_change
                            prop:value=s3_region
                            class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                        />
                    </div>
                    <div class="flex-[2] min-w-[250px]">
                        <label class="block text-sm font-medium text-gray-700 mb-1">
                            "File Path"
                        </label>
                        <input
                            type="text"
                            on:input=on_s3_file_path_change
                            prop:value=s3_file_path
                            class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                        />
                    </div>
                    <div class="flex-1 min-w-[120px] max-w-[120px] self-end">
                        <button
                            type="submit"
                            class="w-full px-4 py-2 border border-green-500 text-green-500 rounded-md hover:border-green-600 hover:text-green-600"
                        >
                            "Read S3"
                        </button>
                    </div>
                </div>
            </form>
        </div>
    }
}
