use leptos::prelude::*;
use leptos::wasm_bindgen::{prelude::Closure, JsCast};
use leptos_router::hooks::query_signal;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use opendal::{services::Http, services::S3, Operator};
use web_sys::{js_sys, Url};

use crate::{ParquetTable, INMEMORY_STORE, SESSION_CTX};

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

async fn update_file(
    parquet_table: ParquetTable,
    parquet_table_setter: WriteSignal<Option<ParquetTable>>,
) {
    let ctx = SESSION_CTX.as_ref();
    let object_store = &*INMEMORY_STORE;
    let path = Path::parse(&parquet_table.table_name).unwrap();
    let payload = PutPayload::from_bytes(parquet_table.bytes.clone());
    object_store.put(&path, payload).await.unwrap();
    ctx.register_parquet(
        &parquet_table.table_name,
        &format!("mem:///{}", parquet_table.table_name),
        Default::default(),
    )
    .await
    .unwrap();
    parquet_table_setter.set(Some(parquet_table));
}

#[component]
pub fn FileReader(
    set_error_message: WriteSignal<Option<String>>,
    set_parquet_table: WriteSignal<Option<ParquetTable>>,
) -> impl IntoView {
    let (active_tab, set_active_tab) = query_signal::<String>("tab");

    let (url_query, set_url_query) = query_signal::<String>("url");
    let default_url = {
        if let Some(url) = url_query.get() {
            url
        } else {
            "https://raw.githubusercontent.com/RobinL/iris_parquet/main/gridwatch/gridwatch_2023-01-08.parquet".to_string()
        }
    };
    let (url, set_url) = signal(default_url);

    let (s3_endpoint, _) = signal(get_stored_value(
        S3_ENDPOINT_KEY,
        "https://s3.amazonaws.com",
    ));
    let (s3_access_key_id, _) = signal(get_stored_value(S3_ACCESS_KEY_ID_KEY, ""));
    let (s3_secret_key, _) = signal(get_stored_value(S3_SECRET_KEY_KEY, ""));

    let (s3_bucket_query, set_s3_bucket_query) = query_signal::<String>("bucket");
    let default_bucket = {
        if let Some(bucket) = s3_bucket_query.get() {
            bucket
        } else {
            get_stored_value(S3_BUCKET_KEY, "")
        }
    };
    let (s3_bucket, set_s3_bucket) = signal(default_bucket);

    let (s3_region_query, set_s3_region_query) = query_signal::<String>("region");
    let default_region = {
        if let Some(region) = s3_region_query.get() {
            region
        } else {
            get_stored_value(S3_REGION_KEY, "us-east-1")
        }
    };
    let (s3_region, set_s3_region) = signal(default_region);

    let (s3_file_path_query, set_s3_file_path_query) = query_signal::<String>("file_path");
    let default_file_path = {
        if let Some(file_path) = s3_file_path_query.get() {
            file_path
        } else {
            get_stored_value(S3_FILE_PATH_KEY, "")
        }
    };
    let (s3_file_path, set_s3_file_path) = signal(default_file_path);

    let (is_folded, set_is_folded) = signal(false);

    Effect::watch(
        url,
        move |url, _, _| {
            let Some(active_tab) = active_tab.get() else {
                return;
            };
            if active_tab == "url" {
                set_url_query.set(Some(url.clone()));
            }
        },
        true,
    );

    let set_active_tab_fn = move |tab: &str| {
        match tab {
            "file" => {
                set_url_query.set(None);
                set_s3_bucket_query.set(None);
                set_s3_region_query.set(None);
                set_s3_file_path_query.set(None);
            }
            "url" => {
                set_url_query.set(Some(url.get()));
                set_s3_bucket_query.set(None);
                set_s3_region_query.set(None);
                set_s3_file_path_query.set(None);
            }
            "s3" => {
                set_url_query.set(None);
                set_s3_bucket_query.set(Some(s3_bucket.get()));
                set_s3_region_query.set(Some(s3_region.get()));
                set_s3_file_path_query.set(Some(s3_file_path.get()));
            }
            _ => {}
        }
        if let Some(active_t) = active_tab.get() {
            if active_t == tab {
                set_is_folded.set(!is_folded.get());
            } else {
                set_active_tab.set(Some(tab.to_string()));
                set_is_folded.set(false);
            }
        } else {
            set_active_tab.set(Some(tab.to_string()));
            set_is_folded.set(false);
        }
    };

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
            let parquet_table = ParquetTable { bytes, table_name };
            leptos::task::spawn_local(async move {
                update_file(parquet_table, set_parquet_table).await;
                set_is_folded.set(true);
            });
        }) as Box<dyn FnMut(_)>);

        file_reader.set_onload(Some(onload.as_ref().unchecked_ref()));
        file_reader.read_as_array_buffer(&file).unwrap();
        onload.forget();
    };

    let on_url_submit = move || {
        let url_str = url.get();
        set_error_message.set(None);

        let Ok(url) = Url::new(&url_str) else {
            set_error_message.set(Some(format!("Invalid URL: {}", url_str)));
            return;
        };
        // NOTE: protocol will include `:`, for example: `https:`
        let endpoint = format!("{}//{}", url.protocol(), url.host());
        // We don't support query so far.
        let path = url.pathname();

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

            match op.read(&path).await {
                Ok(bs) => {
                    let parquet_table = ParquetTable {
                        bytes: bs.to_bytes(),
                        table_name,
                    };
                    update_file(parquet_table, set_parquet_table).await;
                    set_is_folded.set(true);
                }
                Err(e) => {
                    set_error_message.set(Some(format!("Failed to read from HTTP: {}", e)));
                }
            }
        });
    };

    let on_s3_submit = move || {
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

            match Operator::new(cfg) {
                Ok(op) => {
                    let operator = op.finish();
                    match operator.read(&s3_file_path.get()).await {
                        Ok(bs) => {
                            let parquet_table = ParquetTable {
                                bytes: bs.to_bytes(),
                                table_name: file_name,
                            };
                            update_file(parquet_table, set_parquet_table).await;
                            set_is_folded.set(true);
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

    match active_tab.get() {
        Some(tab) => match tab.as_str() {
            "url" => on_url_submit(),
            "s3" => on_s3_submit(),
            _ => {}
        },
        None => set_active_tab_fn("file"),
    }

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

    view! {
        <div class="bg-white rounded-lg border border-gray-300 p-3">
            <div class="border-b border-gray-200">
                <nav class="-mb-px flex space-x-8">
                    <button
                        class=move || {
                            let base = "py-2 px-1 border-b-2 font-medium text-sm";
                            if let Some(active_t) = active_tab.get() {
                                if active_t == "file" {
                                    return format!("{} border-green-500 text-green-600", base);
                                }
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
                            if let Some(active_t) = active_tab.get() {
                                if active_t == "url" {
                                    return format!("{} border-green-500 text-green-600", base);
                                }
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
                            if let Some(active_t) = active_tab.get() {
                                if active_t == "s3" {
                                    return format!("{} border-green-500 text-green-600", base);
                                }
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
                let transition_class = if is_folded.get() {
                    "max-h-0 overflow-hidden transition-all duration-300 ease-in-out"
                } else {
                    "max-h-[500px] overflow-hidden transition-all duration-300 ease-in-out p-6"
                };
                let active_tab = active_tab.get();
                let Some(active_tab) = active_tab else {
                    return ().into_any();
                };
                match active_tab.as_str() {
                    "file" => {

                        view! {
                            <div class=move || transition_class.to_string()>
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
                                        <label
                                            for="file-input"
                                            class="cursor-pointer text-gray-600"
                                        >
                                            "Drop Parquet file or click to browse"
                                        </label>
                                    </div>
                                </div>
                            </div>
                        }
                            .into_any()
                    }
                    "url" => {
                        view! {
                            <div class=move || transition_class.to_string()>
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
                                                class="px-4 py-2 bg-green-500 text-white rounded-md hover:bg-green-600"
                                            >
                                                "Load from URL"
                                            </button>
                                        </div>
                                    </form>
                                </div>
                            </div>
                        }
                            .into_any()
                    }
                    "s3" => {
                        view! {
                            <div class=move || transition_class.to_string()>
                                <form
                                    on:submit=move |ev| {
                                        ev.prevent_default();
                                        on_s3_submit();
                                    }
                                    class="space-y-4 w-full"
                                >
                                    <div class="flex flex-wrap gap-4">
                                        <div class="flex-1 min-w-[250px]">
                                            <label class="block text-sm font-medium text-gray-700 mb-1">
                                                "Bucket"
                                            </label>
                                            <input
                                                type="text"
                                                on:input=on_s3_bucket_change
                                                prop:value=s3_bucket
                                                class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                                            />
                                        </div>
                                        <div class="flex-1 min-w-[250px]">
                                            <label class="block text-sm font-medium text-gray-700 mb-1">
                                                "Region"
                                            </label>
                                            <input
                                                type="text"
                                                on:input=on_s3_region_change
                                                prop:value=s3_region
                                                class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-green-500"
                                            />
                                        </div>
                                        <div class="flex-1 min-w-[250px]">
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
                                        <div class="flex-1 min-w-[150px] max-w-[250px] self-end">
                                            <button
                                                type="submit"
                                                class="w-full px-4 py-2 bg-green-500 text-white rounded-md hover:bg-green-600"
                                            >
                                                "Read from S3"
                                            </button>
                                        </div>
                                    </div>
                                </form>
                            </div>
                        }
                            .into_any()
                    }
                    _ => ().into_any(),
                }
            }}
        </div>
    }
}
