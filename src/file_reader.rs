use bytes::Bytes;
use leptos::prelude::*;
use leptos::wasm_bindgen::{prelude::Closure, JsCast};
use opendal::{services::S3, Operator};
use wasm_bindgen_futures::JsFuture;
use web_sys::js_sys;

const S3_ENDPOINT_KEY: &str = "s3_endpoint";
const S3_ACCESS_KEY_ID_KEY: &str = "s3_access_key_id";
const S3_SECRET_KEY_KEY: &str = "s3_secret_key";
const S3_BUCKET_KEY: &str = "s3_bucket";
const S3_REGION_KEY: &str = "s3_region";
const S3_FILE_PATH_KEY: &str = "s3_file_path";

fn get_stored_value(key: &str, default: &str) -> String {
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

#[component]
pub fn FileReader(
    set_error_message: WriteSignal<Option<String>>,
    set_file_bytes: WriteSignal<Option<Bytes>>,
    set_file_name: WriteSignal<String>,
) -> impl IntoView {
    let default_url = "https://raw.githubusercontent.com/RobinL/iris_parquet/main/gridwatch/gridwatch_2023-01-08.parquet";
    let (url, set_url) = signal(default_url.to_string());
    let (active_tab, set_active_tab) = signal("file".to_string());
    let (s3_endpoint, set_s3_endpoint) = signal(get_stored_value(
        S3_ENDPOINT_KEY,
        "https://s3.amazonaws.com",
    ));
    let (s3_access_key_id, set_s3_access_key_id) =
        signal(get_stored_value(S3_ACCESS_KEY_ID_KEY, ""));
    let (s3_secret_key, set_s3_secret_key) = signal(get_stored_value(S3_SECRET_KEY_KEY, ""));
    let (s3_bucket, set_s3_bucket) = signal(get_stored_value(S3_BUCKET_KEY, ""));
    let (s3_region, set_s3_region) = signal(get_stored_value(S3_REGION_KEY, "us-east-1"));
    let (s3_file_path, set_s3_file_path) = signal(get_stored_value(S3_FILE_PATH_KEY, ""));
    let (is_folded, set_is_folded) = signal(false);

    let set_active_tab = move |tab: &str| {
        if active_tab.get() == tab {
            set_is_folded.set(!is_folded.get());
        } else {
            set_active_tab.set(tab.to_string());
            set_is_folded.set(false);
        }
    };

    let on_file_select = move |ev: web_sys::Event| {
        let input: web_sys::HtmlInputElement = event_target(&ev);
        let files = input.files().unwrap();
        let file = files.get(0).unwrap();

        let file_reader = web_sys::FileReader::new().unwrap();
        let file_reader_clone = file_reader.clone();

        let onload = Closure::wrap(Box::new(move |_: web_sys::Event| {
            let result = file_reader_clone.result().unwrap();
            let array_buffer = result.dyn_into::<js_sys::ArrayBuffer>().unwrap();
            let uint8_array = js_sys::Uint8Array::new(&array_buffer);
            let bytes = bytes::Bytes::from(uint8_array.to_vec());
            set_file_bytes.set(Some(bytes.clone()));
            set_is_folded.set(true);
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
                Ok(bytes) => {
                    set_file_bytes.set(Some(bytes.clone()));
                    set_is_folded.set(true);
                }
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
        let file_name = s3_file_path
            .get()
            .split('/')
            .last()
            .unwrap_or("uploaded.parquet")
            .strip_suffix(".parquet")
            .unwrap_or("uploaded")
            .to_string();
        set_file_name.set(file_name);

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
                            set_file_bytes.set(Some(bytes.clone()));
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

    let on_s3_endpoint_change = move |ev| {
        let value = event_target_value(&ev);
        save_to_storage(S3_ENDPOINT_KEY, &value);
        set_s3_endpoint.set(value);
    };

    let on_s3_access_key_change = move |ev| {
        let value = event_target_value(&ev);
        save_to_storage(S3_ACCESS_KEY_ID_KEY, &value);
        set_s3_access_key_id.set(value);
    };

    let on_s3_secret_key_change = move |ev| {
        let value = event_target_value(&ev);
        save_to_storage(S3_SECRET_KEY_KEY, &value);
        set_s3_secret_key.set(value);
    };

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
                            if active_tab.get() == "file" {
                                format!("{} border-blue-500 text-blue-600", base)
                            } else {
                                format!(
                                    "{} border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300",
                                    base,
                                )
                            }
                        }
                        on:click=move |_| set_active_tab("file")
                    >
                        "From file"
                    </button>
                    <button
                        class=move || {
                            let base = "py-2 px-1 border-b-2 font-medium text-sm";
                            if active_tab.get() == "url" {
                                format!("{} border-blue-500 text-blue-600", base)
                            } else {
                                format!(
                                    "{} border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300",
                                    base,
                                )
                            }
                        }
                        on:click=move |_| set_active_tab("url")
                    >
                        "From URL"
                    </button>
                    <button
                        class=move || {
                            let base = "py-2 px-1 border-b-2 font-medium text-sm";
                            if active_tab.get() == "s3" {
                                format!("{} border-blue-500 text-blue-600", base)
                            } else {
                                format!(
                                    "{} border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300",
                                    base,
                                )
                            }
                        }
                        on:click=move |_| set_active_tab("s3")
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
                match active_tab.get().as_str() {
                    "file" => {
                        view! {
                            <div class=move || format!("{}", transition_class)>
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
                            <div class=move || format!("{}", transition_class)>
                                <div class="h-full flex items-center">
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
                                </div>
                            </div>
                        }
                            .into_any()
                    }
                    "s3" => {
                        view! {
                            <div class=move || format!("{}", transition_class)>
                                <form on:submit=on_s3_submit class="space-y-4 w-full">
                                    <div class="flex flex-wrap gap-4">
                                        <div class="flex-1 min-w-[250px]">
                                            <label class="block text-sm font-medium text-gray-700 mb-1">
                                                "S3 Endpoint"
                                            </label>
                                            <input
                                                type="text"
                                                placeholder="https://s3.amazonaws.com"
                                                on:input=on_s3_endpoint_change
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
                                                on:input=on_s3_access_key_change
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
                                                on:input=on_s3_secret_key_change
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
                                                on:input=on_s3_bucket_change
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
                                                on:input=on_s3_region_change
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
                                                on:input=on_s3_file_path_change
                                                prop:value=s3_file_path
                                                class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                                            />
                                        </div>
                                        <div class="flex-1 min-w-[150px] max-w-[250px] self-end">
                                            <button
                                                type="submit"
                                                class="w-full px-4 py-2 bg-blue-500 text-white rounded-md hover:bg-blue-600"
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
                    _ => view! {}.into_any(),
                }
            }}
        </div>
    }
}
