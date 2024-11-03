use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use leptos::*;
use parquet::{
    arrow::parquet_to_arrow_schema,
    errors::ParquetError,
    file::{metadata::ParquetMetaData, metadata::ParquetMetaDataReader},
};
use wasm_bindgen::{prelude::Closure, JsCast};
use wasm_bindgen_futures::JsFuture;
use web_sys::js_sys;

#[derive(Debug, Clone)]
struct ParquetInfo {
    file_size: u64,
    row_group_count: u64,
    row_count: u64,
    columns: u64,
    has_row_group_stats: bool,
    has_column_index: bool,
    has_page_index: bool,
    has_bloom_filter: bool,
    schema: SchemaRef,
}

impl ParquetInfo {
    fn from_metadata(metadata: &ParquetMetaData, file_size: u64) -> Result<Self, ParquetError> {
        let schema = parquet_to_arrow_schema(
            metadata.file_metadata().schema_descr(),
            metadata.file_metadata().key_value_metadata(),
        )?;

        Ok(Self {
            file_size,
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
        })
    }
}

fn get_parquet_info(bytes: Bytes) -> Result<ParquetInfo, ParquetError> {
    let file_size = bytes.len() as u64;
    let mut metadata_reader = ParquetMetaDataReader::new()
        .with_page_indexes(true)
        .with_column_indexes(true)
        .with_offset_indexes(true);
    metadata_reader.try_parse(&bytes)?;
    let metadata = metadata_reader.finish()?;

    let parquet_info = ParquetInfo::from_metadata(&metadata, file_size)?;

    Ok(parquet_info)
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

#[component]
fn App() -> impl IntoView {
    let (file_content, set_file_content) = create_signal(None);
    let (url, set_url) = create_signal(String::new());
    let (error_message, set_error_message) = create_signal(Option::<String>::None);

    let on_file_select = move |ev: web_sys::Event| {
        let input: web_sys::HtmlInputElement = event_target(&ev);
        let files = input.files().unwrap();
        let file = files.get(0).unwrap();

        const MAX_SIZE: f64 = 20.0 * 1024.0 * 1024.0; // 20MB in bytes
        let file_size = file.size();
        let start = if file_size > MAX_SIZE {
            file_size - MAX_SIZE
        } else {
            0.0
        };
        let blob = file.slice_with_f64_and_f64(start, file_size).unwrap();

        let file_reader = web_sys::FileReader::new().unwrap();
        let file_content_setter = set_file_content.clone();
        let file_reader_clone = file_reader.clone();

        let onload = Closure::wrap(Box::new(move |_: web_sys::Event| {
            let result = file_reader_clone.result().unwrap();
            let array_buffer = result.dyn_into::<js_sys::ArrayBuffer>().unwrap();
            let uint8_array = js_sys::Uint8Array::new(&array_buffer);
            let bytes = uint8_array.to_vec();
            let parquet_info = get_parquet_info(bytes::Bytes::from(bytes));

            match parquet_info {
                Ok(info) => {
                    web_sys::console::log_1(&info.to_string().into());
                    file_content_setter.set(Some(info));
                }
                Err(_e) => {
                    file_content_setter.set(None);
                }
            }
        }) as Box<dyn FnMut(_)>);

        file_reader.set_onload(Some(onload.as_ref().unchecked_ref()));
        file_reader.read_as_array_buffer(&blob).unwrap();
        onload.forget();
    };

    let on_url_submit = move |ev: web_sys::SubmitEvent| {
        ev.prevent_default();
        let url_str = url.get();
        set_error_message.set(None); // Clear previous errors

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
            let bytes = uint8_array.to_vec();

            let parquet_info = get_parquet_info(bytes::Bytes::from(bytes));
            match parquet_info {
                Ok(info) => {
                    set_file_content.set(Some(info));
                }
                Err(_) => {
                    set_file_content.set(None);
                }
            }
        });
    };

    view! {
        <div>
            <div class="file-input-container">
                <input
                    type="file"
                    accept=".parquet"
                    on:change=on_file_select
                    class="file-input"
                />
                <div class="file-input-help">
                    "Drop your Parquet file here or click to browse"
                </div>
            </div>
            <div class="url-input-container">
                <form on:submit=on_url_submit>
                    <input
                        type="url"
                        placeholder="Enter Parquet file URL (e.g., https://raw.githubusercontent.com/RobinL/iris_parquet/main/gridwatch/gridwatch_2023-01-08.parquet)"
                        on:input=move |ev| {
                            set_url.set(event_target_value(&ev));
                        }
                        prop:value=url
                        class="url-input"
                    />
                    <button type="submit" class="url-submit">
                        "Load from URL"
                    </button>
                </form>
                {move || error_message.get().map(|msg| view! {
                    <div class="error-message">
                        {msg}
                        <div class="error-help">
                            "Tip: Try these sources:"
                            <ul>
                                <li>"Grid watch data: " <code>"https://raw.githubusercontent.com/RobinL/iris_parquet/main/gridwatch/gridwatch_2023-01-08.parquet"</code></li>
                                <li>"Your own S3 bucket with CORS enabled"</li>
                                <li>"Or download the file and use the file picker above"</li>
                            </ul>
                        </div>
                    </div>
                })}
            </div>
            <div class="parquet-info">
                {move || {
                    let info = file_content.get();
                    match info {
                        Some(info) => view! {
                            <div class="info-container">
                                <div class="info-section">
                                    <div class="section-header">"Basic Information"</div>
                                    <div class="info-grid">
                                        <div class="info-item">
                                            <span class="label">"File size"</span>
                                            <span class="value">{format!("{:.2} MB", info.file_size as f64 / 1_048_576.0)}</span>
                                        </div>
                                        <div class="info-item">
                                            <span class="label">"Row groups"</span>
                                            <span class="value">{info.row_group_count}</span>
                                        </div>
                                        <div class="info-item">
                                            <span class="label">"Total rows"</span>
                                            <span class="value">{info.row_count}</span>
                                        </div>
                                        <div class="info-item">
                                            <span class="label">"Columns"</span>
                                            <span class="value">{info.columns}</span>
                                        </div>
                                    </div>

                                    <div class="section-header" style="margin-top: 1.5rem">"Features"</div>
                                    <div class="features">
                                        <div class={"feature".to_owned() + if info.has_row_group_stats {" active"} else {""}}>
                                            {if info.has_row_group_stats {"✓"} else {"✗"}} " Row Group Statistics"
                                        </div>
                                        <div class={"feature".to_owned() + if info.has_column_index {" active"} else {""}}>
                                            {if info.has_column_index {"✓"} else {"✗"}} " Column Index"
                                        </div>
                                        <div class={"feature".to_owned() + if info.has_page_index {" active"} else {""}}>
                                            {if info.has_page_index {"✓"} else {"✗"}} " Page Index"
                                        </div>
                                        <div class={"feature".to_owned() + if info.has_bloom_filter {" active"} else {""}}>
                                            {if info.has_bloom_filter {"✓"} else {"✗"}} " Bloom Filter"
                                        </div>
                                    </div>
                                </div>
                                <div class="schema-section">
                                    <div class="schema-header">"Schema"</div>
                                    <div class="schema-grid">
                                        {info.schema.fields.into_iter().map(|field| {
                                            let data_type_name = format!("{}", field.data_type());
                                            view! {
                                                <div class="schema-item">
                                                    <span class="schema-name">{field.name()}</span>
                                                    <span class="schema-type">{data_type_name}</span>
                                                </div>
                                            }
                                        }).collect::<Vec<_>>()}
                                    </div>
                                </div>
                            </div>
                        }.into_view(),
                        None => view! {
                            <div class="error">
                                "No file selected"
                            </div>
                        }.into_view(),
                    }
                }}
            </div>
        </div>
    }
}

fn main() {
    console_error_panic_hook::set_once();
    mount_to_body(|| view! { <App /> })
}
