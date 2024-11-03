use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use leptos::*;
use parquet::{
    arrow::parquet_to_arrow_schema,
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

#[component]
fn SchemaSection(parquet_info: ParquetInfo) -> impl IntoView {
    let schema = parquet_info.schema.clone();
    let metadata = parquet_info.metadata.clone();
    let mut column_info =
        vec![(0, 0, metadata.row_group(0).column(0).compression()); schema.fields.len()];
    for rg in metadata.row_groups() {
        for (i, col) in rg.columns().iter().enumerate() {
            column_info[i].0 += col.compressed_size() as u64;
            column_info[i].1 += col.uncompressed_size() as u64;
            column_info[i].2 = col.compression();
        }
    }

    fn format_size(size: u64) -> String {
        if size > 1_048_576 {
            // 1MB
            format!("{:.2} MB", size as f64 / 1_048_576.0)
        } else if size > 1024 {
            // 1KB
            format!("{:.2} KB", size as f64 / 1024.0)
        } else {
            format!("{} B", size)
        }
    }
    view! {
        <div class="info-section">
            <div class="section-header">"Arrow schema"</div>
            <div class="schema-grid">
                {schema
                    .fields
                    .into_iter()
                    .enumerate()
                    .map(|(i, field)| {
                        let data_type_name = format!("{}", field.data_type());
                        let output = format!(
                            "{}/{}",
                            format_size(column_info[i].0),
                            format_size(column_info[i].1),
                        );
                        view! {
                            <div class="schema-item">
                                <span class="label">{format!("{}.{}", i, field.name())}</span>
                                <span class="schema-type">{data_type_name}</span>
                                <span class="schema-size">{output}</span>
                            </div>
                        }
                    })
                    .collect::<Vec<_>>()}
            </div>
        </div>
    }
}

#[component]
fn InfoSection(parquet_info: ParquetInfo) -> impl IntoView {
    let created_by = parquet_info
        .metadata
        .file_metadata()
        .created_by()
        .unwrap_or("Unknown")
        .to_string();
    let version = parquet_info.metadata.file_metadata().version();

    // Create a signal for the selected row group
    let (selected_row_group, set_selected_row_group) = create_signal(0);

    view! {
        <div class="info-section">
            <div class="section-header">"Basic Information"</div>
            <div class="info-grid">
                <div class="info-item">
                    <span class="label">"File size"</span>
                    <span class="value">
                        {format!("{:.2} MB", parquet_info.file_size as f64 / 1_048_576.0)}
                    </span>
                </div>
                <div class="info-item">
                    <span class="label">"Metadata size"</span>
                    <span class="value">
                        {format!("{:.2} KB", parquet_info.metadata_len as f64 / 1024.0)}
                    </span>
                </div>
                <div class="info-item">
                    <span class="label">"Uncompressed size"</span>
                    <span class="value">
                        {format!("{:.2} MB", parquet_info.uncompressed_size as f64 / 1_048_576.0)}
                    </span>
                </div>
                <div class="info-item">
                    <span class="label">"Compression ratio"</span>
                    <span class="value">
                        {format!("{:.2}%", parquet_info.compression_ratio * 100.0)}
                    </span>
                </div>
                <div class="info-item">
                    <span class="label">"Row groups"</span>
                    <span class="value">{parquet_info.row_group_count}</span>
                </div>
                <div class="info-item">
                    <span class="label">"Total rows"</span>
                    <span class="value">{format_rows(parquet_info.row_count)}</span>
                </div>
                <div class="info-item">
                    <span class="label">"Columns"</span>
                    <span class="value">{parquet_info.columns}</span>
                </div>
                <div class="info-item">
                    <span class="label">"Created by"</span>
                    <span class="value">{created_by}</span>
                </div>
                <div class="info-item">
                    <span class="label">"Version"</span>
                    <span class="value">{version}</span>
                </div>
            </div>

            <RowGroupSection
                parquet_info=parquet_info.clone()
                selected_row_group=selected_row_group
                set_selected_row_group=set_selected_row_group
            />

            <div class="section-header" style="margin-top: 1.5rem">
                "Features"
            </div>
            <div class="features">
                <div class="feature".to_owned()
                    + if parquet_info.has_row_group_stats {
                        " active"
                    } else {
                        ""
                    }>
                    {if parquet_info.has_row_group_stats { "✓" } else { "✗" }}
                    " Row Group Statistics"
                </div>
                <div class="feature".to_owned()
                    + if parquet_info.has_column_index {
                        " active"
                    } else {
                        ""
                    }>
                    {if parquet_info.has_column_index { "✓" } else { "✗" }} " Column Index"
                </div>
                <div class="feature".to_owned()
                    + if parquet_info.has_page_index {
                        " active"
                    } else {
                        ""
                    }>{if parquet_info.has_page_index { "✓" } else { "✗" }} " Page Index"</div>
                <div class="feature".to_owned()
                    + if parquet_info.has_bloom_filter {
                        " active"
                    } else {
                        ""
                    }>
                    {if parquet_info.has_bloom_filter { "✓" } else { "✗" }} " Bloom Filter"
                </div>
            </div>
        </div>
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
                _ => {}
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

#[component]
fn RowGroupSection(
    parquet_info: ParquetInfo,
    selected_row_group: ReadSignal<usize>,
    set_selected_row_group: WriteSignal<usize>,
) -> impl IntoView {
    let (selected_column, set_selected_column) = create_signal(0);

    let parquet_info_clone = parquet_info.clone();
    let row_group_info = move || {
        let rg = parquet_info_clone
            .metadata
            .row_group(selected_row_group.get());
        let compressed_size = rg.compressed_size() as f64 / 1_048_576.0;
        let uncompressed_size = rg.total_byte_size() as f64 / 1_048_576.0;
        let num_rows = rg.num_rows() as u64;
        let compression = rg.column(0).compression();
        (compressed_size, uncompressed_size, num_rows, compression)
    };

    let parquet_info_clone = parquet_info.clone();

    let column_info = move || {
        let rg = parquet_info_clone
            .metadata
            .row_group(selected_row_group.get());
        let col = rg.column(selected_column.get());
        let compressed_size = col.compressed_size() as f64 / 1_048_576.0;
        let uncompressed_size = col.uncompressed_size() as f64 / 1_048_576.0;
        let compression = col.compression();
        let statistics = col.statistics().cloned();
        let has_bloom_filter = col.bloom_filter_offset().is_some();
        let encodings = col.encodings().clone();
        (
            compressed_size,
            uncompressed_size,
            compression,
            statistics,
            has_bloom_filter,
            encodings,
        )
    };

    view! {
        <div class="row-group-section">

            <div class="selector">
                <select
                    id="row-group-select"
                    on:change=move |ev| {
                        set_selected_row_group
                            .set(event_target_value(&ev).parse::<usize>().unwrap_or(0))
                    }
                >
                    {(0..parquet_info.row_group_count)
                        .map(|i| {
                            view! {
                                <option value=i.to_string()>{format!("Row Group {}", i)}</option>
                            }
                        })
                        .collect::<Vec<_>>()}
                </select>
            </div>

            {move || {
                let (compressed_size, uncompressed_size, num_rows, compression) = row_group_info();
                view! {
                    <div class="info-grid">
                        <div class="info-item">
                            <span class="label">"Size"</span>
                            <span class="value">{format!("{:.2} MB", compressed_size)}</span>
                        </div>
                        <div class="info-item">
                            <span class="label">"Uncompressed size"</span>
                            <span class="value">{format!("{:.2} MB", uncompressed_size)}</span>
                        </div>
                        <div class="info-item">
                            <span class="label">"Compression ratio"</span>
                            <span class="value">
                                {format!("{:.2}%", compressed_size / uncompressed_size * 100.0)}
                            </span>
                        </div>
                        <div class="info-item">
                            <span class="label">"Row count"</span>
                            <span class="value">{format_rows(num_rows)}</span>
                        </div>
                        <div class="info-item">
                            <span class="label">"Compression"</span>
                            <span class="value">{format!("{:?}", compression)}</span>
                        </div>
                    </div>
                }
            }}
            <div class="selector">
                <select
                    id="column-select"
                    on:change=move |ev| {
                        set_selected_column
                            .set(event_target_value(&ev).parse::<usize>().unwrap_or(0))
                    }
                >
                    {parquet_info
                        .schema
                        .fields
                        .iter()
                        .enumerate()
                        .map(|(i, field)| {
                            view! { <option value=i.to_string()>{field.name()}</option> }
                        })
                        .collect::<Vec<_>>()}
                </select>
            </div>
            {move || {
                let (
                    compressed_size,
                    uncompressed_size,
                    compression,
                    statistics,
                    has_bloom_filter,
                    encodings,
                ) = column_info();
                view! {
                    <div class="info-grid">
                        <div class="info-item">
                            <span class="label">"Size"</span>
                            <span class="value">{format!("{:.2} MB", compressed_size)}</span>
                        </div>
                        <div class="info-item">
                            <span class="label">"Uncompressed size"</span>
                            <span class="value">{format!("{:.2} MB", uncompressed_size)}</span>
                        </div>
                        <div class="info-item">
                            <span class="label">"Compression ratio"</span>
                            <span class="value">
                                {format!("{:.2}%", compressed_size / uncompressed_size * 100.0)}
                            </span>
                        </div>
                        <div class="info-item">
                            <span class="label">"Compression"</span>
                            <span class="value">{format!("{:?}", compression)}</span>
                        </div>
                        <div class="info-item">
                            <span class="label">"Encodings"</span>
                            <span class="value">{format!("{:?}", encodings)}</span>
                        </div>
                        <div class="info-item">
                            <span class="label">"Bloom Filter"</span>
                            <span class="value">
                                {if has_bloom_filter { "✓" } else { "✗" }}
                            </span>
                        </div>
                        <div class="info-item">
                            <span class="label">"Statistics"</span>
                            <span class="value">{stats_to_string(statistics)}</span>
                        </div>
                    </div>
                }
            }}
        </div>
    }
}

#[component]
fn App() -> impl IntoView {
    let default_url = "https://raw.githubusercontent.com/RobinL/iris_parquet/main/gridwatch/gridwatch_2023-01-08.parquet";
    let (url, set_url) = create_signal(default_url.to_string());
    let (file_content, set_file_content) = create_signal(None);
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
            <h1 class="title">
                "Parquet Explorer"
                <a
                    href="https://github.com/XiangpengHao/parquet-explorer"
                    target="_blank"
                    class="github-link"
                >
                    <svg height="24" width="24" viewBox="0 0 16 16" class="github-icon">
                        <path
                            fill="currentColor"
                            d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"
                        ></path>
                    </svg>
                </a>
            </h1>
            <div class="input-container">
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
                <div class="separator">
                    <span class="or-text">"OR"</span>
                </div>
                <div class="url-input-container">
                    <form on:submit=on_url_submit>
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
                            class="url-input"
                        />
                        <button type="submit" class="url-submit">
                            "Load from URL"
                        </button>
                    </form>
                    {move || {
                        error_message
                            .get()
                            .map(|msg| {
                                view! {
                                    <div class="error-message">
                                        {msg}
                                        <div class="error-help">
                                            "Tip: Try these sources:" <ul>
                                                <li>
                                                    "Grid watch data: "
                                                    <code>
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
                </div>
            </div>
            <div class="parquet-info">
                {move || {
                    let info = file_content.get();
                    match info {
                        Some(info) => {
                            view! {
                                <div class="info-container">
                                    <InfoSection parquet_info=info.clone() />
                                    <SchemaSection parquet_info=info.clone() />
                                </div>
                            }
                                .into_view()
                        }
                        None => view! { <div class="error">"No file selected"</div> }.into_view(),
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
