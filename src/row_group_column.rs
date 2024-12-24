use std::sync::Arc;

use leptos::prelude::*;
use parquet::file::{reader::SerializedPageReader, statistics::Statistics};

use crate::format_rows;

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
                    if let Some(min) = s.min_opt() {
                        if let Ok(min_utf8) = min.as_utf8() {
                            parts.push(format!("min: {:?}", min_utf8));
                        }
                    }
                    if let Some(max) = s.max_opt() {
                        if let Ok(max_utf8) = max.as_utf8() {
                            parts.push(format!("max: {:?}", max_utf8));
                        }
                    }
                }
                Statistics::FixedLenByteArray(s) => {
                    if let Some(min) = s.min_opt() {
                        if let Ok(min_utf8) = min.as_utf8() {
                            parts.push(format!("min: {:?}", min_utf8));
                        }
                    }
                    if let Some(max) = s.max_opt() {
                        if let Ok(max_utf8) = max.as_utf8() {
                            parts.push(format!("max: {:?}", max_utf8));
                        }
                    }
                }
            }

            if let Some(null_count) = stats.null_count_opt() {
                parts.push(format!("nulls: {}", format_rows(null_count)));
            }

            if let Some(distinct_count) = stats.distinct_count_opt() {
                parts.push(format!("distinct: {}", format_rows(distinct_count)));
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
pub fn RowGroupColumn(parquet_reader: super::ParquetReader) -> impl IntoView {
    let (selected_row_group, set_selected_row_group) = signal(0);
    let (selected_column, set_selected_column) = signal(0);

    let parquet_info_clone = parquet_reader.info().clone();
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

    let parquet_info_clone = parquet_reader.info().clone();
    let parquet_bytes = parquet_reader.bytes().clone();
    let column_info = move || {
        let rg = parquet_info_clone
            .metadata
            .row_group(selected_row_group.get());
        let col = rg.column(selected_column.get());
        let row_count = rg.num_rows();
        let compressed_size = col.compressed_size() as f64 / 1_048_576.0;
        let uncompressed_size = col.uncompressed_size() as f64 / 1_048_576.0;
        let compression = col.compression();
        let statistics = col.statistics().cloned();
        let has_bloom_filter = col.bloom_filter_offset().is_some();
        let encodings = col.encodings().clone();

        let parquet_bytes = Arc::new(parquet_bytes.clone());
        let page_reader =
            SerializedPageReader::new(parquet_bytes, col, row_count as usize, None).unwrap();

        let mut page_info = Vec::new();
        for page in page_reader {
            if let Ok(page) = page {
                let page_type = page.page_type();
                let page_size = page.buffer().len() as f64 / 1024.0;
                let num_values = page.num_values();
                page_info.push((page_type, page_size, num_values));
            }
        }

        (
            compressed_size,
            uncompressed_size,
            compression,
            statistics,
            has_bloom_filter,
            encodings,
            page_info,
        )
    };

    view! {
        <div class="space-y-8">
            // Row Group Selection
            <div class="flex flex-col space-y-2">
                <div class="flex items-center">
                    <label for="row-group-select" class="text-sm font-medium text-gray-700 w-32">
                        "Row Group"
                    </label>
                    <select
                        id="row-group-select"
                        class="w-full bg-white text-gray-700 text-sm font-medium rounded-lg border border-gray-200 px-4 py-2.5 hover:border-gray-300 focus:outline-none focus:border-blue-500 appearance-none cursor-pointer bg-[url('data:image/svg+xml;charset=US-ASCII,%3Csvg%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%20width%3D%2224%22%20height%3D%2224%22%20viewBox%3D%220%200%2024%2024%22%20fill%3D%22none%22%20stroke%3D%22%23666%22%20stroke-width%3D%222%22%20stroke-linecap%3D%22round%22%20stroke-linejoin%3D%22round%22%3E%3Cpolyline%20points%3D%226%209%2012%2015%2018%209%22%3E%3C%2Fpolyline%3E%3C%2Fsvg%3E')] bg-[length:1.5em] bg-[right_0.5em_center] bg-no-repeat"
                        on:change=move |ev| {
                            set_selected_row_group
                                .set(event_target_value(&ev).parse::<usize>().unwrap_or(0))
                        }
                    >
                        {(0..parquet_reader.info().row_group_count)
                            .map(|i| {
                                view! {
                                    <option value=i.to_string() class="py-2">
                                        {format!("{}", i)}
                                    </option>
                                }
                            })
                            .collect::<Vec<_>>()}
                    </select>
                </div>

                {move || {
                    let (compressed_size, uncompressed_size, num_rows, compression) = row_group_info();
                    view! {
                        <div class="grid grid-cols-2 gap-4 bg-gray-50 p-4 rounded-md">
                            <div class="space-y-1">
                                <div class="text-sm text-gray-500">"Size"</div>
                                <div class="font-medium">
                                    {format!("{:.2} MB", compressed_size)}
                                </div>
                            </div>
                            <div class="space-y-1">
                                <div class="text-sm text-gray-500">"Uncompressed"</div>
                                <div class="font-medium">
                                    {format!("{:.2} MB", uncompressed_size)}
                                </div>
                            </div>
                            <div class="space-y-1">
                                <div class="text-sm text-gray-500">"Compression"</div>
                                <div class="font-medium">
                                    {format!("{:.1}%", compressed_size / uncompressed_size * 100.0)}
                                </div>
                            </div>
                            <div class="space-y-1">
                                <div class="text-sm text-gray-500">"Rows"</div>
                                <div class="font-medium">{format_rows(num_rows)}</div>
                            </div>
                            <div class="col-span-2 space-y-1">
                                <div class="text-sm text-gray-500">"Compression Type"</div>
                                <div class="font-medium">{format!("{:?}", compression)}</div>
                            </div>
                        </div>
                    }
                }}
            </div>

            // Column Selection
            <div class="flex flex-col space-y-2">
                <div class="flex items-center">
                    <label for="column-select" class="text-sm font-medium text-gray-700 w-32">
                        "Column"
                    </label>
                    <select
                        id="column-select"
                        class="w-full bg-white text-gray-700 text-sm font-medium rounded-lg border border-gray-200 px-4 py-2.5 hover:border-gray-300 focus:outline-none focus:border-blue-500 appearance-none cursor-pointer bg-[url('data:image/svg+xml;charset=US-ASCII,%3Csvg%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%20width%3D%2224%22%20height%3D%2224%22%20viewBox%3D%220%200%2024%2024%22%20fill%3D%22none%22%20stroke%3D%22%23666%22%20stroke-width%3D%222%22%20stroke-linecap%3D%22round%22%20stroke-linejoin%3D%22round%22%3E%3Cpolyline%20points%3D%226%209%2012%2015%2018%209%22%3E%3C%2Fpolyline%3E%3C%2Fsvg%3E')] bg-[length:1.5em] bg-[right_0.5em_center] bg-no-repeat"
                        on:change=move |ev| {
                            set_selected_column
                                .set(event_target_value(&ev).parse::<usize>().unwrap_or(0))
                        }
                    >
                        {parquet_reader.info()
                            .schema
                            .fields
                            .iter()
                            .enumerate()
                            .map(|(i, field)| {
                                view! {
                                    <option value=i.to_string() class="py-2">
                                        {field.name().clone()}
                                    </option>
                                }
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
                        page_info,
                    ) = column_info();
                    view! {
                        <div class="grid grid-cols-2 gap-4 bg-gray-50 p-4 rounded-md">
                            <div class="space-y-1">
                                <div class="text-sm text-gray-500">"Size"</div>
                                <div class="font-medium">
                                    {format!("{:.2} MB", compressed_size)}
                                </div>
                            </div>
                            <div class="space-y-1">
                                <div class="text-sm text-gray-500">"Uncompressed"</div>
                                <div class="font-medium">
                                    {format!("{:.2} MB", uncompressed_size)}
                                </div>
                            </div>
                            <div class="space-y-1">
                                <div class="text-sm text-gray-500">"Compression"</div>
                                <div class="font-medium">
                                    {format!("{:.1}%", compressed_size / uncompressed_size * 100.0)}
                                </div>
                            </div>
                            <div class="space-y-1">
                                <div class="text-sm text-gray-500">"Bloom Filter"</div>
                                <div class="font-medium">
                                    {if has_bloom_filter { "✓" } else { "✗" }}
                                </div>
                            </div>
                            <div class="col-span-2 space-y-1">
                                <div class="text-sm text-gray-500">"Compression Type"</div>
                                <div class="font-medium">{format!("{:?}", compression)}</div>
                            </div>
                            <div class="col-span-2 space-y-1">
                                <div class="text-sm text-gray-500">"Encodings"</div>
                                <div class="font-medium text-sm">{format!("{:?}", encodings)}</div>
                            </div>
                            <div class="col-span-2 space-y-1">
                                <div class="text-sm text-gray-500">"Statistics"</div>
                                <div class="font-medium text-sm">{stats_to_string(statistics)}</div>
                            </div>
                            <div class="col-span-2 space-y-1">
                                <div class="space-y-0.5">
                                    <div class="flex gap-4 text-sm text-gray-500">
                                        <span class="w-16">Page #</span>
                                        <span class="w-32">Type</span>
                                        <span class="w-24">Size</span>
                                        <span>Rows</span>
                                    </div>
                                    <div class="max-h-[250px] overflow-y-auto pr-2">
                                        {page_info
                                            .into_iter()
                                            .enumerate()
                                            .map(|(i, (page_type, size, values))| {
                                                view! {
                                                    <div class="flex gap-4 text-sm">
                                                        <span class="w-16">{format!("{}.", i)}</span>
                                                        <span class="w-32">{format!("{:?}", page_type)}</span>
                                                        <span class="w-24 text-gray-600">
                                                            {format!("{:.1} KB", size)}
                                                        </span>
                                                        <span class="text-gray-600">
                                                            {format_rows(values as u64)}
                                                        </span>
                                                    </div>
                                                }
                                            })
                                            .collect::<Vec<_>>()}
                                    </div>
                                </div>
                            </div>
                        </div>
                    }
                }}
            </div>
        </div>
    }
}
