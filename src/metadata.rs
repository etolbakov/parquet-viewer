use leptos::prelude::*;

#[component]
pub fn MetadataSection(parquet_info: super::ParquetInfo) -> impl IntoView {
    let created_by = parquet_info
        .metadata
        .file_metadata()
        .created_by()
        .unwrap_or("Unknown")
        .to_string();
    let version = parquet_info.metadata.file_metadata().version();
    let has_bloom_filter = parquet_info.has_bloom_filter;
    let has_page_index = parquet_info.has_page_index;
    let has_column_index = parquet_info.has_column_index;
    let has_row_group_stats = parquet_info.has_row_group_stats;

    // Create a signal for the selected row group
    let (selected_row_group, set_selected_row_group) = signal(0);

    view! {
        <div class="bg-white rounded-lg border border-gray-300 p-6">
            <div class="grid grid-cols-2 gap-6">
                <div>
                    <h2 class="text-xl font-semibold mb-4">"Metadata"</h2>
                    <div class="grid grid-cols-2 gap-4 bg-gray-50 p-4 rounded-md mb-8">
                        <div class="space-y-2">
                            <span class="text-gray-600 text-sm">"File size"</span>
                            <span class="block font-medium">
                                {format!("{:.2} MB", parquet_info.file_size as f64 / 1_048_576.0)}
                            </span>
                        </div>
                        <div class="space-y-2">
                            <span class="text-gray-600 text-sm">"Metadata size"</span>
                            <span class="block font-medium">
                                {format!("{:.2} KB", parquet_info.metadata_len as f64 / 1024.0)}
                            </span>
                        </div>
                        <div class="space-y-2">
                            <span class="text-gray-600 text-sm">"Uncompressed size"</span>
                            <span class="block font-medium">
                                {format!(
                                    "{:.2} MB",
                                    parquet_info.uncompressed_size as f64 / 1_048_576.0,
                                )}
                            </span>
                        </div>
                        <div class="space-y-2">
                            <span class="text-gray-600 text-sm">"Compression ratio"</span>
                            <span class="block font-medium">
                                {format!("{:.2}%", parquet_info.compression_ratio * 100.0)}
                            </span>
                        </div>
                        <div class="space-y-2">
                            <span class="text-gray-600 text-sm">"Row groups"</span>
                            <span class="block font-medium">{parquet_info.row_group_count}</span>
                        </div>
                        <div class="space-y-2">
                            <span class="text-gray-600 text-sm">"Total rows"</span>
                            <span class="block font-medium">
                                {super::format_rows(parquet_info.row_count)}
                            </span>
                        </div>
                        <div class="space-y-2">
                            <span class="text-gray-600 text-sm">"Columns"</span>
                            <span class="block font-medium">{parquet_info.columns}</span>
                        </div>
                        <div class="space-y-2">
                            <span class="text-gray-600 text-sm">"Created by"</span>
                            <span class="block font-medium">{created_by}</span>
                        </div>
                        <div class="space-y-2">
                            <span class="text-gray-600 text-sm">"Version"</span>
                            <span class="block font-medium">{version}</span>
                        </div>
                    </div>

                    <h2 class="text-xl font-semibold mt-6 mb-4">"Features"</h2>
                    <div class="grid grid-cols-2 gap-2">
                        <div class="p-2 rounded ".to_owned()
                            + if has_row_group_stats {
                                "bg-green-100 text-green-800"
                            } else {
                                "bg-gray-100 text-gray-800"
                            }>
                            {if has_row_group_stats { "✓" } else { "✗" }}
                            " Row Group Statistics"
                        </div>
                        <div class="p-2 rounded ".to_owned()
                            + if has_column_index {
                                "bg-green-100 text-green-800"
                            } else {
                                "bg-gray-100 text-gray-800"
                            }>{if has_column_index { "✓" } else { "✗" }} " Column Index"</div>
                        <div class="p-2 rounded ".to_owned()
                            + if has_page_index {
                                "bg-green-100 text-green-800"
                            } else {
                                "bg-gray-100 text-gray-800"
                            }>{if has_page_index { "✓" } else { "✗" }} " Page Index"</div>
                        <div class="p-2 rounded ".to_owned()
                            + if has_bloom_filter {
                                "bg-green-100 text-green-800"
                            } else {
                                "bg-gray-100 text-gray-800"
                            }>{if has_bloom_filter { "✓" } else { "✗" }} " Bloom Filter"</div>
                    </div>
                </div>

                {move || {
                    if parquet_info.row_group_count > 0 {
                        Some(
                            view! {
                                <div>
                                    <super::row_group::RowGroupSection
                                        parquet_info=parquet_info.clone()
                                        selected_row_group=selected_row_group
                                        set_selected_row_group=set_selected_row_group
                                    />
                                </div>
                            },
                        )
                    } else {
                        None
                    }
                }}
            </div>
        </div>
    }
}
