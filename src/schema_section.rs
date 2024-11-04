use leptos::*;

#[component]
pub fn SchemaSection(parquet_info: super::ParquetInfo) -> impl IntoView {
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
        <div class="bg-white rounded-lg shadow-md p-6">
            <h2 class="text-xl font-semibold mb-4">"Arrow schema"</h2>
            <div class="space-y-2">
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
                            <div class="flex items-center justify-between p-2 hover:bg-gray-50 rounded">
                                <div class="flex flex-col">
                                    <span class="text-gray-700">{format!("{}.{}", i, field.name())}</span>
                                    <span class="text-xs text-gray-400">{output}</span>
                                </div>
                                <span class="text-sm text-gray-500">{data_type_name}</span>
                            </div>
                        }
                    })
                    .collect::<Vec<_>>()}
            </div>
        </div>
    }
} 