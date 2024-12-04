use leptos::*;

#[derive(Clone, Debug, PartialEq)]
struct ColumnData {
    id: usize,
    name: String,
    data_type: String,
    compressed_size: u64,
    uncompressed_size: u64,
    compression_ratio: f64,
}

#[derive(Clone, Copy, PartialEq)]
enum SortField {
    Id,
    Name,
    DataType,
    CompressedSize,
    UncompressedSize,
    CompressionRatio,
}

#[component]
pub fn SchemaSection(parquet_info: super::ParquetInfo) -> impl IntoView {
    let schema = parquet_info.schema.clone();
    let metadata = parquet_info.metadata.clone();
    let mut column_info = vec![
        (
            0,
            0,
            metadata
                .row_groups()
                .first()
                .map(|rg| rg.columns().first().map(|c| c.compression()))
                .flatten(),
        );
        schema.fields.len()
    ];
    for rg in metadata.row_groups() {
        for (i, col) in rg.columns().iter().enumerate() {
            column_info[i].0 += col.compressed_size() as u64;
            column_info[i].1 += col.uncompressed_size() as u64;
            column_info[i].2 = Some(col.compression());
        }
    }

    let (sort_field, set_sort_field) = create_signal(SortField::Id);
    let (sort_ascending, set_sort_ascending) = create_signal(true);

    // Transform the data into ColumnData structs
    let column_data = create_memo(move |_| {
        let mut data: Vec<ColumnData> = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let compressed = column_info[i].0;
                let uncompressed = column_info[i].1;
                ColumnData {
                    id: i,
                    name: field.name().to_string(),
                    data_type: format!("{}", field.data_type()),
                    compressed_size: compressed,
                    uncompressed_size: uncompressed,
                    compression_ratio: if uncompressed > 0 {
                        compressed as f64 / uncompressed as f64
                    } else {
                        0.0
                    },
                }
            })
            .collect();

        // Sort the data based on current sort field
        data.sort_by(|a, b| {
            let cmp = match sort_field.get() {
                SortField::Id => a.id.cmp(&b.id),
                SortField::Name => a.name.cmp(&b.name),
                SortField::DataType => a.data_type.cmp(&b.data_type),
                SortField::CompressedSize => a.compressed_size.cmp(&b.compressed_size),
                SortField::UncompressedSize => a.uncompressed_size.cmp(&b.uncompressed_size),
                SortField::CompressionRatio => a
                    .compression_ratio
                    .partial_cmp(&b.compression_ratio)
                    .unwrap(),
            };
            if sort_ascending.get() {
                cmp
            } else {
                cmp.reverse()
            }
        });
        data
    });

    let sort_by = move |field: SortField| {
        if sort_field.get() == field {
            set_sort_ascending.update(|v| *v = !*v);
        } else {
            set_sort_field.set(field);
            set_sort_ascending.set(true);
        }
    };

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
        <div class="bg-white rounded-lg shadow-md p-6 flex-1 overflow-auto">
            <h2 class="text-xl font-semibold mb-4">"Arrow Schema"</h2>
            <table class="min-w-full table-fixed">
                <thead>
                    <tr class="bg-gray-50">
                        <th
                            class="px-4 py-2 cursor-pointer hover:bg-gray-100"
                            on:click=move |_| sort_by(SortField::Id)
                        >
                            "ID"
                        </th>
                        <th
                            class="px-4 py-2 cursor-pointer hover:bg-gray-100"
                            on:click=move |_| sort_by(SortField::Name)
                        >
                            "Name"
                        </th>
                        <th
                            class="px-4 py-2 cursor-pointer hover:bg-gray-100"
                            on:click=move |_| sort_by(SortField::DataType)
                        >
                            "Type"
                        </th>
                        <th
                            class="px-4 py-2 cursor-pointer hover:bg-gray-100"
                            on:click=move |_| sort_by(SortField::CompressedSize)
                        >
                            "Compressed"
                        </th>
                        <th
                            class="px-4 py-2 cursor-pointer hover:bg-gray-100"
                            on:click=move |_| sort_by(SortField::UncompressedSize)
                        >
                            "Uncompressed"
                        </th>
                        <th
                            class="px-4 py-2 cursor-pointer hover:bg-gray-100"
                            on:click=move |_| sort_by(SortField::CompressionRatio)
                        >
                            "Ratio"
                        </th>
                    </tr>
                </thead>
                <tbody>
                    {move || {
                        column_data
                            .get()
                            .into_iter()
                            .map(|col| {
                                view! {
                                    <tr class="hover:bg-gray-50">
                                        <td class="px-4 py-2 text-gray-700">{col.id}</td>
                                        <td class="px-4 py-2 text-gray-700">{col.name}</td>
                                        <td class="px-4 py-2 text-gray-500">{col.data_type}</td>
                                        <td class="px-4 py-2 text-gray-500">
                                            {format_size(col.compressed_size)}
                                        </td>
                                        <td class="px-4 py-2 text-gray-500">
                                            {format_size(col.uncompressed_size)}
                                        </td>
                                        <td class="px-4 py-2 text-gray-500">
                                            {format!("{:.2}%", col.compression_ratio * 100.0)}
                                        </td>
                                    </tr>
                                }
                            })
                            .collect::<Vec<_>>()
                    }}
                </tbody>
            </table>
        </div>
    }
}
