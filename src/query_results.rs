use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::common::cast::{as_binary_array, as_binary_view_array, as_string_view_array};
use leptos::*;

#[component]
pub fn QueryResults(query_result: Vec<RecordBatch>) -> impl IntoView {
    view! {
        <div class="mt-4 p-4 bg-white border border-gray-300 rounded-md">
            {
                move || {
                    if query_result.is_empty() {
                        view! { <p>"No results"</p> }.into_view()
                    } else {
                        let batch = &query_result[0];
                        let schema = batch.schema();

                        view! {
                            <div class="max-h-[32rem] overflow-auto">
                                <table class="min-w-full bg-white border border-gray-300 table-fixed">
                                    <thead>
                                        <tr class="bg-gray-100">
                                            {
                                                schema.fields().iter().map(|field| {
                                                    view! {
                                                        <th class="px-4 py-2 text-left border-b w-48 min-w-48">
                                                            <div class="truncate" title={field.name()}>{field.name()}</div>
                                                        </th>
                                                    }
                                                }).collect::<Vec<_>>()
                                            }
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {
                                            (0..batch.num_rows()).map(|row_idx| {
                                                view! {
                                                    <tr class="hover:bg-gray-50">
                                                        {
                                                            (0..batch.num_columns()).map(|col_idx| {
                                                                let column = batch.column(col_idx);
                                                                let cell_value = if column.is_null(row_idx) {
                                                                    "NULL".to_string()
                                                                } else {
                                                                    column.as_ref().value_to_string(row_idx)
                                                                };

                                                                view! {
                                                                    <td class="px-4 py-2 border-b w-48 min-w-48">
                                                                        <div class="overflow-x-auto whitespace-nowrap" title={cell_value.clone()}>
                                                                            {cell_value}
                                                                        </div>
                                                                    </td>
                                                                }
                                                            }).collect::<Vec<_>>()
                                                        }
                                                    </tr>
                                                }
                                            }).collect::<Vec<_>>()
                                        }
                                    </tbody>
                                </table>
                            </div>
                        }.into_view()
                    }
                }
            }
        </div>
    }
}

trait ArrayExt {
    fn value_to_string(&self, index: usize) -> String;
}

impl ArrayExt for dyn Array {
    fn value_to_string(&self, index: usize) -> String {
        use arrow::array::*;

        let array = self;

        downcast_primitive_array!(
            array => {
                format!("{:?}", array.value(index))
            }
            DataType::Utf8 => {
                let array = as_string_array(array);
                array.value(index).to_string()
            }
            DataType::Utf8View => {
                let array = as_string_view_array(array).unwrap();
                array.value(index).to_string()
            }
            DataType::Binary => {
                let array = as_binary_array(array).unwrap();
                let value = array.value(index);
                String::from_utf8_lossy(value).to_string()
            }
            DataType::BinaryView => {
                let array = as_binary_view_array(array).unwrap();
                let value = array.value(index);
                String::from_utf8_lossy(value).to_string()
            }
            t => format!("Unsupported datatype {}", t)
        )
    }
}
