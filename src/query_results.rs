use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::{
    common::cast::{as_binary_array, as_binary_view_array, as_string_view_array},
    logical_expr::LogicalPlan,
    physical_plan::{display::DisplayableExecutionPlan, ExecutionPlan},
};
use leptos::*;

#[component]
pub fn QueryResults(
    query_result: Vec<RecordBatch>,
    logical_plan: LogicalPlan,
    physical_plan: Arc<dyn ExecutionPlan>,
) -> impl IntoView {
    let (active_tab, set_active_tab) = create_signal("results".to_string());

    view! {
        <div class="mt-4 p-4 bg-white border border-gray-300 rounded-md">
            <div class="mb-4 border-b border-gray-300">
                <button
                    class=move || format!(
                        "px-4 py-2 {} {}",
                        if active_tab() == "results" { "border-b-2 border-blue-500 text-blue-600" } else { "text-gray-600" },
                        "hover:text-blue-600"
                    )
                    on:click=move |_| set_active_tab("results".to_string())
                >
                    "Query Results"
                </button>
                <button
                    class=move || format!(
                        "px-4 py-2 {} {}",
                        if active_tab() == "logical_plan" { "border-b-2 border-blue-500 text-blue-600" } else { "text-gray-600" },
                        "hover:text-blue-600"
                    )
                    on:click=move |_| set_active_tab("logical_plan".to_string())
                >
                    "Logical Plan"
                </button>
                <button
                    class=move || format!(
                        "px-4 py-2 {} {}",
                        if active_tab() == "logical_graphviz" { "border-b-2 border-blue-500 text-blue-600" } else { "text-gray-600" },
                        "hover:text-blue-600"
                    )
                    on:click=move |_| set_active_tab("logical_graphviz".to_string())
                >
                    "Logical GraphViz"
                </button>
                <button
                    class=move || format!(
                        "px-4 py-2 {} {}",
                        if active_tab() == "physical_plan" { "border-b-2 border-blue-500 text-blue-600" } else { "text-gray-600" },
                        "hover:text-blue-600"
                    )
                    on:click=move |_| set_active_tab("physical_plan".to_string())
                >
                    "Physical Plan"
                </button>
            </div>

            {move || match active_tab().as_str() {
                "results" => view! {
                    <div class="max-h-[32rem] overflow-auto">
                        <table class="min-w-full bg-white border border-gray-300 table-fixed">
                            <thead>
                                <tr class="bg-gray-100">
                                    {
                                        query_result[0].schema().fields().iter().map(|field| {
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
                                    (0..query_result[0].num_rows()).map(|row_idx| {
                                        view! {
                                            <tr class="hover:bg-gray-50">
                                                {
                                                    (0..query_result[0].num_columns()).map(|col_idx| {
                                                        let column = query_result[0].column(col_idx);
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
                }.into_view(),
                "logical_plan" => view! {
                    <div class="whitespace-pre-wrap font-mono">
                        {logical_plan.display_indent().to_string()}
                    </div>
                }.into_view(),
                "logical_graphviz" => view! {
                    <div class="whitespace-pre-wrap font-mono">
                        <p class="mb-4">Copy the following GraphViz DOT code and visualize it at:
                            <a
                                href="https://dreampuf.github.io/GraphvizOnline"
                                target="_blank"
                                class="text-blue-600 hover:underline"
                            >
                                GraphvizOnline
                            </a>
                        </p>
                        {logical_plan.display_graphviz().to_string()}
                    </div>
                }.into_view(),
                "physical_plan" => {
                    let displayable = DisplayableExecutionPlan::with_metrics(physical_plan.as_ref());
                    view! {
                        <div class="whitespace-pre-wrap font-mono">
                            {format!("{}", displayable.indent(true))}
                        </div>
                    }.into_view()
            },
                _ => view! { <p>"Invalid tab"</p> }.into_view()
            }}
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
