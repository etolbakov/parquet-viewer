use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::{
    common::cast::{as_binary_array, as_binary_view_array, as_string_view_array},
    logical_expr::LogicalPlan,
    physical_plan::{accept, ExecutionPlan, ExecutionPlanVisitor},
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
                    <div class="max-h-[32rem] overflow-auto relative">
                        <table class="min-w-full bg-white border border-gray-300 table-fixed">
                            <thead class="sticky top-0 z-10">
                                <tr class="bg-gray-100">
                                    {
                                        query_result[0].schema().fields().iter().map(|field| {
                                            view! {
                                                <th class="px-4 py-2 text-left border-b w-48 min-w-48 bg-gray-100">
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
                "physical_plan" => view! {
                    <PhysicalPlan physical_plan=physical_plan.clone() />
                }.into_view(),
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

#[component]
pub fn PhysicalPlan(physical_plan: Arc<dyn ExecutionPlan>) -> impl IntoView {
    #[derive(Debug, Clone)]
    struct PlanNode {
        id: usize,
        label: String,
        metrics: Option<String>,
        children: Vec<PlanNode>,
    }

    struct TreeBuilder {
        next_id: usize,
        current_path: Vec<PlanNode>,
    }

    impl ExecutionPlanVisitor for TreeBuilder {
        type Error = std::fmt::Error;

        fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
            let label = plan.name().to_string();

            let metrics = plan.metrics().map(|m| {
                let metrics = m
                    .aggregate_by_name()
                    .sorted_for_display()
                    .timestamps_removed();
                format!("{metrics}")
            });

            let node = PlanNode {
                id: self.next_id,
                label,
                metrics,
                children: vec![],
            };

            self.next_id += 1;
            self.current_path.push(node);
            Ok(true)
        }

        fn post_visit(&mut self, _: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
            if self.current_path.len() >= 2 {
                let child = self.current_path.pop().unwrap();
                self.current_path.last_mut().unwrap().children.push(child);
            }
            Ok(true)
        }
    }

    let mut builder = TreeBuilder {
        next_id: 0,
        current_path: vec![],
    };
    accept(physical_plan.as_ref(), &mut builder).unwrap();
    let root = builder.current_path.pop().unwrap();

    #[component]
    fn PlanNode(node: PlanNode) -> impl IntoView {
        view! {
            <div class="relative">
                <div class="flex flex-col items-center">
                    <div class="p-4 border rounded-lg bg-white shadow-sm hover:shadow-md transition-shadow">
                        <div class="font-medium">{node.label}</div>
                        {node.metrics.map(|m| view! {
                            <div class="text-sm text-gray-600 mt-1">{m}</div>
                        })}
                    </div>

                    {(!node.children.is_empty()).then(|| view! {
                        <div class="relative pt-4">
                            <svg class="absolute top-0 left-1/2 -translate-x-[0.5px] h-4 w-1" overflow="visible">
                                <defs>
                                    <marker
                                        id="arrowhead"
                                        markerWidth="10"
                                        markerHeight="7"
                                        refX="0"
                                        refY="3.5"
                                        orient="auto">
                                        <polygon
                                            points="10 0, 0 3.5, 10 7"
                                            fill="#D1D5DB"
                                        />
                                    </marker>
                                </defs>
                                <line
                                    x1="0.5"
                                    y1="0"
                                    x2="0.5"
                                    y2="16"
                                    stroke="#D1D5DB"
                                    stroke-width="1"
                                    marker-end="url(#arrowhead)"
                                />
                            </svg>

                            <div class="relative flex items-center justify-center">
                                {(node.children.len() > 1).then(|| view! {
                                    <svg class="absolute top-0 h-[1px]" style="left: 25%; width: 50%;" overflow="visible">
                                        <line
                                            x1="0"
                                            y1="0.5"
                                            x2="100%"
                                            y2="0.5"
                                            stroke="#D1D5DB"
                                            stroke-width="1"
                                        />
                                    </svg>
                                })}
                            </div>

                            <div class="flex gap-8">
                                {node.children.into_iter().map(|child| view! {
                                    <PlanNode node={child}/>
                                }).collect::<Vec<_>>()}
                            </div>
                        </div>
                    })}
                </div>
            </div>
        }
    }

    view! {
        <div class="relative">
            <svg class="absolute" width="0" height="0">
                <defs>
                    <marker
                        id="global-arrowhead"
                        markerWidth="10"
                        markerHeight="7"
                        refX="9"
                        refY="3.5"
                        orient="auto">
                        <polygon
                            points="0 0, 10 3.5, 0 7"
                            fill="#D1D5DB"
                        />
                    </marker>
                </defs>
            </svg>

            <div class="p-8 overflow-auto">
                <PlanNode node={root}/>
            </div>
        </div>
    }
}
