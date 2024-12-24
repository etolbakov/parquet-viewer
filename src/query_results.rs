use std::sync::Arc;

use arrow::array::{types::*, Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::{
    common::cast::{as_binary_array, as_binary_view_array, as_string_view_array},
    physical_plan::{
        accept, display::DisplayableExecutionPlan, DisplayFormatType, ExecutionPlan,
        ExecutionPlanVisitor,
    },
};
use leptos::{logging, prelude::*};
use parquet::arrow::ArrowWriter;
use web_sys::js_sys;
use web_sys::wasm_bindgen::JsCast;

pub(crate) fn export_to_csv_inner(query_result: &[RecordBatch]) {
    let mut csv_data = String::new();
    
    // Headers remain the same as they're based on schema
    let headers: Vec<String> = query_result[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    csv_data.push_str(&headers.join(","));
    csv_data.push('\n');

    // Process all record batches
    for batch in query_result {
        for row_idx in 0..batch.num_rows() {
            let row: Vec<String> = (0..batch.num_columns())
                .map(|col_idx| {
                    let column = batch.column(col_idx);
                    if column.is_null(row_idx) {
                        "NULL".to_string()
                    } else {
                        column.as_ref().value_to_string(row_idx)
                    }
                })
                .collect();
            csv_data.push_str(&row.join(","));
            csv_data.push('\n');
        }
    }

    // Rest of the function remains the same
    let blob = web_sys::Blob::new_with_str_sequence(&js_sys::Array::of1(&csv_data.into())).unwrap();
    let url = web_sys::Url::create_object_url_with_blob(&blob).unwrap();
    let a = web_sys::window()
        .unwrap()
        .document()
        .unwrap()
        .create_element("a")
        .unwrap();
    a.set_attribute("href", &url).unwrap();
    a.set_attribute("download", "query_results.csv").unwrap();
    a.dyn_ref::<web_sys::HtmlElement>().unwrap().click();
    web_sys::Url::revoke_object_url(&url).unwrap();
}

pub(crate) fn export_to_parquet_inner(query_result: &[RecordBatch]) {
    let mut buf = Vec::new();

    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::LZ4)
        .build();

    let mut writer = ArrowWriter::try_new(&mut buf, query_result[0].schema(), Some(props))
        .expect("Failed to create parquet writer");

    // Write all record batches
    for batch in query_result {
        writer
            .write(batch)
            .expect("Failed to write record batch");
    }

    writer.close().expect("Failed to close writer");

    let array = js_sys::Uint8Array::from(&buf[..]);
    let blob = web_sys::Blob::new_with_u8_array_sequence(&js_sys::Array::of1(&array))
        .expect("Failed to create blob");

    // Create a download link
    let url =
        web_sys::Url::create_object_url_with_blob(&blob).expect("Failed to create object URL");
    let a = web_sys::window()
        .unwrap()
        .document()
        .unwrap()
        .create_element("a")
        .unwrap();
    a.set_attribute("href", &url).unwrap();
    a.set_attribute("download", "query_results.parquet")
        .unwrap();
    a.dyn_ref::<web_sys::HtmlElement>().unwrap().click();
    web_sys::Url::revoke_object_url(&url).unwrap();
}

#[component]
pub fn QueryResults(
    sql_query: String,
    query_result: Vec<RecordBatch>,
    physical_plan: Arc<dyn ExecutionPlan>,
) -> impl IntoView {
    let (active_tab, set_active_tab) = signal("results".to_string());

    let sql = sql_query.clone();

    view! {
        <div class="mt-4 p-4 bg-white border border-gray-300 rounded-md">
            <div class="mb-4 p-3 bg-gray-50 rounded border border-gray-200 font-mono text-sm overflow-x-auto relative group">
                {sql.clone()}
                <button
                    class="absolute top-2 right-2 p-2 text-gray-500 hover:text-gray-700"
                    on:click=move |_| {
                        let window = web_sys::window().unwrap();
                        let navigator = window.navigator();
                        let clipboard = navigator.clipboard();
                        let _ = clipboard.write_text(&sql);
                    }
                >
                    <svg
                        xmlns="http://www.w3.org/2000/svg"
                        class="h-5 w-5"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                    >
                        <path
                            stroke-linecap="round"
                            stroke-linejoin="round"
                            stroke-width="2"
                            d="M8 5H6a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2v-1M8 5a2 2 0 002 2h2a2 2 0 002-2M8 5a2 2 0 012-2h2a2 2 0 012 2m0 0h2a2 2 0 012 2v3m2 4H10m0 0l3-3m-3 3l3 3"
                        />
                    </svg>
                </button>
            </div>
            <div class="mb-4 border-b border-gray-300 flex items-center">
                <button
                    class=move || {
                        format!(
                            "px-4 py-2 {} {}",
                            if active_tab() == "results" {
                                "border-b-2 border-blue-500 text-blue-600"
                            } else {
                                "text-gray-600"
                            },
                            "hover:text-blue-600",
                        )
                    }
                    on:click=move |_| set_active_tab("results".to_string())
                >
                    "Query Results"
                </button>
                <button
                    class=move || {
                        format!(
                            "px-4 py-2 {} {}",
                            if active_tab() == "physical_plan" {
                                "border-b-2 border-blue-500 text-blue-600"
                            } else {
                                "text-gray-600"
                            },
                            "hover:text-blue-600",
                        )
                    }
                    on:click=move |_| set_active_tab("physical_plan".to_string())
                >
                    "ExecutionPlan"
                </button>

            </div>

            {move || match active_tab().as_str() {
                "results" => {
                    let query_result_csv = query_result.clone();
                    let export_to_csv = move |_| {
                        export_to_csv_inner(&query_result_csv);
                    };
                    let query_result_parquet = query_result.clone();
                    let export_to_parquet = move |_| {
                        export_to_parquet_inner(&query_result_parquet);
                    };

                    view! {
                        <div class="max-h-[32rem] overflow-auto relative">

                            <button
                                class="mb-4 mx-2 px-2 py-1 border border-gray-500 text-gray-500 text-sm rounded hover:bg-gray-100"
                                on:click=export_to_csv
                            >
                                "Export to CSV"
                            </button>
                            <button
                                class="mb-4 mx-2 px-2 py-1 border border-gray-500 text-gray-500 text-sm rounded hover:bg-gray-100"
                                on:click=export_to_parquet
                            >
                                "Export to Parquet"
                            </button>
                            <table class="min-w-full bg-white border border-gray-300 table-fixed">
                                <thead class="sticky top-0 z-10">
                                    <tr class="bg-gray-100">
                                        {query_result[0]
                                            .schema()
                                            .fields()
                                            .iter()
                                            .map(|field| {
                                                view! {
                                                    <th class="px-4 py-1 text-left border-b w-48 min-w-48 bg-gray-100 leading-tight text-gray-700">
                                                        <div class="truncate" title=field.name().clone()>
                                                            {field.name().clone()}
                                                        </div>
                                                        <div
                                                            class="text-xs text-gray-600 truncate"
                                                            title=field.data_type().to_string()
                                                        >
                                                            {field.data_type().to_string()}
                                                        </div>
                                                    </th>
                                                }
                                            })
                                            .collect::<Vec<_>>()}
                                    </tr>
                                </thead>
                                <tbody>
                                    {(0..query_result[0].num_rows())
                                        .map(|row_idx| {
                                            view! {
                                                <tr class="hover:bg-gray-50">
                                                    {(0..query_result[0].num_columns())
                                                        .map(|col_idx| {
                                                            let column = query_result[0].column(col_idx);
                                                            let cell_value = if column.is_null(row_idx) {
                                                                "NULL".to_string()
                                                            } else {
                                                                column.as_ref().value_to_string(row_idx)
                                                            };

                                                            view! {
                                                                <td class="px-4 py-1 border-b w-48 min-w-48 leading-tight text-gray-700">
                                                                    <div
                                                                        class="overflow-x-auto whitespace-nowrap"
                                                                        title=cell_value.clone()
                                                                    >
                                                                        {cell_value.clone()}
                                                                    </div>
                                                                </td>
                                                            }
                                                        })
                                                        .collect::<Vec<_>>()}
                                                </tr>
                                            }
                                        })
                                        .collect::<Vec<_>>()}
                                </tbody>
                            </table>
                        </div>
                    }
                        .into_any()
                }
                "physical_plan" => {
                    view! { <PhysicalPlan physical_plan=physical_plan.clone() /> }.into_any()
                }
                _ => view! { <p>"Invalid tab"</p> }.into_any(),
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
            DataType::Dictionary(key_type, _) => {
                match key_type.as_ref() {
                    DataType::Int8 => {
                        let array = as_dictionary_array::<Int8Type>(array);
                        let values = array.values();
                        values.value_to_string(array.key(index).unwrap_or_default())
                    }
                    DataType::Int16 => {
                        let array = as_dictionary_array::<Int16Type>(array);
                        let values = array.values();
                        values.value_to_string(array.key(index).unwrap_or_default())
                    }
                    DataType::Int32 => {
                        let array = as_dictionary_array::<Int32Type>(array);
                        let values = array.values();
                        values.value_to_string(array.key(index).unwrap_or_default())
                    }
                    DataType::Int64 => {
                        let array = as_dictionary_array::<Int64Type>(array);
                        let values = array.values();
                        values.value_to_string(array.key(index).unwrap_or_default())
                    }
                    DataType::UInt8 => {
                        let array = as_dictionary_array::<UInt8Type>(array);
                        let values = array.values();
                        values.value_to_string(array.key(index).unwrap_or_default())
                    }
                    DataType::UInt16 => {
                        let array = as_dictionary_array::<UInt16Type>(array);
                        let values = array.values();
                        values.value_to_string(array.key(index).unwrap_or_default())
                    }
                    DataType::UInt32 => {
                        let array = as_dictionary_array::<UInt32Type>(array);
                        let values = array.values();
                        values.value_to_string(array.key(index).unwrap_or_default())
                    }
                    DataType::UInt64 => {
                        let array = as_dictionary_array::<UInt64Type>(array);
                        let values = array.values();
                        values.value_to_string(array.key(index).unwrap_or_default())
                    }
                    _ => format!("Unsupported dictionary key type {}", key_type),
                }
            }
            t => format!("Unsupported datatype {}", t)
        )
    }
}

#[derive(Debug, Clone)]
struct PlanNode {
    _id: usize,
    name: String,
    label: String,
    metrics: Option<String>,
    children: Vec<PlanNode>,
}

struct TreeBuilder {
    next_id: usize,
    current_path: Vec<PlanNode>,
}

struct DisplayPlan<'a> {
    plan: &'a dyn ExecutionPlan,
}

impl std::fmt::Display for DisplayPlan<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.plan.fmt_as(DisplayFormatType::Default, f)
    }
}

impl ExecutionPlanVisitor for TreeBuilder {
    type Error = std::fmt::Error;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        let name = plan.name().to_string();
        let label = format!("{}", DisplayPlan { plan });

        let metrics = plan.metrics().map(|m| {
            let metrics = m
                .aggregate_by_name()
                .sorted_for_display()
                .timestamps_removed();
            format!("{metrics}")
        });

        let node = PlanNode {
            _id: self.next_id,
            name,
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

#[component]
fn PlanNode(node: PlanNode) -> impl IntoView {
    view! {
        <div class="relative">
            <div class="flex flex-col items-center">
                <div class="p-4 border rounded-lg bg-white shadow-sm hover:shadow-md transition-shadow">
                    <div class="font-medium">{node.name}</div>
                    <div class="text-sm text-gray-700 mt-1 font-mono">{node.label}</div>
                    {node
                        .metrics
                        .map(|m| {
                            view! { <div class="text-sm text-blue-600 mt-1 italic">{m}</div> }
                        })}
                </div>

                {(!node.children.is_empty())
                    .then(|| {
                        view! {
                            <div class="relative pt-4">
                                <svg
                                    class="absolute top-0 left-1/2 -translate-x-[0.5px] h-4 w-1 z-10"
                                    overflow="visible"
                                >
                                    <line
                                        x1="0.5"
                                        y1="16"
                                        x2="0.5"
                                        y2="0"
                                        stroke="#D1D5DB"
                                        stroke-width="1"
                                        marker-end="url(#global-arrowhead)"
                                    />
                                </svg>

                                <div class="relative flex items-center justify-center">
                                    {(node.children.len() > 1)
                                        .then(|| {
                                            view! {
                                                <svg
                                                    class="absolute top-0 h-[1px]"
                                                    style="left: 25%; width: 50%;"
                                                    overflow="visible"
                                                >
                                                    <line
                                                        x1="0"
                                                        y1="0.5"
                                                        x2="100%"
                                                        y2="0.5"
                                                        stroke="#D1D5DB"
                                                        stroke-width="1"
                                                    />
                                                </svg>
                                            }
                                        })}
                                </div>

                                <div class="flex gap-8">
                                    {node
                                        .children
                                        .into_iter()
                                        .map(|child| view! { <PlanNode node=child /> })
                                        .collect::<Vec<_>>()}
                                </div>
                            </div>
                        }
                    })}
            </div>
        </div>
    }
    .into_any()
}

#[component]
pub fn PhysicalPlan(physical_plan: Arc<dyn ExecutionPlan>) -> impl IntoView {
    let mut builder = TreeBuilder {
        next_id: 0,
        current_path: vec![],
    };
    let displayable_plan = DisplayableExecutionPlan::with_metrics(physical_plan.as_ref());
    accept(physical_plan.as_ref(), &mut builder).unwrap();
    let root = builder.current_path.pop().unwrap();
    logging::log!("{}", displayable_plan.indent(true).to_string());

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
                        orient="auto"
                    >
                        <polygon points="0 0, 10 3.5, 0 7" fill="#D1D5DB" />
                    </marker>
                </defs>
            </svg>

            <div class="p-8 overflow-auto">
                <PlanNode node=root />
            </div>
        </div>
    }
}
