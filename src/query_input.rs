use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{
        collect, stream::RecordBatchStreamAdapter, streaming::PartitionStream, ExecutionPlan,
    },
};
use leptos::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::ParquetInfo;

use futures::StreamExt;
#[derive(Debug)]
struct DummyStreamPartition {
    schema: SchemaRef,
    bytes: Bytes,
}

impl PartitionStream for DummyStreamPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let parquet_builder = ParquetRecordBatchReaderBuilder::try_new(self.bytes.clone()).unwrap();
        let reader = parquet_builder.build().unwrap();
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::iter(reader)
                .map(|batch| batch.map_err(|e| DataFusionError::ArrowError(e, None))),
        ))
    }
}

pub(crate) async fn execute_query_inner(
    table_name: &str,
    parquet_info: ParquetInfo,
    data: Bytes,
    query: &str,
) -> Result<(Vec<RecordBatch>, Arc<dyn ExecutionPlan>), DataFusionError> {
    web_sys::console::log_1(&table_name.into());
    let ctx = datafusion::prelude::SessionContext::new();

    let schema = parquet_info.schema.clone();

    let streaming_table = datafusion::datasource::streaming::StreamingTable::try_new(
        schema.clone(),
        vec![Arc::new(DummyStreamPartition {
            schema: schema.clone(),
            bytes: data.clone(),
        })],
    )?;

    ctx.register_table(table_name, Arc::new(streaming_table))?;

    let plan = ctx.sql(&query).await?;

    let (state, plan) = plan.into_parts();
    let plan = state.optimize(&plan)?;

    web_sys::console::log_1(&plan.display_indent().to_string().into());

    let physical_plan = state.create_physical_plan(&plan).await?;

    let results = collect(physical_plan.clone(), ctx.task_ctx().clone()).await?;
    Ok((results, physical_plan))
}

#[component]
pub fn QueryInput(
    sql_query: ReadSignal<String>,
    set_sql_query: WriteSignal<String>,
    file_name: ReadSignal<String>,
    execute_query: Arc<dyn Fn(String)>,
) -> impl IntoView {
    let key_down = execute_query.clone();
    view! {
        <div class="flex gap-2 items-center">
            <input
                type="text"
                placeholder=move || {
                    format!("select * from \"{}\" limit 10", file_name.get())
                }
                prop:value=sql_query
                on:input=move |ev| set_sql_query(event_target_value(&ev))
                on:keydown=move |ev| {
                    if ev.key() == "Enter" {
                        key_down(sql_query.get());
                    }
                }
                class="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
            <button
                on:click=move |_| execute_query(sql_query.get())
                class="px-4 py-2 bg-green-500 text-white rounded-md hover:bg-green-600 whitespace-nowrap"
            >
                "Run Query"
            </button>
        </div>
    }
}
