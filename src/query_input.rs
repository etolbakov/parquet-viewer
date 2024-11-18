use std::sync::Arc;

use leptos::*;

#[component]
pub fn QueryInput(
    sql_query: ReadSignal<String>,
    set_sql_query: WriteSignal<String>,
    file_name: ReadSignal<String>,
    execute_query: Arc<dyn Fn()>,
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
                on:dblclick=move |_| {
                    if sql_query.get().trim().is_empty() {
                        set_sql_query(format!("select * from \"{}\" limit 10", file_name.get()))
                    }
                }
                on:keydown=move |ev| {
                    if ev.key() == "Enter" {
                        key_down();
                    }
                }
                class="flex-1 px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
            <button
                on:click=move |_| execute_query()
                class="px-4 py-2 bg-green-500 text-white rounded-md hover:bg-green-600 whitespace-nowrap"
            >
                "Run Query"
            </button>
        </div>
    }
} 