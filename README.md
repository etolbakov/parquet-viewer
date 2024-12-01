# Parquet Viewer

Online at: https://parquet-viewer.xiangpeng.systems

### Features

- View Parquet metadata ✅
- Explore Parquet data with SQL ✅
- Ask questions about Parquet data with natural language ✅
- View Parquet files from local file system, S3, and URLs ✅
- Everything runs in the browser, no data upload ✅

### Demo

![screenshot](doc/parquet-viewer.gif)



## Development

It compiles [parquet-rs](https://github.com/apache/arrow-rs) to WebAssembly and uses it to explore Parquet files, [more details](https://blog.haoxp.xyz/posts/parquet-viewer/).


Checkout the awesome [Leptos](https://github.com/leptos-rs/leptos) framework.

```bash
trunk serve --open

trunk build --release
```
