//! 默认二进制入口。
//!
//! 真正有用的启动入口在 `src/bin/kv_server.rs` 和 `src/bin/kv_client.rs`。
//! 这个文件只给直接运行 `cargo run` 的用户一个提示。

fn main() {
    println!("Use `cargo run --bin kv_server -- --help` or `cargo run --bin kv_client -- --help`.");
}
