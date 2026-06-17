fn main() {
    if let Ok(protoc) = protoc_bin_vendored::protoc_bin_path() {
        std::env::set_var("PROTOC", protoc);
    }
    println!("cargo:rerun-if-changed=proto/raft.proto");
    println!("cargo:rerun-if-changed=proto/kv.proto");

    prost_build::Config::new()
        .compile_protos(&["proto/raft.proto", "proto/kv.proto"], &["proto"])
        .expect("compile protobuf files");
}
