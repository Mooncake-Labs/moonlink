fn main() {
    // Ensure protoc is available via vendored binary
    let protoc_path = protoc_bin_vendored::protoc_bin_path().expect("protoc not found");
    std::env::set_var("PROTOC", protoc_path);
    let proto_files = &["proto/row.proto"];
    let proto_includes = &["proto"];

    // Re-run build.rs if any proto changes
    for p in proto_files {
        println!("cargo:rerun-if-changed={p}");
    }
    println!("cargo:rerun-if-changed=build.rs");

    prost_build::Config::new()
        .bytes([
            ".moonlink.RowValue.bytes",
            ".moonlink.RowValue.fixed_len_bytes",
        ]) // map bytes fields to bytes::Bytes if desired
        .compile_protos(proto_files, proto_includes)
        .expect("Failed to compile protos");
}
