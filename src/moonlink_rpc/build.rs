fn main() {
    println!("cargo:rerun-if-changed=proto");
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("protoc not found");
    std::env::set_var("PROTOC", protoc);
    tonic_prost_build::configure()
        .compile_protos(&["proto/rpc.proto"], &["proto"])
        .expect("error compiling protos");
}
