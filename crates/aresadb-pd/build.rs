//! Compile `pd.proto` with tonic_build.
//!
//! The generated code lives under `OUT_DIR/aresadb.pd.v1.rs` and is
//! re-exported from `src/admin/mod.rs` via `tonic::include_proto!`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Point tonic_build at the vendored protoc so nobody has to
    // install one by hand — matches the convention in
    // `aresadb-net/build.rs` and `aresadb-cluster/build.rs`.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["proto/pd.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/pd.proto");
    Ok(())
}
