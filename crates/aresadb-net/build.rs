//! Compile `raft.proto` with tonic_build.
//!
//! The generated code lives under `OUT_DIR/aresadb.raft.v1.rs` and is
//! re-exported from `src/pb.rs`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Point tonic_build at the vendored protoc so nobody has to
    // install one by hand.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["proto/raft.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/raft.proto");
    Ok(())
}
