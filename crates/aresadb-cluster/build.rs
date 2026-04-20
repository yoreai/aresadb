fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Same vendored-protoc trick as aresadb-net — keeps the build
    // hermetic without requiring an externally installed toolchain.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["proto/admin.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/admin.proto");
    Ok(())
}
