use anyhow::Result;
use vergen_git2::{Emitter, Git2Builder};

pub fn main() -> Result<()> {
    let git = Git2Builder::default().describe(true, true, None).build()?;
    Emitter::default().add_instructions(&git)?.emit()?;

    grpc_build_proto()?;

    Ok(())
}

fn grpc_build_proto() -> Result<()> {
    // tonic_build::compile_protos("proto/minkv.proto")?;
    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .compile(&["proto/minkv.proto"], &["proto"])?;
    Ok(())
}
