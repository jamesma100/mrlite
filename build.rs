// build script for cargo, will configure tonic build

fn main() -> Result<(), Box<dyn std::error::Error>> {
	tonic_build::compile_protos("proto/tasks.proto")?;
	Ok(())
}
