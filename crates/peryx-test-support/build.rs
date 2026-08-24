use std::path::PathBuf;
use std::process::Command;

fn main() {
    let source = PathBuf::from("tests/fixtures/process.rs");
    println!("cargo::rerun-if-changed={}", source.display());
    let output = PathBuf::from(std::env::var_os("OUT_DIR").expect("Cargo OUT_DIR"))
        .join(format!("peryx-test-fixture{}", std::env::consts::EXE_SUFFIX));
    let status = Command::new(std::env::var_os("RUSTC").expect("Cargo rustc"))
        .args(["--edition=2024", "--crate-name", "peryx_test_fixture"])
        .arg(source)
        .arg("-o")
        .arg(&output)
        .status()
        .expect("compile process fixture");
    assert!(status.success(), "process fixture compilation failed");
    println!("cargo::rustc-env=PERYX_TEST_FIXTURE={}", output.display());
}
