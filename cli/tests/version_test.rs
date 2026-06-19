use std::process::Command;

#[test]
fn version_output_contains_all_names_and_versions() {
    let output = Command::new(env!("CARGO_BIN_EXE_ibkr-diag"))
        .arg("version")
        .output()
        .expect("failed to run ibkr-diag version");

    assert!(output.status.success(), "version subcommand should exit 0");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("ibkr-diag"),
        "stdout should contain ibkr-diag: {stdout}"
    );
    assert!(
        stdout.contains("ibcore"),
        "stdout should contain ibcore: {stdout}"
    );
    assert!(
        stdout.contains("ibapi"),
        "stdout should contain ibapi: {stdout}"
    );
}
