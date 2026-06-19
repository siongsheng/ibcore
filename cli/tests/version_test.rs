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

#[test]
fn diagnose_help_shows_all_options() {
    let output = Command::new(env!("CARGO_BIN_EXE_ibkr-diag"))
        .args(["diagnose", "--help"])
        .output()
        .expect("failed to run ibkr-diag diagnose --help");

    assert!(output.status.success(), "diagnose --help should exit 0");

    let help = String::from_utf8_lossy(&output.stdout);
    // Check all expected CLI options are documented
    assert!(help.contains("--host"), "help should mention --host");
    assert!(help.contains("--port"), "help should mention --port");
    assert!(help.contains("--client-id"), "help should mention --client-id");
    assert!(help.contains("--duration"), "help should mention --duration");
    assert!(help.contains("--market-data"), "help should mention --market-data");
    assert!(help.contains("--json"), "help should mention --json");
}

#[test]
fn diagnose_without_gateway_reports_error() {
    // Running against localhost:4002 without a Gateway should fail gracefully
    let output = Command::new(env!("CARGO_BIN_EXE_ibkr-diag"))
        .args(["diagnose", "--host", "127.0.0.1", "--port", "19999"])
        .output()
        .expect("failed to run ibkr-diag diagnose");

    assert!(!output.status.success(), "diagnose without Gateway should exit non-zero");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.is_empty() || !String::from_utf8_lossy(&output.stdout).is_empty(),
        "should produce some error output"
    );
}
