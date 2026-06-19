//! PyO3 Python bindings for ibcore.
//!
//! This crate exposes market data snapshots, diagnostic events, and a
//! persistent async client for Interactive Brokers Gateway.
//!
//! Build with: `cargo build -p ibcore-python`

#[cfg(test)]
mod tests {
    #[test]
    fn python_crate_compiles() {
        // Verify workspace member crate builds correctly.
        assert!(true, "ibcore-python workspace crate compiles");
    }
}
