# Add maturin packaging to ibcore for PyPI publishing. Create python/pyproject.toml with maturin build config: name='ibcore', requires-python='>=3.9', classifiers for financial/quant, module-name='ibcore._ibcore', features=['python']. Also create python/tests/ directory with pytest test files for: test_client.py (IbClient construction, connection mock), test_snapshots.py (StockSnapshot/OptionSnapshot fields), test_diagnostics.py (DiagnosticEvent fields). Do NOT modify existing Rust code or cli/. This is packaging + Python tests only. Follow the existing project patterns.

Let me start by loading the relevant skills and gathering project context in parallel.

Now let me gather all the project context in parallel:

Now let me dig into the existing Python crate and source structure:

Let me check a few more things - specs status on disk, and the rest of the Python lib:

Let me check one more thing — whether there's a python feature gate in the ibcore root crate and how maturin should be configured:

Now I have complete context. Let me produce the design specification.
    
    
    
    Position: Create Python packaging config + pytest suite for existing PyO3 bindings. No Rust changes, no cli/ changes. (High confidence)
    
    Context Summary
    
    - python/ crate (ibcore-python) already has PyO3 bindings for IbClient, StockSnapshot, OptionSnapshot, DiagnosticEvent, and enum-like classes
    - Zero Python test files exist today; all tests live inline in python/src/lib.rs as #[cfg(test)] Rust tests
    - No python feature gate exists on ibcore (confirmed: "PyO3 bindings in separate workspace crate, not feature-gated")
    - maturin is NOT installed on this system (will need pip install maturin)
    - Specs directory was deleted; conventions now live in AGENTS.md
    
    Decision Table
    
    Approach: A: Import from built .so — pytest tests that import
      ibcore._ibcore and validate types
    Isolation: Requires maturin develop first
    Faithfulness to Patterns: Matches README: cd python && pytest tests/
    Simplicity: Medium (build step needed)
    Verdict: Accept
    ────────────────────────────────────────
    Approach: B: Mock-only — stub all PyO3 types, never import real module
    Isolation: Zero dependencies
    Faithfulness to Patterns: Deviates from project patterns
    Simplicity: High (no build)
    Verdict: Reject — tests wouldn't validate actual bindings
    ────────────────────────────────────────
    Approach: C: PyO3 inline only — skip Python tests, rely on Rust
      #[cfg(test)]
    Isolation: None (no Python tests)
    Faithfulness to Patterns: Current state
    Simplicity: High
    Verdict: Reject — user explicitly wants pytest suite
    
    Verdict: Approach A. The README already documents cd python && pytest tests/ as the test command. This makes it real.
    
    Assumption: User will run pip install maturin && cd python && maturin develop --release before pytest tests/. Validate — maturin not yet installed.
    
    Decision: features=['python'] in pyproject.toml
    
    Conflict detected: User requested features=['python'] in maturin config. However, no python Cargo feature exists — the PyO3 bindings are a separate workspace crate (ibcore-python), not feature-gated.
    
    Option: Include features = ["python"] as requested
    Risk: Cargo warns "unused feature" but build succeeds
    Verdict: Accept (forward-looking, user may add feature later)
    ────────────────────────────────────────
    Option: Omit it
    Risk: User expectation unmet
    Verdict: Reject
    
    Verdict: Include features = ["python"] as specified. Cargo ignores unknown features with a warning, build still succeeds.
    
    Impact Assessment
    
    Category: Files created
    Detail: python/pyproject.toml, python/tests/init.py,
      python/tests/test_client.py, python/tests/test_snapshots.py,
      python/tests/test_diagnostics.py
    ────────────────────────────────────────
    Category: Files modified
    Detail: None (explicit requirement)
    ────────────────────────────────────────
    Category: Cascading effects
    Detail: None — pure additive
    ────────────────────────────────────────
    Category: Est. lines
    Detail: ~30 lines pyproject.toml, ~30 lines test_client.py, ~40 lines
      test_snapshots.py, ~50 lines test_diagnostics.py = ~150 lines total
    ────────────────────────────────────────
    Category: Risk
    Detail: LOW — tests only; no production code touched
    
    Confidence + Impact Markers
    
    Confidence: (High)
    Impact: (LOW)
    
    Clear scope, well-understood types, no ambiguity.
    
    API / Interface Proposal
    
    python/pyproject.toml — Maturin Build Config
    
    
    [build-system]
    requires = ["maturin>=1.0"]
    build-backend = "maturin"
    
    [project]
    name = "ibcore"
    version = "0.1.0"
    description = "Standalone IB Gateway integration — market data snapshots, diagnostics, order placement"
    requires-python = ">=3.9"
    readme = "../README.md"
    license = {text = "MIT"}
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Office/Business :: Financial :: Investment",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Rust",
        "Operating System :: OS Independent",
    ]
    
    [tool.maturin]
    module-name = "ibcore._ibcore"
    features = ["python"]
    
    
    python/tests/test_client.py — IbClient Tests
    
    Test functions:
    
    Test: test_ibclient_class_exists
    What it validates: from ibcore._ibcore import IbClient succeeds
    ────────────────────────────────────────
    Test: test_ibclient_has_connect_method
    What it validates: hasattr(IbClient, 'connect') is True
    ────────────────────────────────────────
    Test: test_ibclient_has_methods
    What it validates: All documented methods exist on class: connect,
      reconnect, disconnect, stock_snapshot, option_snapshot, positions,
      account_summary, net_liquidation, place_order, cancel_order,
      diagnostic_events
    ────────────────────────────────────────
    Test: test_ibclient_connect_requires_gateway
    What it validates: IbClient.connect("127.0.0.1", 9999, 1, "delayed",
      "paper") raises IbError (no Gateway on port 9999) — proves error
      handling works
    ────────────────────────────────────────
    Test: test_ibclient_connect_rejects_bad_account_type
    What it validates: IbClient.connect("127.0.0.1", 9999, 1, "delayed",
      "invalid") raises IbError — input validation
    
    Imports needed: ibcore._ibcore.IbClient, ibcore._ibcore.IbError, pytest
    
    python/tests/test_snapshots.py — Snapshot Type Tests
    
    Test functions:
    
    Test: test_stock_snapshot_fields
    What it validates: Construct StockSnapshot, verify last, bid, ask, close
      are accessible as float attributes
    ────────────────────────────────────────
    Test: test_stock_snapshot_defaults
    What it validates: Default-constructed StockSnapshot has all zero fields
    ────────────────────────────────────────
    Test: test_option_snapshot_fields
    What it validates: Construct OptionSnapshot, verify bid, ask, last,
      option_iv, option_delta, option_gamma, option_theta, option_price,
      underlying_price
    ────────────────────────────────────────
    Test: test_option_snapshot_defaults
    What it validates: Default-constructed OptionSnapshot has all zero fields
    ────────────────────────────────────────
    Test: test_option_snapshot_repr
    What it validates: repr(snap) contains "OptionSnapshot" and field values
    
    Imports needed: ibcore._ibcore.StockSnapshot, ibcore._ibcore.OptionSnapshot
    
    python/tests/test_diagnostics.py — DiagnosticEvent + Enum Tests
    
    Test functions:
    
    Test: test_diagnostic_event_fields
    What it validates: Construct DiagnosticEvent, verify all 8 fields:
      gateway_version (int), error_code (int), error_message (str),
      farm_status (str), connection_state (str), account_type (str), os (str),
      timestamp (str)
    ────────────────────────────────────────
    Test: test_farm_state_constants
    What it validates: FarmState.OK == "ok", FarmState.WARNING == "warning",
      FarmState.INACTIVE == "inactive"
    ────────────────────────────────────────
    Test: test_farm_state_from_code
    What it validates: FarmState.from_code(2104) == "ok",
      FarmState.from_code(2107) == "inactive", FarmState.from_code(999) ==
      "unknown(999)"
    ────────────────────────────────────────
    Test: test_connection_state_constants
    What it validates: ConnectionState.CONNECTED, .DISCONNECTED, .RECONNECTING
      are correct strings
    ────────────────────────────────────────
    Test: test_account_type_constants
    What it validates: AccountType.LIVE == "live", AccountType.PAPER ==
      "paper"
    ────────────────────────────────────────
    Test: test_diagnostic_event_repr
    What it validates: repr(event) contains "DiagnosticEvent"
    
    Imports needed: ibcore._ibcore.DiagnosticEvent, ibcore._ibcore.FarmState, ibcore._ibcore.ConnectionState, ibcore._ibcore.AccountType
    
    Security Considerations
    
    - NONE. No credentials, no network access in tests, no data exposure. Test on port 9999 (non-existent Gateway) proves error handling without touching real infrastructure.
    - test_ibclient_connect_requires_gateway intentionally connects to 127.0.0.1:9999 — a port with nothing listening. This is safe.
    
    Documentation Impact
    
    README: YES — needs update. The README already documents Python tests at line 26-27: cd python && pytest tests/. No change needed to the README itself — the test command is already correct. However, a prerequisite note should be added to the "Python Bindings" section (around line 273-278) indicating that pip install maturin is required before building.
    
    Actually, the README already says PYO3_PYTHON=$(which python3) cargo build -p ibcore-python --release — there's no maturin mention. This is a gap we should fill. The coder should add a brief "Build with maturin" subsection.
    
    
    
    Task Breakdown — DAG
    
    Task 1: Create python/pyproject.toml with maturin config
    Files: python/pyproject.toml (create)
    Dependencies: [none]
    Parallelizable: yes
    Description: Create pyproject.toml with [build-system], [project], and [tool.maturin] sections. Name='ibcore', requires-python='>=3.9', classifiers for Financial/Quant, module-name='ibcore._ibcore', features=['python']. Validate syntax with python -c "import tomllib; tomllib.load(open('python/pyproject.toml','rb'))".
    
    Task 2: Create python/tests/init.py
    Files: python/tests/init.py (create, empty)
    Dependencies: [none]
    Parallelizable: yes
    Description: Create empty init.py for pytest package discovery.
    
    Task 3: Create python/tests/test_snapshots.py
    Files: python/tests/test_snapshots.py (create)
    Dependencies: [Task 2]
    Parallelizable: yes
    Description: Write pytest tests for StockSnapshot (4 fields: last, bid, ask, close) and OptionSnapshot (9 fields including Greeks). Test construction with kw args, field access, default values, and repr(). Requires maturin develop before running.
    
    Task 4: Create python/tests/test_diagnostics.py
    Files: python/tests/test_diagnostics.py (create)
    Dependencies: [Task 2]
    Parallelizable: yes
    Description: Write pytest tests for DiagnosticEvent (8 fields), FarmState constants + from_code(), ConnectionState constants, AccountType constants. Test field access and repr().
    
    Task 5: Create python/tests/test_client.py
    Files: python/tests/test_client.py (create)
    Dependencies: [Task 2]
    Parallelizable: yes
    Description: Write pytest tests for IbClient class existence, method presence (connect, reconnect, disconnect, stock_snapshot, option_snapshot, positions, account_summary, net_liquidation, place_order, cancel_order, diagnostic_events, server_version, account_type), and error handling (connect to dead port raises IbError, bad account_type raises IbError).
    
    Task 6: Validate full test suite runs
    Files: none (verification only)
    Dependencies: [Task 1, Task 3, Task 4, Task 5]
    Parallelizable: no
    Description: Install maturin, run maturin develop, then pytest python/tests/ -v. All tests must pass. Confirm test count matches expectations.
    
    
    
    DAG Visualization
    
    
    Task 1 (pyproject.toml) ──┐
    Task 2 (init.py)    ──┼── Task 6 (validate)
    Task 3 (test_snapshots) ──┤
    Task 4 (test_diagnostics)─┤
    Task 5 (test_client)    ──┘
    
    
    Tasks 1-5 are all parallelizable (touch completely different files). Task 6 is the integration gate.