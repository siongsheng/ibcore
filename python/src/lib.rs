//! PyO3 Python bindings for ibcore.
//!
//! This crate exposes market data snapshots, diagnostic events, and a
//! persistent async client for Interactive Brokers Gateway.
//!
//! Build with: `cargo build -p ibcore-python`

use std::sync::{Arc, Mutex};

use ibcore::IbClient as RustIbClient;
use pyo3::prelude::*;
use pyo3::exceptions::PyException;
use pyo3::types::PyAnyMethods;

// ── PyO3 type bindings ─────────────────────────────────────────────────────

/// Market data snapshot for a stock or index.
#[pyclass(name = "StockSnapshot")]
#[derive(Clone)]
pub struct PyStockSnapshot {
    #[pyo3(get)]
    last: f64,
    #[pyo3(get)]
    bid: f64,
    #[pyo3(get)]
    ask: f64,
    #[pyo3(get)]
    close: f64,
}

#[pymethods]
impl PyStockSnapshot {
    fn __repr__(&self) -> String {
        format!(
            "StockSnapshot(last={}, bid={}, ask={}, close={})",
            self.last, self.bid, self.ask, self.close
        )
    }
}

/// Market data snapshot for an option (includes Greeks).
#[pyclass(name = "OptionSnapshot")]
#[derive(Clone)]
pub struct PyOptionSnapshot {
    #[pyo3(get)]
    bid: f64,
    #[pyo3(get)]
    ask: f64,
    #[pyo3(get)]
    last: f64,
    #[pyo3(get)]
    option_iv: f64,
    #[pyo3(get)]
    option_delta: f64,
    #[pyo3(get)]
    option_gamma: f64,
    #[pyo3(get)]
    option_theta: f64,
    #[pyo3(get)]
    option_price: f64,
    #[pyo3(get)]
    underlying_price: f64,
}

#[pymethods]
impl PyOptionSnapshot {
    fn __repr__(&self) -> String {
        format!(
            "OptionSnapshot(bid={}, ask={}, last={}, iv={}, delta={}, gamma={}, theta={}, price={}, underlying={})",
            self.bid, self.ask, self.last,
            self.option_iv, self.option_delta, self.option_gamma,
            self.option_theta, self.option_price, self.underlying_price,
        )
    }
}

/// Structured diagnostic event from IB Gateway notice stream.
#[pyclass(name = "DiagnosticEvent")]
#[derive(Clone)]
pub struct PyDiagnosticEvent {
    #[pyo3(get)]
    gateway_version: i32,
    #[pyo3(get)]
    error_code: i32,
    #[pyo3(get)]
    error_message: String,
    #[pyo3(get)]
    farm_status: String,
    #[pyo3(get)]
    connection_state: String,
    #[pyo3(get)]
    account_type: String,
    #[pyo3(get)]
    os: String,
    #[pyo3(get)]
    timestamp: String,
}

#[pymethods]
impl PyDiagnosticEvent {
    fn __repr__(&self) -> String {
        format!(
            "DiagnosticEvent(gv={}, code={}, msg={:?}, farm={}, conn={}, acct={}, os={}, ts={})",
            self.gateway_version,
            self.error_code,
            self.error_message,
            self.farm_status,
            self.connection_state,
            self.account_type,
            self.os,
            self.timestamp,
        )
    }
}

// ── Enum-like classes ──────────────────────────────────────────────────────

/// Farm connection status constants.
#[pyclass(name = "FarmState")]
pub struct PyFarmState;

#[pymethods]
impl PyFarmState {
    #[classattr]
    const OK: &'static str = "ok";
    #[classattr]
    const WARNING: &'static str = "warning";
    #[classattr]
    const INACTIVE: &'static str = "inactive";

    #[staticmethod]
    fn from_code(code: i32) -> String {
        match code {
            2104 | 2106 => "ok".to_string(),
            2105 | 2108 => "warning".to_string(),
            2107 => "inactive".to_string(),
            other => format!("unknown({})", other),
        }
    }

    #[staticmethod]
    fn unknown(code: i32) -> String {
        format!("unknown({})", code)
    }
}

/// Connection state constants.
#[pyclass(name = "ConnectionState")]
pub struct PyConnectionState;

#[pymethods]
impl PyConnectionState {
    #[classattr]
    const CONNECTED: &'static str = "connected";
    #[classattr]
    const DISCONNECTED: &'static str = "disconnected";
    #[classattr]
    const RECONNECTING: &'static str = "reconnecting";
}

/// Account type constants.
#[pyclass(name = "AccountType")]
pub struct PyAccountType;

#[pymethods]
impl PyAccountType {
    #[classattr]
    const LIVE: &'static str = "live";
    #[classattr]
    const PAPER: &'static str = "paper";
}

// ── IbError as Python exception ───────────────────────────────────────────

/// Typed IB error — maps raw error codes to semantic variants.
#[pyclass(name = "IbError", extends = PyException)]
pub struct PyIbError {
    #[pyo3(get)]
    category: String,
    #[pyo3(get)]
    code: Option<i32>,
    #[pyo3(get)]
    message: String,
}

#[pymethods]
impl PyIbError {
    #[new]
    #[pyo3(signature = (category, code, message))]
    fn new(category: String, code: Option<i32>, message: String) -> Self {
        PyIbError { category, code, message }
    }

    fn __str__(&self) -> String {
        if let Some(c) = self.code {
            format!("[{}] {}: {}", self.category, c, self.message)
        } else {
            format!("[{}] {}", self.category, self.message)
        }
    }
}

// ── IbClient — async client ──────────────────────────────────────────────

/// Persistent IB Gateway client with snapshot, account, and order methods.
#[pyclass(name = "IbClient")]
pub struct PyIbClient {
    inner: Arc<Mutex<Option<RustIbClient>>>,
    _account_type: String,
}

#[pymethods]
impl PyIbClient {
    #[staticmethod]
    fn connect(
        py: Python<'_>,
        _host: &str,
        _port: u16,
        _client_id: i32,
        _market_data_type: &str,
        _account_type: &str,
    ) -> PyResult<Py<Self>> {
        todo!()
    }

    fn _disconnect(&self) -> PyResult<()> {
        todo!()
    }

    // ── Snapshot methods ──

    fn stock_snapshot(&self, _symbol: &str) -> PyResult<PyStockSnapshot> {
        todo!()
    }

    #[allow(clippy::too_many_arguments)]
    fn option_snapshot(
        &self,
        _symbol: &str,
        _expiry_year: i32,
        _expiry_month: i32,
        _expiry_day: i32,
        _strike: f64,
        _is_call: bool,
        _exchange: &str,
    ) -> PyResult<PyOptionSnapshot> {
        todo!()
    }

    // ── Account methods ──

    fn positions(&self) -> PyResult<Vec<PyObject>> {
        todo!()
    }

    fn account_summary(&self, _tags: Vec<String>) -> PyResult<Vec<PyObject>> {
        todo!()
    }

    fn net_liquidation(&self, _account_id: String) -> PyResult<f64> {
        todo!()
    }

    // ── Order methods ──

    #[pyo3(signature = (_symbol, _action, _quantity, _order_type, _limit_price=None, _exchange="SMART"))]
    fn place_order(
        &self,
        _symbol: &str,
        _action: &str,
        _quantity: f64,
        _order_type: &str,
        _limit_price: Option<f64>,
        _exchange: &str,
    ) -> PyResult<i32> {
        todo!()
    }

    fn cancel_order(&self, _order_id: i32) -> PyResult<()> {
        todo!()
    }

    // ── Properties ──

    #[getter]
    fn server_version(&self) -> PyResult<i32> {
        todo!()
    }

    #[getter]
    fn account_type(&self) -> String {
        self._account_type.clone()
    }

    // ── Context manager ──

    fn __aenter__(slf: PyRef<'_, Self>) -> PyResult<Py<Self>> {
        todo!()
    }

    fn __aexit__(
        &self,
        _exc_type: PyObject,
        _exc_value: PyObject,
        _traceback: PyObject,
    ) -> PyResult<()> {
        todo!()
    }

    // ── Diagnostics ──

    fn diagnostic_events(&self) -> PyResult<PyDiagnosticEventReceiver> {
        todo!()
    }
}

// ── DiagnosticEventReceiver ──────────────────────────────────────────────

/// Iterator over diagnostic events from the broadcast channel.
#[pyclass(name = "DiagnosticEventReceiver")]
pub struct PyDiagnosticEventReceiver;

#[pymethods]
impl PyDiagnosticEventReceiver {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self) -> PyResult<Option<PyDiagnosticEvent>> {
        todo!()
    }

    fn try_next(&self) -> PyResult<Option<PyDiagnosticEvent>> {
        todo!()
    }

    fn poll(&self, _timeout: f64) -> PyResult<Option<PyDiagnosticEvent>> {
        todo!()
    }
}

// ── Module registration ───────────────────────────────────────────────────

#[pymodule]
fn _ibcore(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyStockSnapshot>()?;
    m.add_class::<PyOptionSnapshot>()?;
    m.add_class::<PyDiagnosticEvent>()?;
    m.add_class::<PyFarmState>()?;
    m.add_class::<PyConnectionState>()?;
    m.add_class::<PyAccountType>()?;
    m.add_class::<PyIbError>()?;
    m.add_class::<PyIbClient>()?;
    m.add_class::<PyDiagnosticEventReceiver>()?;
    Ok(())
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── StockSnapshot tests ──

    #[test]
    fn stock_snapshot_fields() -> PyResult<()> {
        Python::with_gil(|py| {
            let snap = PyStockSnapshot {
                last: 450.25,
                bid: 450.20,
                ask: 450.30,
                close: 448.00,
            };
            let py_snap = Py::new(py, snap)?;
            let bind = py_snap.bind(py);
            assert_eq!(bind.getattr("last")?.extract::<f64>()?, 450.25);
            assert_eq!(bind.getattr("bid")?.extract::<f64>()?, 450.20);
            assert_eq!(bind.getattr("ask")?.extract::<f64>()?, 450.30);
            assert_eq!(bind.getattr("close")?.extract::<f64>()?, 448.00);
            Ok(())
        })
    }

    #[test]
    fn stock_snapshot_repr() -> PyResult<()> {
        Python::with_gil(|py| {
            let snap = PyStockSnapshot {
                last: 1.0,
                bid: 0.99,
                ask: 1.01,
                close: 0.98,
            };
            let py_snap = Py::new(py, snap)?;
            let bind = py_snap.bind(py);
            let repr_str = bind.call_method0("__repr__")?.extract::<String>()?;
            assert!(repr_str.contains("StockSnapshot"));
            assert!(repr_str.contains("last=1"));
            Ok(())
        })
    }

    // ── OptionSnapshot tests ──

    #[test]
    fn option_snapshot_fields() -> PyResult<()> {
        Python::with_gil(|py| {
            let snap = PyOptionSnapshot {
                bid: 2.50,
                ask: 2.60,
                last: 2.55,
                option_iv: 0.25,
                option_delta: 0.45,
                option_gamma: 0.05,
                option_theta: -0.03,
                option_price: 2.55,
                underlying_price: 450.0,
            };
            let py_snap = Py::new(py, snap)?;
            let bind = py_snap.bind(py);
            assert_eq!(bind.getattr("bid")?.extract::<f64>()?, 2.50);
            assert_eq!(bind.getattr("ask")?.extract::<f64>()?, 2.60);
            assert_eq!(bind.getattr("option_iv")?.extract::<f64>()?, 0.25);
            assert_eq!(bind.getattr("option_delta")?.extract::<f64>()?, 0.45);
            assert_eq!(bind.getattr("option_gamma")?.extract::<f64>()?, 0.05);
            assert_eq!(bind.getattr("option_theta")?.extract::<f64>()?, -0.03);
            assert_eq!(bind.getattr("option_price")?.extract::<f64>()?, 2.55);
            assert_eq!(bind.getattr("underlying_price")?.extract::<f64>()?, 450.0);
            Ok(())
        })
    }

    // ── DiagnosticEvent tests ──

    #[test]
    fn diagnostic_event_fields() -> PyResult<()> {
        Python::with_gil(|py| {
            let event = PyDiagnosticEvent {
                gateway_version: 221,
                error_code: 2104,
                error_message: "Market data farm OK".into(),
                farm_status: "ok".into(),
                connection_state: "connected".into(),
                account_type: "paper".into(),
                os: "linux".into(),
                timestamp: "2026-01-15T10:30:00Z".into(),
            };
            let py_event = Py::new(py, event)?;
            let bind = py_event.bind(py);
            assert_eq!(
                bind.getattr("gateway_version")?.extract::<i32>()?,
                221
            );
            assert_eq!(bind.getattr("error_code")?.extract::<i32>()?, 2104);
            assert_eq!(
                bind.getattr("error_message")?.extract::<String>()?,
                "Market data farm OK"
            );
            assert_eq!(bind.getattr("farm_status")?.extract::<String>()?, "ok");
            assert_eq!(
                bind.getattr("connection_state")?.extract::<String>()?,
                "connected"
            );
            assert_eq!(
                bind.getattr("account_type")?.extract::<String>()?,
                "paper"
            );
            assert_eq!(bind.getattr("os")?.extract::<String>()?, "linux");
            assert_eq!(
                bind.getattr("timestamp")?.extract::<String>()?,
                "2026-01-15T10:30:00Z"
            );
            Ok(())
        })
    }

    // ── FarmState tests ──

    #[test]
    fn farm_state_constants() -> PyResult<()> {
        Python::with_gil(|py| {
            let state = Py::new(py, PyFarmState)?;
            let bind = state.bind(py);
            assert_eq!(bind.getattr("OK")?.extract::<String>()?, "ok");
            assert_eq!(bind.getattr("WARNING")?.extract::<String>()?, "warning");
            assert_eq!(bind.getattr("INACTIVE")?.extract::<String>()?, "inactive");
            Ok(())
        })
    }

    #[test]
    fn farm_state_from_code() -> PyResult<()> {
        Python::with_gil(|py| {
            let state = Py::new(py, PyFarmState)?;
            let bind = state.bind(py);
            assert_eq!(
                bind.call_method1("from_code", (2104,))?
                    .extract::<String>()?,
                "ok"
            );
            assert_eq!(
                bind.call_method1("from_code", (2106,))?
                    .extract::<String>()?,
                "ok"
            );
            assert_eq!(
                bind.call_method1("from_code", (2105,))?
                    .extract::<String>()?,
                "warning"
            );
            assert_eq!(
                bind.call_method1("from_code", (2108,))?
                    .extract::<String>()?,
                "warning"
            );
            assert_eq!(
                bind.call_method1("from_code", (2107,))?
                    .extract::<String>()?,
                "inactive"
            );
            assert_eq!(
                bind.call_method1("from_code", (999,))?
                    .extract::<String>()?,
                "unknown(999)"
            );
            Ok(())
        })
    }

    #[test]
    fn farm_state_unknown() -> PyResult<()> {
        Python::with_gil(|py| {
            let state = Py::new(py, PyFarmState)?;
            let bind = state.bind(py);
            assert_eq!(
                bind.call_method1("unknown", (42,))?
                    .extract::<String>()?,
                "unknown(42)"
            );
            Ok(())
        })
    }

    // ── ConnectionState tests ──

    #[test]
    fn connection_state_constants() -> PyResult<()> {
        Python::with_gil(|py| {
            let cs = Py::new(py, PyConnectionState)?;
            let bind = cs.bind(py);
            assert_eq!(
                bind.getattr("CONNECTED")?.extract::<String>()?,
                "connected"
            );
            assert_eq!(
                bind.getattr("DISCONNECTED")?.extract::<String>()?,
                "disconnected"
            );
            assert_eq!(
                bind.getattr("RECONNECTING")?.extract::<String>()?,
                "reconnecting"
            );
            Ok(())
        })
    }

    // ── AccountType tests ──

    #[test]
    fn account_type_constants() -> PyResult<()> {
        Python::with_gil(|py| {
            let at = Py::new(py, PyAccountType)?;
            let bind = at.bind(py);
            assert_eq!(bind.getattr("LIVE")?.extract::<String>()?, "live");
            assert_eq!(bind.getattr("PAPER")?.extract::<String>()?, "paper");
            Ok(())
        })
    }

    // ── IbError tests ──

    #[test]
    fn ib_error_construct_and_str() -> PyResult<()> {
        Python::with_gil(|py| {
            let cls = py.get_type::<PyIbError>();
            let err = cls.call1((
                "connection_failed",
                Some(502_i32),
                "Not connected".to_string(),
            ))?;
            let err_str = err.call_method0("__str__")?.extract::<String>()?;
            assert!(err_str.contains("connection_failed"));
            assert!(err_str.contains("502"));
            assert!(err_str.contains("Not connected"));
            Ok(())
        })
    }

    #[test]
    fn ib_error_without_code() -> PyResult<()> {
        Python::with_gil(|py| {
            let cls = py.get_type::<PyIbError>();
            let err = cls.call1(("other", py.None(), "Something weird".to_string()))?;
            let err_str = err.call_method0("__str__")?.extract::<String>()?;
            assert!(err_str.contains("other"));
            assert!(err_str.contains("Something weird"));
            Ok(())
        })
    }

    // ── Module registration tests ──

    #[test]
    fn module_registers_all_classes() -> PyResult<()> {
        Python::with_gil(|py| {
            let m = PyModule::new(py, "_ibcore")?;
            _ibcore(py, &m)?;
            assert!(m.getattr("StockSnapshot").is_ok());
            assert!(m.getattr("OptionSnapshot").is_ok());
            assert!(m.getattr("DiagnosticEvent").is_ok());
            assert!(m.getattr("FarmState").is_ok());
            assert!(m.getattr("ConnectionState").is_ok());
            assert!(m.getattr("AccountType").is_ok());
            assert!(m.getattr("IbError").is_ok());
            assert!(m.getattr("IbClient").is_ok());
            assert!(m.getattr("DiagnosticEventReceiver").is_ok());
            Ok(())
        })
    }
}
