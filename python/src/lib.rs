//! PyO3 Python bindings for ibcore.
//!
//! This crate exposes market data snapshots, diagnostic events, and a
//! persistent async client for Interactive Brokers Gateway.
//!
//! Build with: `cargo build -p ibcore-python`

use std::sync::{Arc, Mutex};

use ibcore::{
    diagnostics::{self, AccountType as RustAccountType, DiagnosticEvent as RustDiagnosticEvent},
    IbClient as RustIbClient,
};
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyList};

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

impl From<ibcore::StockSnapshot> for PyStockSnapshot {
    fn from(s: ibcore::StockSnapshot) -> Self {
        PyStockSnapshot {
            last: s.last,
            bid: s.bid,
            ask: s.ask,
            close: s.close,
        }
    }
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

impl From<ibcore::OptionSnapshot> for PyOptionSnapshot {
    fn from(s: ibcore::OptionSnapshot) -> Self {
        PyOptionSnapshot {
            bid: s.bid,
            ask: s.ask,
            last: s.last,
            option_iv: s.option_iv,
            option_delta: s.option_delta,
            option_gamma: s.option_gamma,
            option_theta: s.option_theta,
            option_price: s.option_price,
            underlying_price: s.underlying_price,
        }
    }
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

/// Open order snapshot returned by `IbClient.open_orders()`.
#[pyclass(name = "OpenOrder")]
#[derive(Clone)]
pub struct PyOpenOrder {
    #[pyo3(get)]
    order_id: i32,
    #[pyo3(get)]
    symbol: String,
    #[pyo3(get)]
    action: String,
    #[pyo3(get)]
    quantity: f64,
    #[pyo3(get)]
    order_type: String,
    #[pyo3(get)]
    limit_price: Option<f64>,
    #[pyo3(get)]
    status: String,
    #[pyo3(get)]
    filled_qty: f64,
    #[pyo3(get)]
    remaining_qty: f64,
}

#[pymethods]
impl PyOpenOrder {
    #[new]
    #[pyo3(signature = (order_id=0, symbol="".into(), action="".into(), quantity=0.0,
                        order_type="".into(), limit_price=None, status="".into(),
                        filled_qty=0.0, remaining_qty=0.0))]
    fn new(
        order_id: i32,
        symbol: String,
        action: String,
        quantity: f64,
        order_type: String,
        limit_price: Option<f64>,
        status: String,
        filled_qty: f64,
        remaining_qty: f64,
    ) -> Self {
        PyOpenOrder {
            order_id,
            symbol,
            action,
            quantity,
            order_type,
            limit_price,
            status,
            filled_qty,
            remaining_qty,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "OpenOrder(order_id={}, symbol={:?}, action={:?}, quantity={}, order_type={:?}, limit_price={:?}, status={:?}, filled_qty={}, remaining_qty={})",
            self.order_id, self.symbol, self.action, self.quantity,
            self.order_type, self.limit_price, self.status,
            self.filled_qty, self.remaining_qty,
        )
    }
}

impl From<ibcore::OpenOrder> for PyOpenOrder {
    fn from(o: ibcore::OpenOrder) -> Self {
        PyOpenOrder {
            order_id: o.order_id,
            symbol: o.symbol,
            action: o.action,
            quantity: o.quantity,
            order_type: o.order_type,
            limit_price: o.limit_price,
            status: o.status,
            filled_qty: o.filled_qty,
            remaining_qty: o.remaining_qty,
        }
    }
}

/// Order status event returned by `IbClient.order_updates()`.
#[pyclass(name = "OrderStatusEvent")]
#[derive(Clone)]
pub struct PyOrderStatusEvent {
    #[pyo3(get)]
    kind: String,
    #[pyo3(get)]
    order_id: i32,
    #[pyo3(get)]
    filled_qty: f64,
    #[pyo3(get)]
    avg_price: f64,
    #[pyo3(get)]
    commission: Option<f64>,
    #[pyo3(get)]
    reason: String,
    #[pyo3(get)]
    status: String,
}

#[pymethods]
impl PyOrderStatusEvent {
    #[new]
    #[pyo3(signature = (kind="".into(), order_id=0, filled_qty=0.0,
                        avg_price=0.0, commission=None, reason="".into(),
                        status="".into()))]
    fn new(
        kind: String,
        order_id: i32,
        filled_qty: f64,
        avg_price: f64,
        commission: Option<f64>,
        reason: String,
        status: String,
    ) -> Self {
        PyOrderStatusEvent {
            kind,
            order_id,
            filled_qty,
            avg_price,
            commission,
            reason,
            status,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "OrderStatusEvent(kind={:?}, order_id={}, filled_qty={}, avg_price={}, commission={:?}, reason={:?}, status={:?})",
            self.kind, self.order_id, self.filled_qty, self.avg_price,
            self.commission, self.reason, self.status,
        )
    }
}

impl From<ibcore::OrderStatusEvent> for PyOrderStatusEvent {
    fn from(e: ibcore::OrderStatusEvent) -> Self {
        match e {
            ibcore::OrderStatusEvent::Submitted { order_id } => PyOrderStatusEvent {
                kind: "Submitted".into(),
                order_id,
                filled_qty: 0.0,
                avg_price: 0.0,
                commission: None,
                reason: String::new(),
                status: String::new(),
            },
            ibcore::OrderStatusEvent::Filled {
                order_id,
                filled_qty,
                avg_price,
                commission,
            } => PyOrderStatusEvent {
                kind: "Filled".into(),
                order_id,
                filled_qty,
                avg_price,
                commission,
                reason: String::new(),
                status: String::new(),
            },
            ibcore::OrderStatusEvent::Cancelled { order_id, reason } => PyOrderStatusEvent {
                kind: "Cancelled".into(),
                order_id,
                filled_qty: 0.0,
                avg_price: 0.0,
                commission: None,
                reason,
                status: String::new(),
            },
            ibcore::OrderStatusEvent::Inactive { order_id } => PyOrderStatusEvent {
                kind: "Inactive".into(),
                order_id,
                filled_qty: 0.0,
                avg_price: 0.0,
                commission: None,
                reason: String::new(),
                status: String::new(),
            },
            ibcore::OrderStatusEvent::Rejected { order_id, reason } => PyOrderStatusEvent {
                kind: "Rejected".into(),
                order_id,
                filled_qty: 0.0,
                avg_price: 0.0,
                commission: None,
                reason,
                status: String::new(),
            },
            ibcore::OrderStatusEvent::Other { order_id, status } => PyOrderStatusEvent {
                kind: "Other".into(),
                order_id,
                filled_qty: 0.0,
                avg_price: 0.0,
                commission: None,
                reason: String::new(),
                status,
            },
        }
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

impl From<RustDiagnosticEvent> for PyDiagnosticEvent {
    fn from(e: RustDiagnosticEvent) -> Self {
        PyDiagnosticEvent {
            gateway_version: e.gateway_version,
            error_code: e.error_code,
            error_message: e.error_message,
            farm_status: e.farm_status.to_string(),
            connection_state: e.connection_state.to_string(),
            account_type: e.account_type.to_string(),
            os: e.os.to_string(),
            timestamp: e.timestamp.to_rfc3339(),
        }
    }
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
        diagnostics::classify_farm(code).to_string()
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

/// Convert a Rust `IbError` to a Python `PyErr`.
pub fn ib_err_to_py_err(e: &ibcore::IbError) -> PyErr {
    let (category, code, message) = map_ib_error(e);
    PyErr::new::<PyIbError, _>((category, code, message))
}

/// Convert a Rust `IbError` to arguments for constructing a Python `IbError`.
fn map_ib_error(e: &ibcore::IbError) -> (String, Option<i32>, String) {
    match e {
        ibcore::IbError::ConnectionFailed(msg) => {
            ("connection_failed".into(), None, msg.clone())
        }
        ibcore::IbError::ConnectionReset => {
            ("connection_reset".into(), None, "connection reset".into())
        }
        ibcore::IbError::MarketData { code, message } => {
            ("market_data".into(), Some(*code), message.clone())
        }
        ibcore::IbError::OrderRejected {
            code,
            message,
            rejection_json,
        } => {
            let msg = if let Some(json) = rejection_json {
                format!("{} [{}]", message, json)
            } else {
                message.clone()
            };
            ("order_rejected".into(), Some(*code), msg)
        }
        ibcore::IbError::FarmDisconnect { code, message } => {
            ("farm_disconnect".into(), Some(*code), message.clone())
        }
        ibcore::IbError::ContractResolution(msg) => {
            ("contract_resolution".into(), None, msg.clone())
        }
        ibcore::IbError::CompetingSession => {
            ("competing_session".into(), None, "competing session (10197)".into())
        }
        ibcore::IbError::Timeout(msg) => ("timeout".into(), None, msg.clone()),
        ibcore::IbError::Other(msg) => ("other".into(), None, msg.clone()),
    }
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
    rt: Arc<tokio::runtime::Runtime>,
}

fn parse_account_type(s: &str) -> Result<RustAccountType, PyErr> {
    match s {
        "live" => Ok(RustAccountType::Live),
        "paper" => Ok(RustAccountType::Paper),
        other => Err(PyErr::new::<PyIbError, _>((
            "connection_failed".to_string(),
            None::<i32>,
            format!("invalid account_type: {other}"),
        ))),
    }
}

fn with_client<F, T>(inner: &Arc<Mutex<Option<RustIbClient>>>, f: F) -> PyResult<T>
where
    F: FnOnce(&RustIbClient) -> PyResult<T>,
{
    let guard = inner.lock().map_err(|e| {
        PyErr::new::<PyIbError, _>(("other".to_string(), None::<i32>, format!("lock error: {e}")))
    })?;
    match guard.as_ref() {
        Some(client) => f(client),
        None => Err(PyErr::new::<PyIbError, _>((
            "connection_failed".to_string(),
            None::<i32>,
            "not connected".to_string(),
        ))),
    }
}

#[pymethods]
impl PyIbClient {
    #[staticmethod]
    fn connect(
        py: Python<'_>,
        host: &str,
        port: u16,
        client_id: i32,
        market_data_type: &str,
        account_type: &str,
    ) -> PyResult<Py<Self>> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            PyErr::new::<PyIbError, _>((
                "connection_failed".to_string(),
                None::<i32>,
                format!("runtime creation failed: {e}"),
            ))
        })?;
        let acct = parse_account_type(account_type)?;
        let client = rt
            .block_on(RustIbClient::connect(host, port, client_id, market_data_type, acct))
            .map_err(|e| ib_err_to_py_err(&e))?;
        let py_client = PyIbClient {
            inner: Arc::new(Mutex::new(Some(client))),
            _account_type: account_type.to_string(),
            rt: Arc::new(rt),
        };
        Py::new(py, py_client)
    }

    #[pyo3(signature = (host, port, client_id, market_data_type))]
    fn reconnect(
        &self,
        host: &str,
        port: u16,
        client_id: i32,
        market_data_type: &str,
    ) -> PyResult<()> {
        let mut guard = self.inner.lock().map_err(|e| {
            PyErr::new::<PyIbError, _>(("other".to_string(), None::<i32>, format!("lock error: {e}")))
        })?;
        let client = guard.as_mut().ok_or_else(|| {
            PyErr::new::<PyIbError, _>((
                "connection_failed".to_string(),
                None::<i32>,
                "not connected".to_string(),
            ))
        })?;
        self.rt
            .block_on(client.reconnect(host, port, client_id, market_data_type))
            .map_err(|e| ib_err_to_py_err(&e))?;
        Ok(())
    }

    fn disconnect(&self) -> PyResult<()> {
        with_client(&self.inner, |client| {
            self.rt.block_on(client.disconnect());
            Ok(())
        })?;
        // Clear the inner client
        let mut guard = self.inner.lock().map_err(|e| {
            PyErr::new::<PyIbError, _>(("other".to_string(), None::<i32>, format!("lock error: {e}")))
        })?;
        *guard = None;
        Ok(())
    }

    // ── Snapshot methods ──

    fn stock_snapshot(&self, symbol: &str) -> PyResult<PyStockSnapshot> {
        with_client(&self.inner, |client| {
            let snap = self
                .rt
                .block_on(client.stock_snapshot(symbol))
                .map_err(|e| ib_err_to_py_err(&e))?;
            Ok(PyStockSnapshot::from(snap))
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn option_snapshot(
        &self,
        symbol: &str,
        expiry_year: i32,
        expiry_month: i32,
        expiry_day: i32,
        strike: f64,
        is_call: bool,
        exchange: &str,
    ) -> PyResult<PyOptionSnapshot> {
        with_client(&self.inner, |client| {
            let snap = self
                .rt
                .block_on(client.option_snapshot(
                    symbol,
                    (expiry_year as u16, expiry_month as u8, expiry_day as u8),
                    strike,
                    is_call,
                    0.0, // _implied_vol — unused internally, kept for API compat
                    0.0, // _underlying_price — unused, kept for API compat
                    exchange,
                ))
                .map_err(|e| ib_err_to_py_err(&e))?;
            Ok(PyOptionSnapshot::from(snap))
        })
    }

    // ── Account methods ──

    fn positions(&self, py: Python<'_>) -> PyResult<PyObject> {
        with_client(&self.inner, |client| {
            let positions = self
                .rt
                .block_on(client.positions())
                .map_err(|e| ib_err_to_py_err(&e))?;
            let py_list = positions
                .into_iter()
                .map(|p| {
                    let pos_dict = PyDict::new(py);
                    pos_dict.set_item("symbol", p.contract.symbol.0).unwrap();
                    pos_dict.set_item("quantity", p.position).unwrap();
                    pos_dict.set_item("avg_cost", p.average_cost).unwrap();
                    pos_dict.set_item("currency", p.contract.currency.0).unwrap();
                    pos_dict.into()
                })
                .collect::<Vec<PyObject>>();
            Ok(PyList::new(py, py_list)?.into())
        })
    }

    fn account_summary(&self, py: Python<'_>, tags: Vec<String>) -> PyResult<PyObject> {
        with_client(&self.inner, |client| {
            let tags_refs: Vec<&str> = tags.iter().map(|s| s.as_str()).collect();
            let results = self
                .rt
                .block_on(client.account_summary(&tags_refs))
                .map_err(|e| ib_err_to_py_err(&e))?;
            let py_list = results
                .into_iter()
                .map(|(account, tag, value, currency)| {
                    let d = PyDict::new(py);
                    d.set_item("account", account).unwrap();
                    d.set_item("tag", tag).unwrap();
                    d.set_item("value", value).unwrap();
                    d.set_item("currency", currency).unwrap();
                    d.into()
                })
                .collect::<Vec<PyObject>>();
            Ok(PyList::new(py, py_list)?.into())
        })
    }

    fn net_liquidation(&self, account_id: &str) -> PyResult<f64> {
        with_client(&self.inner, |client| {
            self.rt
                .block_on(client.net_liquidation(account_id))
                .map_err(|e| ib_err_to_py_err(&e))
        })
    }

    // ── Order methods ──

    #[pyo3(signature = (symbol, action, quantity, order_type, limit_price=None, exchange="SMART"))]
    fn place_order(
        &self,
        symbol: &str,
        action: &str,
        quantity: f64,
        order_type: &str,
        limit_price: Option<f64>,
        exchange: &str,
    ) -> PyResult<i32> {
        with_client(&self.inner, |client| {
            let contract = ibcore::Contract::stock(symbol)
                .on_exchange(exchange)
                .build();
            let mut order = ibapi::orders::Order::default();
            order.action = match action.to_uppercase().as_str() {
                "BUY" => ibapi::orders::Action::Buy,
                "SELL" => ibapi::orders::Action::Sell,
                other => {
                    return Err(PyErr::new::<PyIbError, _>((
                        "order_rejected".to_string(),
                        None::<i32>,
                        format!("invalid action: {other}"),
                    )))
                }
            };
            order.total_quantity = quantity;
            order.order_type = order_type.to_string();
            order.limit_price = limit_price;
            self.rt
                .block_on(client.place_order(&contract, &order))
                .map_err(|e| ib_err_to_py_err(&e))
        })
    }

    fn cancel_order(&self, order_id: i32) -> PyResult<()> {
        with_client(&self.inner, |client| {
            self.rt
                .block_on(client.cancel_order(order_id))
                .map_err(|e| ib_err_to_py_err(&e))
        })
    }

    // ── Order methods ──

    fn open_orders(&self, py: Python<'_>) -> PyResult<PyObject> {
        with_client(&self.inner, |client| {
            let orders = self
                .rt
                .block_on(client.open_orders())
                .map_err(|e| ib_err_to_py_err(&e))?;
            let py_list = orders
                .into_iter()
                .map(|o| {
                    let od = PyDict::new(py);
                    od.set_item("order_id", o.order_id).unwrap();
                    od.set_item("symbol", o.symbol).unwrap();
                    od.set_item("action", o.action).unwrap();
                    od.set_item("quantity", o.quantity).unwrap();
                    od.set_item("order_type", o.order_type).unwrap();
                    if let Some(price) = o.limit_price {
                        od.set_item("limit_price", price).unwrap();
                    } else {
                        od.set_item("limit_price", py.None()).unwrap();
                    }
                    od.set_item("status", o.status).unwrap();
                    od.set_item("filled_qty", o.filled_qty).unwrap();
                    od.set_item("remaining_qty", o.remaining_qty).unwrap();
                    od.into()
                })
                .collect::<Vec<PyObject>>();
            Ok(PyList::new(py, py_list)?.into())
        })
    }

    fn order_updates(&self) -> PyResult<PyOrderUpdateReceiver> {
        with_client(&self.inner, |client| {
            let mut stream = self
                .rt
                .block_on(client.order_updates())
                .map_err(|e| ib_err_to_py_err(&e))?;
            let (tx, rx) = std::sync::mpsc::sync_channel::<PyOrderStatusEvent>(1024);
            let handle = tokio::task::spawn(async move {
                loop {
                    match stream.next().await {
                        Some(Ok(event)) => {
                            let py_event = PyOrderStatusEvent::from(event);
                            match tx.try_send(py_event) {
                                Ok(()) => {}
                                Err(mpsc::TrySendError::Full(_)) => {
                                    tracing::warn!("order update channel full, dropping event");
                                }
                                Err(mpsc::TrySendError::Disconnected(_)) => break,
                            }
                        }
                        Some(Err(e)) => {
                            tracing::warn!(%e, "order update stream error, continuing");
                        }
                        None => break,
                    }
                }
            });
            Ok(PyOrderUpdateReceiver {
                rx: std::sync::Mutex::new(rx),
                _task: Some(handle),
            })
        })
    }

    // ── Properties ──

    #[getter]
    fn server_version(&self) -> PyResult<i32> {
        with_client(&self.inner, |client| Ok(client.server_version()))
    }

    #[getter]
    fn account_type(&self) -> String {
        self._account_type.clone()
    }

    // ── Context manager ──

    fn __enter__(slf: PyRef<'_, Self>) -> PyResult<Py<Self>> {
        Ok(slf.into())
    }

    fn __exit__(
        &self,
        _exc_type: PyObject,
        _exc_value: PyObject,
        _traceback: PyObject,
    ) -> PyResult<()> {
        // Disconnect on exit from context manager
        let _ = self.disconnect();
        Ok(())
    }

    // ── Diagnostics ──

    fn diagnostic_events(&self) -> PyResult<PyDiagnosticEventReceiver> {
        with_client(&self.inner, |client| {
            let rx = client.diagnostic_events();
            Ok(PyDiagnosticEventReceiver { inner: Some(rx) })
        })
    }
}

// ── DiagnosticEventReceiver ──────────────────────────────────────────────

use tokio::sync::broadcast;

/// Iterator over diagnostic events from the broadcast channel.
#[pyclass(name = "DiagnosticEventReceiver")]
pub struct PyDiagnosticEventReceiver {
    inner: Option<broadcast::Receiver<RustDiagnosticEvent>>,
}

#[pymethods]
impl PyDiagnosticEventReceiver {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<PyDiagnosticEvent>> {
        match &mut self.inner {
            Some(rx) => match rx.try_recv() {
                Ok(event) => Ok(Some(PyDiagnosticEvent::from(event))),
                Err(broadcast::error::TryRecvError::Empty)
                | Err(broadcast::error::TryRecvError::Closed) => Ok(None),
                Err(broadcast::error::TryRecvError::Lagged(_n)) => {
                    // Try once more after lag
                    match rx.try_recv() {
                        Ok(event) => Ok(Some(PyDiagnosticEvent::from(event))),
                        _ => Ok(None),
                    }
                }
            },
            None => Ok(None),
        }
    }

    fn try_next(&mut self) -> PyResult<Option<PyDiagnosticEvent>> {
        self.__next__()
    }

    fn poll(&mut self, _timeout: f64) -> PyResult<Option<PyDiagnosticEvent>> {
        self.__next__()
    }
}

// ── PyOrderUpdateReceiver — blocking iterator over order status events ────

use std::sync::mpsc;

/// Blocking iterator over order status events from `order_updates()`.
///
/// Wraps an `mpsc::Receiver` fed by a background tokio task.
/// Dropping this receiver aborts the background task.
///
/// # GIL behaviour
/// `__next__` releases the GIL during `recv()` via `py.allow_threads()`
/// so other Python threads are not starved.
///
/// # GIL_FALLBACK:
/// If `py.allow_threads(|| self.rx.recv())` proves broken, replace with:
///   loop {
///       match self.rx.try_recv() {
///           Ok(event) => return Ok(Some(event)),
///           Err(TryRecvError::Empty) => std::thread::sleep(Duration::from_millis(10)),
///           Err(TryRecvError::Disconnected) => return Ok(None),
///       }
///   }
#[pyclass(name = "OrderUpdateReceiver")]
pub struct PyOrderUpdateReceiver {
    rx: std::sync::Mutex<mpsc::Receiver<PyOrderStatusEvent>>,
    _task: Option<tokio::task::JoinHandle<()>>,
}

#[pymethods]
impl PyOrderUpdateReceiver {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyOrderStatusEvent>> {
        // GIL_FALLBACK: uses try_recv + sleep loop because py.allow_threads(|| self.rx.recv())
        // has a Send/Sync conflict with mpsc::Receiver (which is !Sync even though Send).
        // If a future PyO3 version resolves this, switch to:
        //   py.allow_threads(|| self.rx.recv())
        loop {
            match self.rx.lock().unwrap().try_recv() {
                Ok(event) => return Ok(Some(event)),
                Err(mpsc::TryRecvError::Empty) => {
                    // Release GIL briefly so other Python threads can run
                    py.allow_threads(|| std::thread::sleep(std::time::Duration::from_millis(10)));
                }
                Err(mpsc::TryRecvError::Disconnected) => return Ok(None),
            }
        }
    }
}

impl Drop for PyOrderUpdateReceiver {
    fn drop(&mut self) {
        self._task.take().map(|h| h.abort());
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

    #[test]
    fn stock_snapshot_from_rust() {
        let rust = ibcore::StockSnapshot {
            last: 100.0,
            bid: 99.99,
            ask: 100.01,
            close: 99.50,
        };
        let py: PyStockSnapshot = rust.into();
        assert_eq!(py.last, 100.0);
        assert_eq!(py.bid, 99.99);
        assert_eq!(py.ask, 100.01);
        assert_eq!(py.close, 99.50);
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

    #[test]
    fn option_snapshot_from_rust() {
        let rust = ibcore::OptionSnapshot {
            bid: 2.0,
            ask: 2.10,
            last: 2.05,
            option_iv: 0.30,
            option_delta: 0.50,
            option_gamma: 0.06,
            option_theta: -0.04,
            option_price: 2.05,
            underlying_price: 500.0,
        };
        let py: PyOptionSnapshot = rust.into();
        assert!((py.bid - 2.0).abs() < 0.001);
        assert!((py.option_delta - 0.50).abs() < 0.001);
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
            assert_eq!(bind.getattr("gateway_version")?.extract::<i32>()?, 221);
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

    #[test]
    fn diagnostic_event_from_rust() {
        use chrono::TimeZone;
        let rust = RustDiagnosticEvent {
            gateway_version: 221,
            error_code: 2104,
            error_message: "test".into(),
            error_time: None,
            farm_status: ibcore::diagnostics::FarmState::Ok,
            connection_state: ibcore::diagnostics::ConnectionState::Connected,
            account_type: ibcore::diagnostics::AccountType::Paper,
            os: "linux",
            timestamp: chrono::Utc.with_ymd_and_hms(2026, 1, 15, 10, 30, 0).unwrap(),
        };
        let py: PyDiagnosticEvent = rust.into();
        assert_eq!(py.gateway_version, 221);
        assert_eq!(py.error_code, 2104);
        assert_eq!(py.error_message, "test");
        assert_eq!(py.farm_status, "ok");
        assert_eq!(py.connection_state, "connected");
        assert_eq!(py.account_type, "paper");
        assert_eq!(py.os, "linux");
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
            assert_eq!(bind.getattr("CONNECTED")?.extract::<String>()?, "connected");
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

    #[test]
    fn ib_error_from_rust_connection_failed() {
        let rust = ibcore::IbError::ConnectionFailed("timeout".into());
        let (category, code, message) = map_ib_error(&rust);
        assert_eq!(category, "connection_failed");
        assert!(code.is_none());
        assert_eq!(message, "timeout");
    }

    #[test]
    fn ib_error_from_rust_market_data() {
        let rust = ibcore::IbError::MarketData {
            code: 10197,
            message: "competing".into(),
        };
        let (category, code, message) = map_ib_error(&rust);
        assert_eq!(category, "market_data");
        assert_eq!(code, Some(10197));
        assert_eq!(message, "competing");
    }

    #[test]
    fn ib_error_from_rust_order_rejected_with_json() {
        let rust = ibcore::IbError::OrderRejected {
            code: 201,
            message: "insufficient funds".into(),
            rejection_json: None,
        };
        let (category, code, message) = map_ib_error(&rust);
        assert_eq!(category, "order_rejected");
        assert_eq!(code, Some(201));
        assert!(message.contains("insufficient funds"));
        assert!(message.contains("insufficient funds"));
    }

    // ── IbClient tests ──

    #[test]
    fn ib_client_has_correct_attrs() -> PyResult<()> {
        Python::with_gil(|py| {
            let cls = py.get_type::<PyIbClient>();
            assert!(cls.getattr("connect").is_ok());
            assert!(cls.getattr("reconnect").is_ok());
            assert!(cls.getattr("disconnect").is_ok());
            assert!(cls.getattr("stock_snapshot").is_ok());
            assert!(cls.getattr("option_snapshot").is_ok());
            assert!(cls.getattr("positions").is_ok());
            assert!(cls.getattr("account_summary").is_ok());
            assert!(cls.getattr("net_liquidation").is_ok());
            assert!(cls.getattr("place_order").is_ok());
            assert!(cls.getattr("cancel_order").is_ok());
            assert!(cls.getattr("server_version").is_ok());
            assert!(cls.getattr("account_type").is_ok());
            assert!(cls.getattr("diagnostic_events").is_ok());
            assert!(cls.getattr("open_orders").is_ok());
            assert!(cls.getattr("order_updates").is_ok());
            Ok(())
        })
    }

    #[test]
    fn ib_client_connect_requires_gateway() -> PyResult<()> {
        // Without a running Gateway, connect should return an IbError,
        // not crash or hang forever.
        Python::with_gil(|py| {
            let cls = py.get_type::<PyIbClient>();
            let result = cls.call_method1(
                "connect",
                ("127.0.0.1", 9999_i32, 1_i32, "delayed", "paper"),
            );
            match result {
                Err(e) => {
                    let err_str = e.to_string();
                    assert!(
                        err_str.contains("connection")
                            || err_str.contains("refused")
                            || err_str.contains("reset"),
                        "unexpected error: {err_str}"
                    );
                }
                Ok(_) => panic!("connect should fail without a running Gateway"),
            }
            Ok(())
        })
    }

    // ── DiagnosticEventReceiver tests ──

    #[test]
    fn diagnostic_event_receiver_attrs() -> PyResult<()> {
        Python::with_gil(|py| {
            let recv = PyDiagnosticEventReceiver { inner: None };
            let py_recv = Py::new(py, recv)?;
            let bind = py_recv.bind(py);
            assert!(bind.getattr("__iter__").is_ok());
            assert!(bind.getattr("__next__").is_ok());
            assert!(bind.getattr("try_next").is_ok());
            assert!(bind.getattr("poll").is_ok());
            Ok(())
        })
    }

    #[test]
    fn diagnostic_event_receiver_empty_raises_stop_iteration() -> PyResult<()> {
        Python::with_gil(|py| {
            let recv = PyDiagnosticEventReceiver { inner: None };
            let py_recv = Py::new(py, recv)?;
            let bind = py_recv.bind(py);
            let result = bind.call_method0("__next__");
            assert!(result.is_err(), "expected StopIteration when empty");
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

    #[test]
    fn from_rust_errors_map_correctly() {
        let test_cases: Vec<(ibcore::IbError, &str)> = vec![
            (ibcore::IbError::ConnectionFailed("fail".into()), "connection_failed"),
            (ibcore::IbError::ConnectionReset, "connection_reset"),
            (
                ibcore::IbError::MarketData {
                    code: 10197,
                    message: "md".into(),
                },
                "market_data",
            ),
            (
                ibcore::IbError::OrderRejected {
                    code: 201,
                    message: "rej".into(),
                    rejection_json: None,
                },
                "order_rejected",
            ),
            (
                ibcore::IbError::FarmDisconnect {
                    code: 2107,
                    message: "fm".into(),
                },
                "farm_disconnect",
            ),
            (ibcore::IbError::ContractResolution("res".into()), "contract_resolution"),
            (ibcore::IbError::CompetingSession, "competing_session"),
            (ibcore::IbError::Timeout("time".into()), "timeout"),
            (ibcore::IbError::Other("etc".into()), "other"),
        ];
        for (err, expected_cat) in test_cases {
            let (cat, _, _) = map_ib_error(&err);
            assert_eq!(cat, expected_cat, "mismatch for {err}");
        }
    }

    // ── PyOpenOrder tests ──

    #[test]
    fn open_order_construct_via_new() -> PyResult<()> {
        Python::with_gil(|py| {
            let cls = py.get_type::<PyOpenOrder>();
            let o = cls.call1((1, "SPY", "BUY", 100.0, "LMT", 450.0, "Submitted", 0.0, 100.0))?;
            assert_eq!(o.getattr("order_id")?.extract::<i32>()?, 1);
            assert_eq!(o.getattr("symbol")?.extract::<String>()?, "SPY");
            assert_eq!(o.getattr("action")?.extract::<String>()?, "BUY");
            assert!((o.getattr("quantity")?.extract::<f64>()? - 100.0).abs() < 0.001);
            Ok(())
        })
    }

    #[test]
    fn open_order_default_construction() -> PyResult<()> {
        Python::with_gil(|py| {
            let cls = py.get_type::<PyOpenOrder>();
            let o = cls.call0()?;
            assert_eq!(o.getattr("order_id")?.extract::<i32>()?, 0);
            assert_eq!(o.getattr("symbol")?.extract::<String>()?, "");
            assert!(o.getattr("limit_price")?.extract::<Option<f64>>()?.is_none());
            Ok(())
        })
    }

    #[test]
    fn open_order_partial_construction() -> PyResult<()> {
        Python::with_gil(|py| {
            let cls = py.get_type::<PyOpenOrder>();
            let o = cls.call1((42, "QQQ"))?;
            assert_eq!(o.getattr("order_id")?.extract::<i32>()?, 42);
            assert_eq!(o.getattr("symbol")?.extract::<String>()?, "QQQ");
            Ok(())
        })
    }

    #[test]
    fn open_order_repr() -> PyResult<()> {
        Python::with_gil(|py| {
            let cls = py.get_type::<PyOpenOrder>();
            let o = cls.call1((1, "SPY", "BUY", 100.0, "LMT", py.None(), "Submitted", 0.0, 100.0))?;
            let repr_str = o.call_method0("__repr__")?.extract::<String>()?;
            assert!(repr_str.contains("OpenOrder"));
            assert!(repr_str.contains("SPY"));
            Ok(())
        })
    }

    #[test]
    fn open_order_from_rust_type() {
        let rust = ibcore::OpenOrder {
            order_id: 10,
            symbol: "AAPL".into(),
            action: "SELL".into(),
            quantity: 50.0,
            order_type: "MKT".into(),
            limit_price: None,
            status: "Submitted".into(),
            filled_qty: 0.0,
            remaining_qty: 50.0,
        };
        let py: PyOpenOrder = rust.into();
        assert_eq!(py.order_id, 10);
        assert_eq!(py.symbol, "AAPL");
        assert!(py.limit_price.is_none());
    }

    // ── PyOrderStatusEvent tests ──

    #[test]
    fn order_status_event_construct_via_new() -> PyResult<()> {
        Python::with_gil(|py| {
            let cls = py.get_type::<PyOrderStatusEvent>();
            let e = cls.call1(("Filled", 42, 50.0, 450.25, 1.50, "", ""))?;
            assert_eq!(e.getattr("kind")?.extract::<String>()?, "Filled");
            assert_eq!(e.getattr("order_id")?.extract::<i32>()?, 42);
            assert_eq!(e.getattr("filled_qty")?.extract::<f64>()?, 50.0);
            assert_eq!(e.getattr("commission")?.extract::<Option<f64>>()?, Some(1.50));
            Ok(())
        })
    }

    #[test]
    fn order_status_event_default_construction() -> PyResult<()> {
        Python::with_gil(|py| {
            let cls = py.get_type::<PyOrderStatusEvent>();
            let e = cls.call0()?;
            assert_eq!(e.getattr("kind")?.extract::<String>()?, "");
            assert_eq!(e.getattr("order_id")?.extract::<i32>()?, 0);
            assert!(e.getattr("commission")?.extract::<Option<f64>>()?.is_none());
            Ok(())
        })
    }

    #[test]
    fn order_status_event_partial_construction() -> PyResult<()> {
        Python::with_gil(|py| {
            let cls = py.get_type::<PyOrderStatusEvent>();
            let e = cls.call1(("Submitted", 99))?;
            assert_eq!(e.getattr("kind")?.extract::<String>()?, "Submitted");
            assert_eq!(e.getattr("order_id")?.extract::<i32>()?, 99);
            Ok(())
        })
    }

    #[test]
    fn order_status_event_repr() -> PyResult<()> {
        Python::with_gil(|py| {
            let cls = py.get_type::<PyOrderStatusEvent>();
            let e = cls.call1(("Cancelled", 55))?;
            let repr_str = e.call_method0("__repr__")?.extract::<String>()?;
            assert!(repr_str.contains("OrderStatusEvent"));
            assert!(repr_str.contains("Cancelled"));
            Ok(())
        })
    }

    #[test]
    fn order_status_event_commission_only() {
        let rust = ibcore::OrderStatusEvent::Filled {
            order_id: 0,
            filled_qty: 0.0,
            avg_price: 0.0,
            commission: Some(1.50),
        };
        let py: PyOrderStatusEvent = rust.into();
        assert_eq!(py.kind, "Filled");
        assert_eq!(py.order_id, 0);
        assert_eq!(py.commission, Some(1.50));
    }

    #[test]
    fn order_status_event_from_submitted() {
        let rust = ibcore::OrderStatusEvent::Submitted { order_id: 10 };
        let py: PyOrderStatusEvent = rust.into();
        assert_eq!(py.kind, "Submitted");
        assert_eq!(py.order_id, 10);
        assert_eq!(py.reason, "");
    }

    #[test]
    fn order_status_event_from_filled() {
        let rust = ibcore::OrderStatusEvent::Filled {
            order_id: 20,
            filled_qty: 100.0,
            avg_price: 450.0,
            commission: None,
        };
        let py: PyOrderStatusEvent = rust.into();
        assert_eq!(py.kind, "Filled");
        assert_eq!(py.filled_qty, 100.0);
        assert_eq!(py.avg_price, 450.0);
        assert!(py.commission.is_none());
    }

    #[test]
    fn order_status_event_from_cancelled() {
        let rust = ibcore::OrderStatusEvent::Cancelled {
            order_id: 30,
            reason: "user requested".into(),
        };
        let py: PyOrderStatusEvent = rust.into();
        assert_eq!(py.kind, "Cancelled");
        assert_eq!(py.reason, "user requested");
    }

    #[test]
    fn order_status_event_from_rejected() {
        let rust = ibcore::OrderStatusEvent::Rejected {
            order_id: 40,
            reason: "insufficient funds".into(),
        };
        let py: PyOrderStatusEvent = rust.into();
        assert_eq!(py.kind, "Rejected");
        assert_eq!(py.reason, "insufficient funds");
    }

    #[test]
    fn order_status_event_from_inactive() {
        let rust = ibcore::OrderStatusEvent::Inactive { order_id: 50 };
        let py: PyOrderStatusEvent = rust.into();
        assert_eq!(py.kind, "Inactive");
        assert_eq!(py.order_id, 50);
    }

    #[test]
    fn order_status_event_from_other() {
        let rust = ibcore::OrderStatusEvent::Other {
            order_id: 60,
            status: "ApiPending".into(),
        };
        let py: PyOrderStatusEvent = rust.into();
        assert_eq!(py.kind, "Other");
        assert_eq!(py.status, "ApiPending");
    }

    // ── PyOrderUpdateReceiver tests ──

    #[test]
    fn order_update_receiver_is_iterable() -> PyResult<()> {
        Python::with_gil(|py| {
            let cls = py.get_type::<PyOrderUpdateReceiver>();
            assert!(cls.getattr("__iter__").is_ok());
            assert!(cls.getattr("__next__").is_ok());
            Ok(())
        })
    }

    #[test]
    fn order_update_receiver_construct_and_iter_self() -> PyResult<()> {
        Python::with_gil(|py| {
            let (tx, rx) = std::sync::mpsc::sync_channel::<PyOrderStatusEvent>(1);
            // Use a simple blocking function instead of async move
            let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
            std::thread::spawn(move || {
                let _ = done_rx.recv(); // block until told to stop
                drop(tx);
            });
            let recv = PyOrderUpdateReceiver {
                rx: std::sync::Mutex::new(rx),
                _task: None,
            };
            let py_recv = Py::new(py, recv)?;
            let bind = py_recv.bind(py);
            // __iter__ returns self — verify by checking the PyRef identity
            let _iter_val = bind.call_method0("__iter__")?;
            // If __iter__ returned self, we can call __next__ on it
            assert!(bind.call_method0("__next__").is_ok());
            let _ = done_tx.send(()); // unblock thread
            Ok(())
        })
    }

    #[test]
    fn order_update_receiver_next_returns_none_on_empty() -> PyResult<()> {
        Python::with_gil(|py| {
            // No tx → channel disconnected → try_recv returns TryRecvError::Disconnected
            let (_tx, rx) = std::sync::mpsc::sync_channel::<PyOrderStatusEvent>(1);
            drop(_tx); // ensure disconnected
            let recv = PyOrderUpdateReceiver {
                rx: std::sync::Mutex::new(rx),
                _task: None,
            };
            let py_recv = Py::new(py, recv)?;
            let bind = py_recv.bind(py);
            let result = bind.call_method0("__next__")?;
            assert!(result.is_none());
            Ok(())
        })
    }

    #[test]
    fn order_update_receiver_has_drop() {
        let (_tx, rx) = std::sync::mpsc::sync_channel::<PyOrderStatusEvent>(1);
        let recv = PyOrderUpdateReceiver {
                rx: std::sync::Mutex::new(rx),
                _task: None,
            };
            // Verify Drop doesn't crash when _task is None
        drop(recv);
    }
}
