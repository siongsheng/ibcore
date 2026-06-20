"""ibcore — Standalone IB Gateway integration layer.

Provides market data snapshots, diagnostic event streaming, order placement,
and error handling for Interactive Brokers Gateway.

Usage:
    from ibcore import IbClient

    ib = IbClient.connect("127.0.0.1", 4002, 1, "delayed", "paper")
    snap = ib.stock_snapshot("SPY")
    print(f"SPY: ${snap.last:.2f}")
"""

from ibcore._ibcore import (
    AccountType,
    ConnectionState,
    DiagnosticEvent,
    DiagnosticEventReceiver,
    FarmState,
    IbClient,
    IbError,
    OptionSnapshot,
    StockSnapshot,
)

__all__ = [
    "AccountType",
    "ConnectionState",
    "DiagnosticEvent",
    "DiagnosticEventReceiver",
    "FarmState",
    "IbClient",
    "IbError",
    "OptionSnapshot",
    "StockSnapshot",
]
