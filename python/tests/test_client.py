"""Tests for IbClient class existence, method presence, and error handling."""

import pytest
from _ibcore import IbClient, IbError


class TestIbClientClass:
    """IbClient class existence and method presence."""

    def test_class_exists(self):
        assert IbClient is not None

    def test_has_connect_method(self):
        assert hasattr(IbClient, "connect")

    def test_has_methods(self):
        methods = [
            "connect",
            "reconnect",
            "disconnect",
            "stock_snapshot",
            "option_snapshot",
            "positions",
            "account_summary",
            "net_liquidation",
            "place_order",
            "cancel_order",
            "diagnostic_events",
        ]
        for method in methods:
            assert hasattr(IbClient, method), f"IbClient missing method: {method}"


class TestIbClientConnectErrors:
    """IbClient connect error handling (no running Gateway needed)."""

    def test_connect_requires_gateway(self):
        """Connecting to a dead port raises IbError."""
        with pytest.raises(IbError):
            IbClient.connect("127.0.0.1", 9999, 1, "delayed", "paper")

    def test_connect_rejects_bad_account_type(self):
        """Invalid account_type raises IbError."""
        with pytest.raises(IbError):
            IbClient.connect("127.0.0.1", 9999, 1, "delayed", "invalid")
