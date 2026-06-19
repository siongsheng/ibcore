"""Tests for DiagnosticEvent and enum-like classes (FarmState, ConnectionState, AccountType)."""

from ibcore._ibcore import DiagnosticEvent, FarmState, ConnectionState, AccountType


class TestDiagnosticEvent:
    """DiagnosticEvent field access and repr."""

    def test_fields(self):
        event = DiagnosticEvent(
            gateway_version=221,
            error_code=2104,
            error_message="Market data farm OK",
            farm_status="ok",
            connection_state="connected",
            account_type="paper",
            os="linux",
            timestamp="2026-01-15T10:30:00Z",
        )
        assert isinstance(event.gateway_version, int)
        assert event.gateway_version == 221
        assert isinstance(event.error_code, int)
        assert event.error_code == 2104
        assert isinstance(event.error_message, str)
        assert event.error_message == "Market data farm OK"
        assert isinstance(event.farm_status, str)
        assert event.farm_status == "ok"
        assert isinstance(event.connection_state, str)
        assert event.connection_state == "connected"
        assert isinstance(event.account_type, str)
        assert event.account_type == "paper"
        assert isinstance(event.os, str)
        assert event.os == "linux"
        assert isinstance(event.timestamp, str)
        assert event.timestamp == "2026-01-15T10:30:00Z"

    def test_repr(self):
        event = DiagnosticEvent(
            gateway_version=221,
            error_code=2104,
            error_message="test",
            farm_status="ok",
            connection_state="connected",
            account_type="paper",
            os="linux",
            timestamp="2026-01-15T10:30:00Z",
        )
        r = repr(event)
        assert "DiagnosticEvent" in r


class TestFarmState:
    """FarmState constants and from_code helper."""

    def test_constants(self):
        assert FarmState.OK == "ok"
        assert FarmState.WARNING == "warning"
        assert FarmState.INACTIVE == "inactive"

    def test_from_code(self):
        assert FarmState.from_code(2104) == "ok"
        assert FarmState.from_code(2107) == "inactive"
        assert FarmState.from_code(999) == "unknown(999)"


class TestConnectionState:
    """ConnectionState constants."""

    def test_constants(self):
        assert ConnectionState.CONNECTED == "connected"
        assert ConnectionState.DISCONNECTED == "disconnected"
        assert ConnectionState.RECONNECTING == "reconnecting"


class TestAccountType:
    """AccountType constants."""

    def test_constants(self):
        assert AccountType.LIVE == "live"
        assert AccountType.PAPER == "paper"
