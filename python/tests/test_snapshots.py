"""Tests for StockSnapshot and OptionSnapshot PyO3 types."""

import pytest
from ibcore._ibcore import StockSnapshot, OptionSnapshot


class TestStockSnapshot:
    """StockSnapshot field access and default values."""

    def test_fields(self):
        snap = StockSnapshot(last=450.25, bid=450.20, ask=450.30, close=448.00)
        assert isinstance(snap.last, float)
        assert snap.last == pytest.approx(450.25)
        assert snap.bid == pytest.approx(450.20)
        assert snap.ask == pytest.approx(450.30)
        assert snap.close == pytest.approx(448.00)

    def test_defaults(self):
        snap = StockSnapshot()
        assert snap.last == pytest.approx(0.0)
        assert snap.bid == pytest.approx(0.0)
        assert snap.ask == pytest.approx(0.0)
        assert snap.close == pytest.approx(0.0)


class TestOptionSnapshot:
    """OptionSnapshot field access, defaults, and repr."""

    def test_fields(self):
        snap = OptionSnapshot(
            bid=2.50,
            ask=2.60,
            last=2.55,
            option_iv=0.25,
            option_delta=0.45,
            option_gamma=0.05,
            option_theta=-0.03,
            option_price=2.55,
            underlying_price=450.0,
        )
        assert isinstance(snap.bid, float)
        assert snap.bid == pytest.approx(2.50)
        assert snap.ask == pytest.approx(2.60)
        assert snap.last == pytest.approx(2.55)
        assert snap.option_iv == pytest.approx(0.25)
        assert snap.option_delta == pytest.approx(0.45)
        assert snap.option_gamma == pytest.approx(0.05)
        assert snap.option_theta == pytest.approx(-0.03)
        assert snap.option_price == pytest.approx(2.55)
        assert snap.underlying_price == pytest.approx(450.0)

    def test_defaults(self):
        snap = OptionSnapshot()
        assert snap.bid == pytest.approx(0.0)
        assert snap.ask == pytest.approx(0.0)
        assert snap.last == pytest.approx(0.0)
        assert snap.option_iv == pytest.approx(0.0)
        assert snap.option_delta == pytest.approx(0.0)
        assert snap.option_gamma == pytest.approx(0.0)
        assert snap.option_theta == pytest.approx(0.0)
        assert snap.option_price == pytest.approx(0.0)
        assert snap.underlying_price == pytest.approx(0.0)

    def test_repr(self):
        snap = OptionSnapshot(
            bid=2.50,
            ask=2.60,
            last=2.55,
            option_iv=0.25,
            option_delta=0.45,
            option_gamma=0.05,
            option_theta=-0.03,
            option_price=2.55,
            underlying_price=450.0,
        )
        r = repr(snap)
        assert "OptionSnapshot" in r
        assert "2.5" in r
        assert "0.45" in r
