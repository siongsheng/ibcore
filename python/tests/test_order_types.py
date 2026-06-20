"""Tests for OpenOrder and OrderStatusEvent types."""

from _ibcore import OpenOrder, OrderStatusEvent


class TestOpenOrder:
    """OpenOrder construction and field access."""

    def test_construct_with_all_args(self):
        o = OpenOrder(
            order_id=1,
            symbol="SPY",
            action="BUY",
            quantity=100.0,
            order_type="LMT",
            limit_price=450.0,
            status="Submitted",
            filled_qty=0.0,
            remaining_qty=100.0,
        )
        assert o.order_id == 1
        assert o.symbol == "SPY"
        assert o.action == "BUY"
        assert o.quantity == 100.0
        assert o.order_type == "LMT"
        assert o.limit_price == 450.0
        assert "OpenOrder" in repr(o)

    def test_default_construction(self):
        o = OpenOrder()
        assert o.order_id == 0
        assert o.symbol == ""
        assert o.limit_price is None

    def test_partial_construction(self):
        o = OpenOrder(order_id=42, symbol="QQQ")
        assert o.order_id == 42
        assert o.symbol == "QQQ"

    def test_repr(self):
        o = OpenOrder(order_id=1, symbol="SPY")
        assert "OpenOrder" in repr(o)
        assert "SPY" in repr(o)


class TestOrderStatusEvent:
    """OrderStatusEvent construction and field access."""

    def test_construct_filled(self):
        e = OrderStatusEvent(
            kind="Filled",
            order_id=42,
            filled_qty=50.0,
            avg_price=450.25,
            commission=1.50,
            reason="",
            status="",
        )
        assert e.kind == "Filled"
        assert e.filled_qty == 50.0
        assert "OrderStatusEvent" in repr(e)

    def test_default_construction(self):
        e = OrderStatusEvent()
        assert e.kind == ""
        assert e.order_id == 0
        assert e.filled_qty == 0.0
        assert e.commission is None

    def test_partial_construction(self):
        e = OrderStatusEvent(kind="Submitted", order_id=99)
        assert e.kind == "Submitted"
        assert e.order_id == 99

    def test_commission_only_event(self):
        e = OrderStatusEvent(kind="Filled", order_id=0, commission=1.50)
        assert e.order_id == 0
        assert e.commission == 1.50
        assert e.filled_qty == 0.0
        is_commission = e.order_id == 0 and e.commission is not None
        assert is_commission is True
