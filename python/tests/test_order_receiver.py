"""Tests for OrderUpdateReceiver."""

from ibcore._ibcore import OrderUpdateReceiver


class TestOrderUpdateReceiver:
    """OrderUpdateReceiver class existence and method presence.

    Since OrderUpdateReceiver requires a live IB Gateway to produce real events,
    these tests verify type-level properties (class exists, has expected methods).
    """

    def test_class_exists(self):
        assert OrderUpdateReceiver is not None

    def test_has_iter_and_next(self):
        assert hasattr(OrderUpdateReceiver, "__iter__")
        assert hasattr(OrderUpdateReceiver, "__next__")

    def test_iter_returns_self(self):
        """Verify that iter(receiver) calls __iter__ which should return self.
        This is a structural test only — construction requires a live Gateway."""
        # Just verify the dunder methods exist
        assert callable(OrderUpdateReceiver.__iter__)
        assert callable(OrderUpdateReceiver.__next__)
