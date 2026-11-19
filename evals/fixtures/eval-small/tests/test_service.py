from app.models import Customer
from app.service import OrderService


def test_checkout():
    service = OrderService()
    order = service.create(Customer("a", "a@example.com"))
    order.add_line("x", 1.0)
    assert service.checkout(order).endswith("$1.00")
