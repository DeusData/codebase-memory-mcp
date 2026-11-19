"""Order workflow: validation, notification and checkout."""
from app.models import Order
from app.money import format_cents


class ValidationError(Exception):
    pass


def validate(order: Order) -> None:
    """Reject an order with no lines."""
    if not order.lines:
        raise ValidationError("order has no lines")


def notify(order: Order) -> str:
    """Build the receipt text sent to the customer."""
    return f"{order.customer.display_name()}: {format_cents(order.total())}"


class OrderService:
    def __init__(self):
        self.orders = []

    def create(self, customer) -> Order:
        order = Order(customer)
        self.orders.append(order)
        return order

    def checkout(self, order: Order) -> str:
        """Validate the order and return its receipt."""
        validate(order)
        receipt = notify(order)
        return receipt
