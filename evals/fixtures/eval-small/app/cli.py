"""Command-line entry point."""
from app.models import Customer
from app.service import OrderService
from app.util.strings import slugify


def main() -> int:
    service = OrderService()
    customer = Customer("Ada Lovelace", "ada@example.com")
    order = service.create(customer)
    order.add_line("widget", 12.34)
    print(slugify(service.checkout(order)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
