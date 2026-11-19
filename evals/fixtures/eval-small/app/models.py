"""Domain model: customers and orders."""
from app.money import round_cents


class Customer:
    def __init__(self, name: str, email: str):
        self.name = name
        self.email = email

    def display_name(self) -> str:
        """Customer name in title case."""
        return self.name.title()


class Order:
    def __init__(self, customer: Customer):
        self.customer = customer
        self.lines = []

    def add_line(self, description: str, amount: float) -> None:
        self.lines.append((description, amount))

    def total(self) -> int:
        """Order total in cents."""
        return sum(round_cents(amount) for _, amount in self.lines)
