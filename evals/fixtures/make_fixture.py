#!/usr/bin/env python3
"""Write the `eval-small` fixture repository.

The fixture is committed next to this script (evals/fixtures/eval-small), so
running it is only needed to regenerate the files after a deliberate change.
Every symbol, call edge and file it writes is recorded in
expected/eval-small.json; keep the two in step.

    python3 evals/fixtures/make_fixture.py            # rewrite eval-small/
    python3 evals/fixtures/make_fixture.py --out DIR  # write a copy elsewhere
"""
import argparse
import os
import shutil
import sys

LONG_NAME = ("very_long_function_name_" + "0" * 200)[:200]

FILES = {
    "README.md": """# eval-small

A tiny polyglot repository with a hand-verified ground truth, used by the
codebase-memory-mcp-neo4j evals. Every symbol, call and file in here is
listed in `../expected/eval-small.json`.

The `OrderService` (Python) and `InvoiceService` (Java) classes are the two
entry points. `compute_total` is mentioned here on purpose: it does not exist
in the code, so a search for it must not resolve to a symbol.
""",
    ".cbmignore": """# Vendored code is never indexed (COV-02 / COV-10 rely on this).
vendor/
""",
    ".gitignore": """__pycache__/
*.pyc
# Generated code is git-ignored, so discovery must skip it (COV-10).
generated/
""",
    "Dockerfile": """FROM python:3.12-slim
WORKDIR /srv
COPY app ./app
ENV ORDER_ENV=prod
CMD ["python", "-m", "app.cli"]
""",
    "docker-compose.yml": """services:
  orders:
    build: .
    environment:
      ORDER_ENV: prod
      DATABASE_URL: postgres://db:5432/orders
    depends_on:
      - db
  db:
    image: postgres:16
""",
    "app/__init__.py": "",
    "app/util/__init__.py": "",
    "app/money.py": '''"""Money helpers: rounding and formatting of cent amounts."""


def round_cents(amount: float) -> int:
    """Round a float dollar amount to whole cents."""
    return int(amount * 100 + 0.5)


def format_cents(cents: int) -> str:
    """Format cents as a dollar string, e.g. 1234 -> '$12.34'."""
    cents = round_cents(cents / 100)
    return f"${cents // 100}.{cents % 100:02d}"
''',
    "app/models.py": '''"""Domain model: customers and orders."""
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
''',
    "app/service.py": '''"""Order workflow: validation, notification and checkout."""
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
''',
    "app/util/strings.py": '''"""String helpers."""


def slugify(text: str) -> str:
    """Lower-case and hyphenate a string."""
    return "-".join(text.lower().split())


def unused_helper(text: str) -> str:
    """Dead code: nothing in this repository calls this function."""
    return text[::-1]
''',
    "app/cli.py": '''"""Command-line entry point."""
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
''',
    "hub/__init__.py": "",
    "hub/hub.py": (
        '"""A hub function with twelve callers in one file (COV-05, SG-DEGREE, TP-HUB)."""\n'
        "\n\ndef hub() -> None:\n"
        '    """Called by caller_01 .. caller_12."""\n'
        "    return None\n"
        + "".join(f"\n\ndef caller_{i:02d}() -> None:\n    hub()\n" for i in range(1, 13))
    ),
    "dup/__init__.py": "",
    "dup/alpha.py": '''def shared_name() -> str:
    """Same function name as dup/beta.py (duplicate-name case)."""
    return "alpha"
''',
    "dup/beta.py": '''def shared_name() -> str:
    """Same function name as dup/alpha.py (duplicate-name case)."""
    return "beta"
''',
    "unicode/__init__.py": "",
    "unicode/ünïcødé.py": (
        '"""Unicode file name and identifier, plus a 200-character identifier."""\n'
        "\n\ndef ünïcode_fn() -> str:\n"
        '    return "ok"\n'
        f"\n\ndef {LONG_NAME}() -> None:\n    return None\n"
    ),
    "tests/__init__.py": "",
    "tests/test_service.py": '''from app.models import Customer
from app.service import OrderService


def test_checkout():
    service = OrderService()
    order = service.create(Customer("a", "a@example.com"))
    order.add_line("x", 1.0)
    assert service.checkout(order).endswith("$1.00")
''',
    "docs/ARCHITECTURE.md": """# Architecture

## Orders

`OrderService.checkout` validates an order with `validate` and renders the
receipt with `notify`, which formats money through `format_cents`.

## Billing

`InvoiceService.issue` audits an invoice and formats the total with
`Money.format`.

## Dead code

`unused_helper` is intentionally unreferenced.
""",
    "vendor/thirdparty/lib.py": '''def vendored_fn():
    """Must never appear in the graph: vendor/ is in .cbmignore."""
    return 1
''',
    "generated/gen.py": '''def generated_fn():
    """Must never appear in the graph: generated/ is git-ignored."""
    return 1
''',
    "java/src/main/java/com/example/billing/Money.java": """package com.example.billing;

/** Money helpers: rounding and formatting of cent amounts. */
public final class Money {
    private Money() {}

    public static long roundCents(double amount) {
        return Math.round(amount * 100);
    }

    public static String format(long cents) {
        long c = roundCents(cents / 100.0);
        return "$" + (c / 100) + "." + String.format("%02d", c % 100);
    }
}
""",
    "java/src/main/java/com/example/billing/Invoice.java": """package com.example.billing;

import java.util.ArrayList;
import java.util.List;

public class Invoice {
    private final List<Double> lines = new ArrayList<>();

    public void addLine(double amount) {
        lines.add(amount);
    }

    public long total() {
        long t = 0;
        for (double a : lines) {
            t += Money.roundCents(a);
        }
        return t;
    }
}
""",
    "java/src/main/java/com/example/billing/InvoiceService.java": """package com.example.billing;

import java.util.ArrayList;
import java.util.List;

public class InvoiceService {
    private final List<Invoice> issued = new ArrayList<>();

    public String issue(Invoice invoice) {
        audit(invoice);
        issued.add(invoice);
        return Money.format(invoice.total());
    }

    private void audit(Invoice invoice) {
        if (invoice.total() < 0) {
            throw new IllegalStateException("negative invoice");
        }
    }
}
""",
    "java/src/test/java/com/example/billing/InvoiceServiceTest.java": """package com.example.billing;

public class InvoiceServiceTest {
    public void testIssue() {
        InvoiceService service = new InvoiceService();
        Invoice invoice = new Invoice();
        invoice.addLine(1.0);
        String receipt = service.issue(invoice);
        if (!receipt.equals("$1.00")) {
            throw new AssertionError(receipt);
        }
    }
}
""",
}


def write(out_dir: str, clean: bool) -> int:
    if clean and os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    for rel, body in FILES.items():
        path = os.path.join(out_dir, *rel.split("/"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(body)
    return len(FILES)


def main() -> int:
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=os.path.join(here, "eval-small"))
    ap.add_argument("--clean", action="store_true", help="delete --out first")
    a = ap.parse_args()
    n = write(a.out, a.clean)
    print(f"wrote {n} files to {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
