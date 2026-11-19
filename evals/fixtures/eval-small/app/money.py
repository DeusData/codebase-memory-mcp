"""Money helpers: rounding and formatting of cent amounts."""


def round_cents(amount: float) -> int:
    """Round a float dollar amount to whole cents."""
    return int(amount * 100 + 0.5)


def format_cents(cents: int) -> str:
    """Format cents as a dollar string, e.g. 1234 -> '$12.34'."""
    cents = round_cents(cents / 100)
    return f"${cents // 100}.{cents % 100:02d}"
