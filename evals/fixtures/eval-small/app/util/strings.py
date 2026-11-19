"""String helpers."""


def slugify(text: str) -> str:
    """Lower-case and hyphenate a string."""
    return "-".join(text.lower().split())


def unused_helper(text: str) -> str:
    """Dead code: nothing in this repository calls this function."""
    return text[::-1]
