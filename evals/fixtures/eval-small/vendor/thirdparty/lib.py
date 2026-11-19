def vendored_fn():
    """Must never appear in the graph: vendor/ is in .cbmignore."""
    return 1
