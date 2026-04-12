# Deferred Items

- `tests/test_pipeline.c:4361` — `pipeline_fastapi_depends_edges` still fails when the entire native suite is run with `CBM_FORCE_PIPELINE_MODE=parallel`. This surfaced during plan 03-04 verification but is unrelated to the worker-lifetime use-after-free fixed in this plan.
