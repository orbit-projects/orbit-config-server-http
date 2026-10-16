# Development and validation

Install sibling Core and capability packages from this workspace before the adapter's development
extra. Then run the package CI commands:

```bash
python -m pip install -e ../orbit-core -e ../orbit-config-server
python -m pip install -e '.[dev]'
pytest -q
ruff check src tests
ruff format --check src tests
mypy
python -m build
```

Tests use HTTPX `MockTransport`; they make no network requests and require no running server. Keep
coverage for duplicate JSON fields, body limits, cancellation, timeouts, redirects, malformed
versions, redaction, monotonic versions, and explicit resource closure. Add live-server validation
only when a separately operated compatible configuration endpoint is available.
