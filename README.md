# Orbit Config Server HTTP

`orbit-config-server-http` implements the provider contract from
[`orbit-config-server`](https://github.com/orbit-projects/orbit-config-server) over a small
versioned HTTP API. It owns an asynchronous HTTPX client, fetches complete configuration snapshots,
and exposes an opt-in polling stream. Core and the capability remain independent of HTTPX.

```bash
python -m pip install orbit-core orbit-config-server orbit-config-server-http
```

## Provider endpoint

The provider requests `GET {base_url}/v1/config/{section}` and expects HTTP `200`, a JSON media type,
and an object with exactly this shape:

```json
{"version":12,"config":{"feature_enabled":true,"region":"ap-south-1"}}
```

Each response is a complete replacement. Versions must be positive, strictly increasing integers;
the same version cannot identify different payloads. The endpoint must reject unauthorized callers
and return only the configuration section the caller is permitted to read. The adapter does not
provide a server, persistence layer, or authorization policy.

```python
from orbit_config_server_http import HTTPRemoteConfigurationProvider

provider = HTTPRemoteConfigurationProvider(
    "https://config.internal.example",
    "payments",
    bearer_token=token_loaded_from_a_secret_manager,
    timeout=4,
    poll_interval=2,
    poll_jitter=0.1,
)
try:
    snapshot = await provider.fetch()
    settings = snapshot.validate_as(PaymentSettings)
    # Apply settings with RemoteConfigSection so Core observers can veto atomically.
finally:
    await provider.aclose()
```

Use `async with` when provider lifetime is bounded by one scope. Attach `provider.watch()` to an
application-managed task and cancel/await it before `aclose()`. The provider does not create a
background task by itself.

## Security and limits

HTTPS is required unless `allow_insecure_http=True` is explicitly selected for local development.
The client does not follow redirects or read ambient proxy environment variables. The base URL
cannot contain credentials, query parameters, or fragments. Optional bearer tokens are held as
secret values and sent only to the configured endpoint. Never put tokens in URLs or logs.

Each response is streamed with a hard 1 MiB configuration payload limit plus a small bounded JSON
envelope. Duplicate JSON keys and non-finite numbers fail closed. The provider checks media type,
status, version monotonicity, request timeout, and polling interval. Errors omit response bodies,
credentials, hostnames, and underlying exception messages. `HTTPConfigurationProviderError.status_code`
contains only the numeric HTTP status when the server returns a non-200 response.

The adapter uses polling, so changes are observed no faster than its configured interval plus
request time. A bounded default jitter spreads replica polls; set `poll_jitter=0` only when
deterministic timing is needed. Keep the Core watermark in durable application state when required and pass it to
`watch(after_version=...)`. Provider and Core resource lifetimes are separate: close the provider
explicitly during application shutdown. This package does not retry transient failures; supervise
the watcher with the application lifecycle and compose bounded retry behavior deliberately.

## Status and documentation

This is an unpublished v0.1.0-alpha candidate. See the package-specific [documentation index](docs/README.md),
[architecture](docs/architecture/overview.md), [operations](docs/operations/README.md), and
[security guidance](docs/security/overview.md). The HTTP API shape is an Orbit adapter contract;
it is not an established third-party server standard.

## Development

```bash
python -m pip install -e ../orbit-core -e ../orbit-config-server
python -m pip install -e '.[dev]'
pytest
ruff check src tests
mypy
python -m build
```

Licensed under Apache-2.0.
