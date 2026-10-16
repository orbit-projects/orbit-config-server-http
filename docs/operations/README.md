# Operations

Run one provider per registered Core section and close it with the application. Start the watch
task after configuration composition and cancel/await it before closing the provider's HTTP client.
The adapter has no hidden worker or reconnect loop.

Tune `timeout` against the endpoint's request budget and `poll_interval` against its update SLO and
capacity. Polling cost grows with the number of application instances. The default bounded jitter
reduces synchronized requests across replicas; set `poll_jitter=0` only when deterministic polling
is needed. The adapter does not provide distributed leader election; every configured instance
polls independently.

On a transport or protocol error, `watch()` raises a sanitized
`HTTPConfigurationProviderError`. The application should preserve the last accepted local
configuration, record a bounded error category, and apply its own retry/backoff policy. A response
with stale or reused version data fails closed. Keep any persisted `RemoteConfigSection.last_version`
consistent with the service revision to detect rollback.

The package owns its HTTPX client. Call `aclose()` once during shutdown or use its async context
manager. Do not close the provider while a watch task is still running. The endpoint's certificate
trust is validated by HTTPX; never disable TLS verification in production.
