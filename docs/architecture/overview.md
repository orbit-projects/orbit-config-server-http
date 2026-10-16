# Architecture and ownership

`orbit-config-server` defines complete, versioned remote snapshots and the provider protocol. This
package implements that protocol over HTTP. Core remains responsible for its configuration
registry, typed model validation, atomic section replacement, and lifecycle-managed tasks.

The adapter requests one configured section from one fixed endpoint. It owns HTTPX, timeout and
polling configuration, optional bearer authentication, response size bounds, version checks, and
transport error sanitization. It does not run a server, persist revisions, manage credentials, or
apply configuration to Core. An application composes `RemoteConfigSection` with a Core `Config`
instance and owns the watch task and shutdown order.

The remote response is JSON with exactly two fields: positive integer `version` and object `config`.
Every version describes one immutable full replacement. The capability revalidates the payload and
the application's declared Pydantic section model before Core sees the replacement.

## Dependency direction

```text
orbit-core <- orbit-config-server <- orbit-config-server-http
```

The arrow means “depends on.” Core does not import the adapter or HTTPX. Other provider adapters
can implement the same capability contract with their native client or process language.
