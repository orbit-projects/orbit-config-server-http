# Security guidance

Use an HTTPS endpoint with a trusted certificate. The adapter disables redirects, preventing an
endpoint from redirecting bearer credentials to another host. It does not use ambient proxy
environment variables. Network-level egress policy should further restrict the configured host.

Load bearer tokens from a secret manager and rotate them in the application by replacing the
provider under an orderly lifecycle transition. The provider keeps the token in a `SecretStr` and
does not include it in repr or error text. The caller remains responsible for token refresh and
least-privilege endpoint authorization.

Responses are streamed and capped before parsing; decompressed bytes are subject to the same bound.
Duplicate JSON keys and nonstandard numeric constants are rejected. Do not place secrets in logs,
traces, exception context, metric labels, URLs, or remote error bodies. HTTP status codes are safe
to record as bounded numeric attributes; response content and provider exception messages are not.

`allow_insecure_http=True` is for local development only. It disables transport confidentiality and
server authentication; never use it across an untrusted network. A bearer token over HTTP is
exposed to network observers.
