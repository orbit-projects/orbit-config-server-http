# Roadmap

## v0.1.0-alpha

- [x] Implement the Orbit versioned remote-configuration provider contract over bounded HTTPX.
- [x] Add sanitized errors, strict response parsing, polling, and explicit client lifecycle.
- [x] Add package docs and offline mock-transport tests.
- [ ] Validate against a live, independently operated endpoint and hosted CI.

## Future

- Evaluate server-sent events only after a compatible server contract and backpressure policy are
  defined.
- Evaluate mTLS client identity as an opt-in transport configuration with rotation tests.
