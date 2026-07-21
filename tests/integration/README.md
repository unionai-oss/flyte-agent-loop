# Integration tests (manual)

These tests execute against a **live Union tenant** (the `demo` tenant) and are
intentionally excluded from CI and the default `pytest` run. They are meant for
**Union employees** to run by hand against an actual tenant, to validate that the
Flyte 2 usage in this repo works end-to-end remotely.

## Config

[`config.yaml`](./config.yaml) targets the `demo` tenant with
[device-flow auth](https://www.union.ai/docs/v2/union/user-guide/authenticating/#device-flow):

```yaml
admin:
  endpoint: dns:///demo.hosted.unionai.cloud
  authType: DeviceFlow
image:
  builder: remote
task:
  org: demo
  project: flytesnacks
  domain: development
```

## Running

Because `authType: DeviceFlow` is set, there is no separate login command —
`flyte.init_from_config()` triggers the OAuth2 device flow on the first
authenticated call and prints a URL + code to open in your browser. Subsequent
runs reuse the cached token.

```bash
RUN_INTEGRATION=1 pytest tests/integration -m integration -s
```

Without `RUN_INTEGRATION=1` the tests **skip**, so a plain `pytest tests/integration`
is safe. (You can pre-authenticate out of band with any authenticated CLI call,
e.g. `flyte --config tests/integration/config.yaml get project`.)

## What it covers

- `test_remote_smoke.py` — runs `remote_tasks.memory_roundtrip` on the cluster,
  which writes to and rehydrates a `flyte.ai.agents.MemoryStore` through object
  storage (the same durable-memory path the production pipelines use). It uses a
  standalone task env so it does **not** require the `github-token` /
  `anthropic-api-key` secrets to exist on the cluster.
