# Integration tests (manual)

These tests execute against a **live Union/Flyte cluster** (the demo cluster) and
are intentionally excluded from CI and the default `pytest` run. They are meant
to be run by hand when you want to validate that the Flyte 2 usage in this repo
actually works end-to-end remotely.

## Config

[`config.yaml`](./config.yaml) targets:

```yaml
admin:
  endpoint: dns:///demo.hosted.unionai.cloud
image:
  builder: remote
task:
  org: demo
  project: flytesnacks
  domain: development
```

## Running

1. Authenticate to the demo cluster (opens a browser):

   ```bash
   union create login --auth device-flow --host demo.hosted.unionai.cloud
   ```

2. Run the suite (the `RUN_INTEGRATION` gate + `-m integration` marker keep it
   from running accidentally):

   ```bash
   RUN_INTEGRATION=1 pytest tests/integration -m integration -s
   ```

Without `RUN_INTEGRATION=1` the tests **skip**, so a plain `pytest tests/integration`
is safe.

## What it covers

- `test_remote_smoke.py` — runs `remote_tasks.memory_roundtrip` on the cluster,
  which writes to and rehydrates a `flyte.ai.agents.MemoryStore` through object
  storage (the same durable-memory path the production pipelines use). It uses a
  standalone task env so it does **not** require the `github-token` /
  `anthropic-api-key` secrets to exist on the cluster.
