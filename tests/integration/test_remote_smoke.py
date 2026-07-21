"""Manual integration smoke test against a live Union tenant.

Intended for Union employees running against an actual tenant. It is NOT run in
CI or by the default ``pytest`` invocation (which is scoped to ``tests/unit``).
Run it explicitly:

    RUN_INTEGRATION=1 pytest tests/integration -m integration -s

It initializes from ``tests/integration/config.yaml`` (org=demo,
project=flytesnacks, domain=development, remote image builder) which uses
device-flow auth (``admin.authType: DeviceFlow``) — the first run opens a browser
to authenticate against the tenant. See
https://www.union.ai/docs/v2/union/user-guide/authenticating/#device-flow

The test runs a real remote task that round-trips a ``flyte.ai.agents.MemoryStore``
through object storage — the same durable-memory path the production pipelines
rely on.
"""

from __future__ import annotations

import os
import pathlib
import uuid

import pytest

CONFIG = pathlib.Path(__file__).parent / "config.yaml"


def _require_integration() -> None:
    if os.environ.get("RUN_INTEGRATION") != "1":
        pytest.skip("set RUN_INTEGRATION=1 to run integration tests against the demo cluster")


@pytest.mark.integration
def test_memory_roundtrip_on_demo_cluster() -> None:
    _require_integration()

    import flyte

    import remote_tasks  # noqa: E402  (importable via pytest's prepend path)

    assert CONFIG.exists(), f"missing integration config at {CONFIG}"
    flyte.init_from_config(str(CONFIG))

    value = f"probe-{uuid.uuid4().hex[:8]}"
    key = f"itest-{uuid.uuid4().hex[:8]}"

    run = flyte.run(remote_tasks.memory_roundtrip, key=key, value=value)
    print(f"\nsubmitted run: {run.url}")
    run.wait()

    outputs = run.outputs()
    assert outputs[0] == value, f"round-tripped memory mismatch: {outputs[0]!r} != {value!r}"
