"""Self-contained Flyte task used by the integration smoke test.

Kept separate from the ``flyte_agent_loop`` package so the integration test can
exercise a real remote execution (and the ``flyte.ai.agents.MemoryStore``
round-trip our pipelines depend on) without requiring the production secrets
(``github-token`` / ``anthropic-api-key``) to exist on the cluster.
"""

from __future__ import annotations

import flyte
from flyte.ai.agents import MemoryStore

env = flyte.TaskEnvironment(
    name="flyte_agent_loop_itest",
    image=flyte.Image.from_debian_base(),
    resources=flyte.Resources(cpu="1", memory="1Gi"),
)


@env.task
async def memory_roundtrip(key: str, value: str) -> str:
    """Write ``value`` into a keyed MemoryStore, then reopen it and read it back.

    Exercises the same durable-memory path the real pipelines use: object-store
    persistence via ``MemoryStore.save`` and rehydration via ``get_or_create``.
    Returns the value read back from the reopened store (should equal ``value``).
    """
    store = await MemoryStore.get_or_create.aio(key=key)
    await store.write_text.aio("itest/probe.txt", value, actor="integration")
    await store.save.aio()

    reopened = await MemoryStore.get_or_create.aio(key=key)
    return await reopened.read_text.aio("itest/probe.txt", default="MISSING")
