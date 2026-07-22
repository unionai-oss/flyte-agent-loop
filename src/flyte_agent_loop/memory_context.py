"""Durable shared memory shared across all three pipelines.

Two keyed :class:`flyte.ai.agents.MemoryStore` s with **disjoint writer sets**:

* ``<key>-runs`` — one file per :class:`RunRecord` at ``runs/<ts>_<run>.json``.
  Writers: pipelines 1 & 2 (each writes a *unique* path). Reader: pipeline 3.
* ``<key>-context`` — the compacted ``context/digest.md`` and the ingestion
  ledger ``ingest/state.json``. Writer: pipeline 3 only. Readers: pipelines 1 & 2.

Why two stores? :meth:`MemoryStore.save` uploads the whole local root and only
re-hydrates from remote for *deserialized* stores — a ``get_or_create`` store
re-uploads the snapshot it downloaded at open time. If run records and the
digest/ledger shared one store, a pipeline-1/2 ``record_run().save()`` would
overwrite a newer digest/ledger written concurrently by pipeline 3, reverting
the ingestion state. Splitting by writer removes the cross-writer clobber: within
``-runs`` every writer touches a unique path (identical overwrites are no-ops),
and ``-context`` has a single writer.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .config import Settings
from .evals import IngestState, RunRecord

if TYPE_CHECKING:
    from flyte.ai.agents import MemoryStore

CONTEXT_PATH = "context/digest.md"
RUNS_PREFIX = "runs/"
INGEST_STATE_PATH = "ingest/state.json"


@dataclass
class MemoryFile:
    """One file in the shared-memory filesystem, with its (truncated) content."""

    store: str  # the keyed store the file lives in (e.g. "<key>-runs")
    path: str  # path within the store (e.g. "runs/<ts>_<run>.json")
    size: int  # full content length in characters (before truncation)
    content: str  # content, truncated for display


def _runs_key(settings: Settings) -> str:
    return f"{settings.memory_key}-runs"


def _context_key(settings: Settings) -> str:
    return f"{settings.memory_key}-context"


async def _open(key: str) -> "MemoryStore":
    from flyte.ai.agents import MemoryStore

    return await MemoryStore.get_or_create.aio(key=key)


async def read_shared_context(settings: Settings) -> str:
    """Return the compacted context digest for the builder/reviewer agents.

    Empty string when pipeline 3 has not produced a digest yet.
    """
    store = await _open(_context_key(settings))
    return await store.read_text.aio(CONTEXT_PATH, default="")


async def record_run(settings: Settings, record: RunRecord) -> None:
    """Append a run record to the runs store under a unique path."""
    store = await _open(_runs_key(settings))
    safe_ts = record.timestamp.replace(":", "-")
    rel = f"{RUNS_PREFIX}{safe_ts}_{record.run_id}.json"
    await store.write_json.aio(rel, record.to_dict(), actor=record.pipeline)
    await store.save.aio()


async def load_run_records_with_ids(settings: Settings) -> list[tuple[str, RunRecord]]:
    """Load every run record paired with its unique memory path (its id).

    The path (``runs/<ts>_<run>.json``) is the stable ingestion id used by
    pipeline 3 to track what has already been processed.
    """
    store = await _open(_runs_key(settings))
    out: list[tuple[str, RunRecord]] = []
    for path in store.list_paths(RUNS_PREFIX):
        if not path.endswith(".json"):
            continue
        data = await store.read_json.aio(path, default=None)
        if isinstance(data, dict):
            out.append((path, RunRecord.from_dict(data)))
    out.sort(key=lambda pair: pair[1].timestamp)
    return out


async def load_run_records(settings: Settings) -> list[RunRecord]:
    """Load every accumulated run record from shared memory."""
    return [rec for _, rec in await load_run_records_with_ids(settings)]


async def load_ingest_state(settings: Settings) -> IngestState:
    """Load pipeline 3's ingestion ledger (empty if it has never run)."""
    store = await _open(_context_key(settings))
    data = await store.read_json.aio(INGEST_STATE_PATH, default=None)
    return IngestState.from_dict(data)


async def save_ingest_state(settings: Settings, state: IngestState) -> None:
    """Persist pipeline 3's ingestion ledger (context store)."""
    store = await _open(_context_key(settings))
    await store.write_json.aio(INGEST_STATE_PATH, state.to_dict(), actor="evals")
    await store.save.aio()


async def write_context_digest(settings: Settings, digest: str) -> None:
    """Persist the compacted context digest (context store, pipeline 3 only)."""
    store = await _open(_context_key(settings))
    await store.write_text.aio(CONTEXT_PATH, digest, actor="evals")
    await store.save.aio()


async def _read_memory_file(store: "MemoryStore", key: str, path: str, max_chars: int) -> MemoryFile:
    content = await store.read_text.aio(path, default="")
    size = len(content)
    shown = content[:max_chars]
    if size > max_chars:
        shown += f"\n… [truncated {size - max_chars} chars]"
    return MemoryFile(store=key, path=path, size=size, content=shown)


async def snapshot_memory(
    settings: Settings, *, max_run_files: int = 30, max_chars: int = 2000
) -> list[MemoryFile]:
    """Snapshot the shared-memory filesystem: every file with its truncated content.

    Reads both keyed stores. To keep the snapshot bounded, only the most recent
    ``max_run_files`` run-record files are included (older ones are summarized) and
    each file's content is truncated to ``max_chars``.
    """
    out: list[MemoryFile] = []

    runs_key = _runs_key(settings)
    runs = await _open(runs_key)
    run_paths = sorted(p for p in runs.list_paths() if p.endswith(".json"))
    omitted = max(0, len(run_paths) - max_run_files)
    for path in run_paths[-max_run_files:]:
        out.append(await _read_memory_file(runs, runs_key, path, max_chars))
    if omitted:
        out.append(MemoryFile(store=runs_key, path=f"… {omitted} older run file(s) not shown", size=0, content=""))

    ctx_key = _context_key(settings)
    ctx = await _open(ctx_key)
    for path in sorted(ctx.list_paths()):
        out.append(await _read_memory_file(ctx, ctx_key, path, max_chars))

    return out
