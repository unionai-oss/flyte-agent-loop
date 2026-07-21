# Architecture

`flyte-agent-loop` is a **loop-engineering** system: three scheduled Flyte 2
agent pipelines that cooperate over a shared, durable memory to autonomously move
GitHub issues to merged code.

```
                       ┌─────────────────────────────────────────────┐
                       │            shared MemoryStore                │
                       │  context/digest.md  runs/<ts>_<run>.json     │
                       │  ingest/state.json  (processed-record ledger)│
                       └───────▲───────────────────────▲──────┬───────┘
              reads context    │                       │      │ writes records
                       ┌───────┴───────┐       ┌───────┴──────┴┐
   every 5 min ──────► │ 1. issue_to_pr│       │ 2. pr_review  │ ◄────── every 15 min
                       └───────┬───────┘       └───────┬───────┘
                               │ opens PR              │ pushes fixes
                               ▼                       ▼
                       ┌───────────────────────────────────────┐
                       │              GitHub repo               │
                       └───────────────────────────────────────┘
                               ▲
   every 10 min ──────► ┌──────┴────────┐
                        │  3. evals      │ reads records → metrics + report,
                        └───────────────┘ recompacts context digest
```

## Pipelines

Each pipeline is a single `@env.task(report=True, triggers=[...])` on one shared
`flyte.TaskEnvironment` (`environments.py`). The cron cadence is set via
`flyte.Trigger(name=..., automation=flyte.Cron("*/N * * * *"))`.

### 1. `issue_to_pr` — every 5 minutes (`pipeline_issue_to_pr.py`)

1. **Dibs.** List open issues; for the first one with no active claim, post a
   dibs marker comment (`try_claim`). Concurrent/future runs see the marker and
   skip it until it expires (`FLYTE_AGENT_DIBS_TTL_MINUTES`) or is released.
2. **Build.** A `flyte.ai.agents.Agent` (read-only GitHub tools + the shared
   context digest) designs the change — implementation, **tests, example, and
   docs** — and returns a JSON change plan.
3. **Verify.** A stricter verifier sub-agent checks the plan for correctness and
   completeness, returning a structured `{"verified": bool, "notes": ...}`.
4. **Create PR.** Only if verified, the pipeline opens a PR
   (`open_pr_with_changes`, a durable tool task). If not verified, it posts the
   verifier feedback to the issue and releases the claim for a retry.
5. **Record.** Append a `RunRecord` to shared memory.

### 2. `pr_review` — every 15 minutes (`pipeline_pr_review.py`)

1. **Dibs.** Find open PRs authored by the agent's GitHub user; claim the first
   unclaimed one so prior/parallel runs know it's being worked.
2. **Read comments.** A reviewer agent reads the PR and *all* its comments.
3. **Fix.** It designs tightly-scoped fixes as a JSON plan.
4. **Verify.** The verifier confirms the fixes are **aligned with the comments
   AND correct**.
5. **Update + release.** Only if verified, push the fixes to the PR head branch
   (`push_changes_to_pr`) and **release the dibs** so a later run can pick up any
   additional follow-up comments.
6. **Record.** Append a `RunRecord`.

### 3. `evals` — every 10 minutes (`pipeline_evals.py`)

1. Load every `RunRecord` **with its unique memory-path id**, plus the ingestion
   ledger (`ingest/state.json`).
2. **Ingest only new records.** `select_new_records` filters out any record id
   already in the ledger; `ingest_new_records` folds the rest into a per-target
   (`issue:<n>` / `pr:<n>`) rollup and appends their ids to the processed set,
   then the ledger is saved. This is idempotent — a record whose id is already
   processed is never counted twice, so previously ingested issues/PRs are never
   double-ingested across the every-10-minute fires.
3. Compute success rate, verification rate, error rate, PRs opened, fixes pushed
   over the full history (`evals.evaluate` — a fresh aggregate, not ingestion).
4. **Recompact** into `context/digest.md`: headline metrics + recent verifier
   lessons + the ingested issue/PR rollup ("already processed — do not
   re-litigate"). This is the context fed back into pipelines 1 & 2.
5. Publish the metrics as a Flyte **report** tab.

**Why a ledger and not just recomputed aggregates?** Metrics are safe to
recompute each run, but the *ingestion* of issue/PR content into the per-target
rollup must be incremental — otherwise every fire would re-summarize the same
already-seen records. The `processed_record_ids` set (keyed by the immutable
`runs/<ts>_<run>.json` path) is the dedup boundary; `ingest_count` on each target
reflects only genuinely new records touching that issue/PR.

## Dibs (cooperative locking)

Implemented in `dibs.py` as a pure state machine over comment markers — an
invisible HTML comment:

```
<!-- flyte-agent-loop:dibs v1 op=claim kind=issue agent=<id> run=<run> until=<iso8601> -->
```

`active_claim` walks the markers for a kind (issue/pr); the latest one wins. A
`release` marker or an expired `until` frees the target. Claims are re-entrant
for their owning agent. Because the logic is pure (comments + an explicit `now`),
it is fully unit tested without any network.

## Shared memory

**Two** keyed `flyte.ai.agents.MemoryStore` s with disjoint writer sets
(`memory_context.py`):

- `<key>-runs` — one file per run at `runs/<ts>_<run>.json`. Written by pipelines
  1 & 2 (each to a unique path), read by pipeline 3.
- `<key>-context` — `context/digest.md` + `ingest/state.json`. Written only by
  pipeline 3, read by pipelines 1 & 2.

The split matters: `MemoryStore.save()` uploads the whole local root and only
re-hydrates from remote for *deserialized* stores, so a `get_or_create` store
re-uploads the snapshot it downloaded at open time. If run records and the
digest/ledger shared one store, a pipeline-1/2 `record_run().save()` could
overwrite a newer digest/ledger written concurrently by pipeline 3 — reverting
the ingestion state. With two stores, `-runs` writers only ever touch unique
paths (identical re-uploads are no-ops) and `-context` has a single writer.

## Known limitations / trade-offs

These are deliberate simplifications for a minimal system, called out so they are
not mistaken for guarantees:

- **Dibs is best-effort, not a mutex.** Two runs that read an unclaimed
  issue/PR in the same instant can both post a claim (a TOCTOU window). The TTL
  + agent id make the collision visible and self-healing; they don't prevent it.
- **Pipeline 3 overlap.** `evals` is not itself protected by dibs. If one run
  exceeds its 10-minute cadence, an overlapping run can last-writer-win on the
  `-context` store. Ingestion is id-keyed so records aren't double-counted within
  a run, but a lost `state.json` update would let the next run re-ingest.
- **First page only.** The client fetches one page of issues/PRs/comments
  (50/100). A dibs marker beyond 100 comments won't be seen (→ possible re-claim).
- **Unbounded growth.** `runs/` files and `processed_record_ids` accumulate; a
  compaction/retention step would be needed for long-lived deployments.
- **Blocking I/O.** The GitHub client is synchronous `httpx`; calls block the
  task's event loop. Fine for these single-flight tasks, not for high fan-out.
- **No GitHub retry/backoff.** Tasks run with `retries=0`; transient 5xx / rate
  limits fail the run (the next scheduled fire retries the work).

## Testing strategy

All non-trivial logic is isolated into pure, hermetic modules so it can be tested
without a cluster, a network, or an LLM:

- `dibs.py` — claim state machine (`tests/test_dibs.py`)
- `evals.py` — metrics + context compaction (`tests/test_evals.py`)
- `agents.py` — plan/verdict parsers (`tests/test_agents.py`)
- `github_client.py` — exercised against an in-memory `httpx.MockTransport`
  (`tests/test_github_client.py`)

The Flyte task wrappers (`tools.py`, the pipelines) are thin orchestration over
these tested units.
