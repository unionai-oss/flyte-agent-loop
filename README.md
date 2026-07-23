# flyte-agent-loop

A minimal but fully functional **loop-engineering** system built on
[Flyte 2](https://www.union.ai/docs/v2/flyte/). Three scheduled
`flyte.ai.agents.Agent` pipelines cooperate over a shared, durable memory to
autonomously take GitHub issues all the way to reviewed pull requests — and then
grade themselves.

| Pipeline | Cadence | What it does |
| --- | --- | --- |
| `builder` | every **5 min** | Claims an open issue (via a "dibs" comment), implements it with tests/examples/docs, has a **verifier sub-agent** check the work, then opens a PR. |
| `reviewer` | every **5 min** | Claims an open agent PR, reads its review comments, makes + verifies fixes, pushes them, and releases the claim for follow-ups. |
| `distiller` | every **10 min** | Uses a **distiller Agent** to dedupe + consolidate the builder's and reviewer's run history into a compact, high-signal "lessons" memory (fed back to them as context), and publishes success-rate metrics, the memory filesystem, and per-run reasoning traces as a Flyte **report**. |

See [`docs/architecture.md`](docs/architecture.md) for the full design.

## How it works

- **Dibs** — before working an issue/PR, a run posts an (invisible) HTML-comment
  marker. Future scheduled runs parse the markers and skip anything with an
  active claim, so overlapping cron fires never double-work. Claims expire and
  can be released. Logic lives in [`dibs.py`](src/flyte_agent_loop/dibs.py) and
  is pure + unit tested.
- **Verifier sub-agent** — the builder/reviewer agents only *propose* a change
  plan; a separate strict verifier agent checks correctness & completeness, and
  the PR is opened / fixes are pushed **only if it passes**.
- **Shared memory** — one keyed `flyte.ai.agents.MemoryStore` holds per-run
  records and a consolidated "lessons" digest. Every 10 minutes the `distiller`
  pipeline runs a **distiller Agent** that dedupes and consolidates the newest runs
  into that digest (as much signal as possible per token); the builder/reviewer
  agents read it as context so they learn from past verifier feedback.

## Layout

```
src/flyte_agent_loop/
  config.py              # env-var settings
  dibs.py                # pure cooperative-claim state machine
  github_client.py       # small GitHub REST client (dibs + issues/PRs/commits)
  evals.py               # pure metrics + context compaction
  memory_context.py      # MemoryStore read/write helpers
  environments.py        # shared Image, secrets, TaskEnvironment
  tools.py               # @env.task GitHub tools handed to the agents
  agents.py              # builder / reviewer / verifier agent factories + parsers
  pipeline_builder.py   # Pipeline 1 (cron */5)
  pipeline_reviewer.py     # Pipeline 2 (cron */5)
  pipeline_distiller.py         # Pipeline 3 (cron */10)
  deploy.py              # deploy env + triggers, or run one pipeline
tests/                   # hermetic pytest suite (no cluster/network/LLM needed)
examples/run_local.py    # run one pipeline once, locally
```

## Setup

```bash
uv venv --python 3.13 && source .venv/bin/activate
uv pip install -e ".[dev]"
```

Configure the target repo and model (see [`.env.example`](.env.example)):

```bash
export FLYTE_AGENT_REPO=your-org/your-sandbox-repo   # a repo you own!
export FLYTE_AGENT_MODEL=claude-sonnet-4-5
```

### Create a GitHub token

The agent reads and writes the target repo through a GitHub **fine-grained
personal access token (PAT)**. Create one scoped to *only* the repo in
`FLYTE_AGENT_REPO`:

1. GitHub → **Settings → Developer settings → Personal access tokens →
   Fine-grained tokens → Generate new token**
   (or go straight to <https://github.com/settings/personal-access-tokens/new>).
2. **Token name** — e.g. `flyte-agent-loop`; set an **Expiration**.
3. **Resource owner** — the user or org that owns the target repo.
4. **Repository access** → **Only select repositories** → pick the repo you want
   the agent to work on (the one in `FLYTE_AGENT_REPO`).
5. **Permissions → Repository permissions** — set each of these to
   **Read and write**:

   | Permission | Access | Why the agent needs it |
   | --- | --- | --- |
   | **Contents** | Read and write | read repo files, create branches, commit changes |
   | **Pull requests** | Read and write | open/update PRs, read + post PR comments |
   | **Issues** | Read and write | read issues, post the "dibs" claim comments |
   | **Discussions** | Read and write | read/participate in repo discussions |

   (**Metadata: Read-only** is selected automatically and is required.)
6. **Generate token** and copy the `github_pat_...` value — you won't see it again.

> If you use a classic PAT instead, grant the `repo` scope (full control of
> private repositories). Fine-grained tokens are strongly preferred because they
> can be limited to the single target repo.


## Run on a local Flyte Devbox

The [Flyte Devbox](https://www.union.ai/docs/v2/flyte/user-guide/run-modes/running-devbox/)
is a full single-node Flyte cluster that runs locally in Docker — tasks execute
in containers with schedules and reports, just like a remote cluster, but with
nothing to provision. It's the easiest way to exercise the scheduled pipelines
end-to-end on your machine.

**Prerequisites:** Docker running (and `kubectl` if you want to inspect the
cluster). The `flyte` CLI ships with the `flyte` package installed above.

1. **Start the devbox** (first run pulls the image; UI at
   <http://localhost:30080/v2>, image registry at `localhost:30000`):

   ```bash
   flyte start devbox
   ```

2. **Point the CLI/SDK at it.** Generate a config for the local cluster (writes
   `./config.yaml`, which `flyte` and `flyte.init_from_config()` auto-discover):

   ```bash
   flyte create config \
     --endpoint localhost:30080 --insecure --builder local \
     --project flytesnacks --domain development \
     --registry localhost:30000
   ```

   (It's gitignored. Use `-o ~/.flyte` to write `~/.flyte/config.yaml` instead.)

3. **Add the secrets** to the devbox cluster (same names as before; see
   [Create a GitHub token](#create-a-github-token)):

   ```bash
   flyte create secret --project flytesnacks --domain development github-token       --value github_pat_xxx
   flyte create secret --project flytesnacks --domain development anthropic-api-key  --value sk-ant-xxx
   ```

4. **Deploy the pipelines** (uses `~/.flyte/config.yaml` via
   `flyte.init_from_config()`); the local image builder builds into the devbox
   registry:

   ```bash
   export FLYTE_AGENT_REPO=your-org/your-sandbox-repo
   python -m flyte_agent_loop.deploy            # activate schedules on the devbox
   python -m flyte_agent_loop.deploy --run builder  # or trigger one run now {builder, reviewer, distiller}
   ```

   Watch executions and reports at <http://localhost:30080/v2>.

5. **Pause / tear down** when done:

   ```bash
   flyte stop devbox              # pause (keeps state; resume with `flyte start devbox`)
   flyte delete devbox --volume   # remove the container and its storage volume
   ```

> The manual integration test can also target the devbox instead of the demo
> cluster — point `tests/integration/config.yaml` at `endpoint: dns:///localhost:30080`
> with `builder: local`.

## Stop the agents (deactivate all triggers)

Deploying activates three cron triggers. To stop the agents from firing without
un-deploying, **deactivate** each trigger with `flyte update trigger <trigger-name>
<task-name> --deactivate`:

```bash
flyte update trigger builder_every_5m  flyte_agent_loop.builder --deactivate -p flytesnacks -d development
flyte update trigger reviewer_every_5m    flyte_agent_loop.reviewer   --deactivate -p flytesnacks -d development
flyte update trigger distiller_every_10m       flyte_agent_loop.distiller       --deactivate -p flytesnacks -d development
```

Use the same `--config`/endpoint (and `-p`/`-d`) you deployed with. Verify with
`flyte get trigger -p flytesnacks -d development`.

- **Reactivate** later: rerun the commands with `--activate` (or just redeploy —
  `python -m flyte_agent_loop.deploy` re-activates them, since the triggers default
  to `auto_activate=True`).
- **Remove** entirely: `flyte delete trigger <trigger-name> <task-name> -p … -d …`.

> Deactivating stops the schedules but leaves the tasks deployed, so any run
> already in flight finishes and ad-hoc `--run` invocations still work.

## Test

```bash
pytest -q                 # unit suite (scoped to tests/unit via pyproject)
```

`tests/unit` is fully hermetic: the claim state machine, evals, and agent-output
parsers are pure functions, and the GitHub client is exercised against an
in-memory `httpx.MockTransport`. No cluster, network, or LLM key required. This
is what CI runs (`.github/workflows/unit-tests.yml`, Python 3.11–3.13).

`tests/integration` runs against a live Union tenant and is **manual** (skipped
unless `RUN_INTEGRATION=1`). It's intended for Union employees running against an
actual tenant — [`tests/integration/config.yaml`](tests/integration/config.yaml)
points at the `demo` tenant with
[device-flow auth](https://www.union.ai/docs/v2/union/user-guide/authenticating/#device-flow)
(`admin.authType: DeviceFlow`), so the first run opens a browser to authenticate:

```bash
RUN_INTEGRATION=1 pytest tests/integration -m integration -s
```

See [`tests/integration/README.md`](tests/integration/README.md) for details.
