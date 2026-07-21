# flyte-agent-loop

A minimal but fully functional **loop-engineering** system built on
[Flyte 2](https://www.union.ai/docs/v2/flyte/). Three scheduled
`flyte.ai.agents.Agent` pipelines cooperate over a shared, durable memory to
autonomously take GitHub issues all the way to reviewed pull requests — and then
grade themselves.

| Pipeline | Cadence | What it does |
| --- | --- | --- |
| `issue_to_pr` | every **5 min** | Claims an open issue (via a "dibs" comment), implements it with tests/examples/docs, has a **verifier sub-agent** check the work, then opens a PR. |
| `pr_review` | every **15 min** | Claims an open agent PR, reads its review comments, makes + verifies fixes, pushes them, and releases the claim for follow-ups. |
| `evals` | every **10 min** | Compacts the run history into Flyte **Memory**, computes success-rate/evals as a Flyte **report**, and feeds the digest back as context to pipelines 1 & 2. |

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
  records and a compacted context digest. The evals pipeline recompacts it every
  10 minutes; the builder/reviewer agents read it as context so they learn from
  past verifier feedback.

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
  pipeline_issue_to_pr.py   # Pipeline 1 (cron */5)
  pipeline_pr_review.py     # Pipeline 2 (cron */15)
  pipeline_evals.py         # Pipeline 3 (cron */10)
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

Create the two secrets in your Flyte/Union project (names must match
[`environments.py`](src/flyte_agent_loop/environments.py)):

```bash
flyte create secret github-token       --value ghp_xxx     # repo + PR scopes
flyte create secret anthropic-api-key  --value sk-ant-xxx
```

## Run

Locally, once (agents hit the real GitHub + model APIs — use a throwaway repo):

```bash
python examples/run_local.py issue_to_pr
python examples/run_local.py pr_review
python examples/run_local.py evals
```

Deploy with the cron triggers active:

```bash
python -m flyte_agent_loop.deploy            # deploy + activate schedules
python -m flyte_agent_loop.deploy --dryrun   # plan only
python -m flyte_agent_loop.deploy --run evals  # ad-hoc single run
```

## Test

```bash
pytest -q
```

The suite is fully hermetic: the claim state machine, evals, and agent-output
parsers are pure functions, and the GitHub client is exercised against an
in-memory `httpx.MockTransport`. No cluster, network, or LLM key required.
