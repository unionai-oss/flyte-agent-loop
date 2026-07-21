"""Run a single pipeline once against a real repo, locally.

This is the fastest way to smoke-test the agents end-to-end without waiting for a
cron trigger. It executes the task locally (agents call the real GitHub + model
APIs), so make sure the environment is configured first:

    export FLYTE_AGENT_REPO=your-org/your-sandbox-repo   # use a throwaway repo!
    export GITHUB_TOKEN=ghp_...                          # repo + PR scopes
    export ANTHROPIC_API_KEY=sk-ant-...
    export FLYTE_AGENT_MODEL=claude-sonnet-4-5           # optional

    python examples/run_local.py issue_to_pr
    python examples/run_local.py pr_review
    python examples/run_local.py evals

Because these agents open PRs and post comments, point FLYTE_AGENT_REPO at a
repository you own and don't mind being written to.
"""

from __future__ import annotations

import sys

import flyte

from flyte_agent_loop.pipeline_evals import evals
from flyte_agent_loop.pipeline_issue_to_pr import issue_to_pr
from flyte_agent_loop.pipeline_pr_review import pr_review

PIPELINES = {"issue_to_pr": issue_to_pr, "pr_review": pr_review, "evals": evals}


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in PIPELINES:
        print(f"usage: python examples/run_local.py [{'|'.join(PIPELINES)}]")
        raise SystemExit(2)

    # Run everything on the local machine instead of a remote cluster.
    flyte.init()
    result = flyte.run(PIPELINES[sys.argv[1]])
    print("result:", result.outputs())


if __name__ == "__main__":
    main()
