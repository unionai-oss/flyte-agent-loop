"""Deploy the agent-loop environment (and its three scheduled triggers).

Usage::

    # configure the target repo + model (secrets are configured separately)
    export FLYTE_AGENT_REPO=your-org/your-repo
    export FLYTE_AGENT_MODEL=claude-sonnet-4-5

    python -m flyte_agent_loop.deploy            # deploy with triggers active
    python -m flyte_agent_loop.deploy --dryrun   # plan only, don't apply
    python -m flyte_agent_loop.deploy --run issue_to_pr   # run one pipeline now

Deploying registers the tasks and activates their cron triggers:

* ``issue_to_pr`` — every 5 minutes
* ``pr_review``   — every 15 minutes
* ``evals``       — every 10 minutes

Secrets ``github-token`` and ``anthropic-api-key`` must exist in your Flyte /
Union project before the scheduled runs will succeed.
"""

from __future__ import annotations

import argparse

import flyte

from .environments import env

# Importing the pipeline modules registers their tasks (and triggers) on ``env``.
from .pipeline_evals import evals
from .pipeline_issue_to_pr import issue_to_pr
from .pipeline_pr_review import pr_review

PIPELINES = {
    "issue_to_pr": issue_to_pr,
    "pr_review": pr_review,
    "evals": evals,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy or run the flyte-agent-loop pipelines.")
    parser.add_argument("--dryrun", action="store_true", help="Plan the deploy without applying it.")
    parser.add_argument(
        "--run",
        choices=sorted(PIPELINES),
        help="Run a single pipeline once (ad hoc) instead of deploying.",
    )
    args = parser.parse_args()

    flyte.init_from_config()

    if args.run:
        run = flyte.run(PIPELINES[args.run])
        print(f"Launched {args.run}: {run.url}")
        return

    deployments = flyte.deploy(env, dryrun=args.dryrun)
    for dep in deployments:
        print(f"{'Planned' if args.dryrun else 'Deployed'}: {dep}")
    print(
        "\nScheduled triggers:\n"
        "  issue_to_pr  every 5 minutes\n"
        "  pr_review    every 15 minutes\n"
        "  evals        every 10 minutes"
    )


if __name__ == "__main__":
    main()
