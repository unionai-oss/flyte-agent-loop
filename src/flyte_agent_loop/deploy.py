"""Deploy the agent-loop environment (and its three scheduled triggers).

Usage::

    # configure the target repo + model (secrets are configured separately)
    export FLYTE_AGENT_REPO=your-org/your-repo
    export FLYTE_AGENT_MODEL=claude-sonnet-4-5

    python -m flyte_agent_loop.deploy            # deploy with triggers active
    python -m flyte_agent_loop.deploy --dryrun   # plan only, don't apply
    python -m flyte_agent_loop.deploy --run builder   # run one pipeline now

Deploying registers the tasks and activates their cron triggers:

* ``builder`` — every 5 minutes
* ``reviewer``   — every 5 minutes
* ``distiller``       — every 10 minutes

Secrets ``github-token`` and ``anthropic-api-key`` must exist in your Flyte /
Union project before the scheduled runs will succeed.
"""

from __future__ import annotations

import argparse
import logging

import flyte

from .environments import env

# Importing the pipeline modules registers their tasks (and triggers) on ``env``.
from .distiller_agent import distiller
from .builder_agent import builder
from .reviewer_agent import reviewer

PIPELINES = {
    "builder": builder,
    "reviewer": reviewer,
    "distiller": distiller,
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

    flyte.init_from_config(log_level=logging.INFO)

    if args.run:
        run = flyte.run(PIPELINES[args.run])
        print(f"Launched {args.run}: {run.url}")
        return

    deployments = flyte.deploy(env, dryrun=args.dryrun)

    from flyte._initialize import get_client

    console = get_client().console
    for deployment in deployments:
        for deployed_env in deployment.envs.values():
            for task in deployed_env.deployed_entities:
                task_id = task.deployed_task.task_template.id
                url = console.task_url(
                    project=task_id.project, domain=task_id.domain, task_name=task_id.name
                )
                print(f"{task_id.name}  {url}")


if __name__ == "__main__":
    main()
