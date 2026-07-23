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

    # Only the scheduled triggers are interesting here — not the ~dozen tool tasks
    # that also get deployed. Collect each deployed task's triggers with its active
    # flag, then print the activated ones (falling back to all if the platform
    # doesn't report activation state).
    triggers: list[tuple[str, str, bool]] = []
    for deployment in deployments:
        for deployed_env in deployment.envs.values():
            for task in deployed_env.deployed_entities:
                task_id = task.deployed_task.task_template.id
                for trig in getattr(task, "deployed_triggers", None) or []:
                    url = console.trigger_url(
                        project=task_id.project,
                        domain=task_id.domain,
                        task_name=task_id.name,
                        trigger_name=trig.name,
                    )
                    active = bool(getattr(getattr(trig, "spec", None), "active", False))
                    triggers.append((trig.name, url, active))

    active_triggers = [(n, u) for n, u, a in triggers if a]
    to_show = active_triggers or [(n, u) for n, u, _ in triggers]
    if not to_show:
        print("No triggers deployed." if not args.dryrun else "No triggers planned.")
    else:
        print(f"Activated {len(to_show)} trigger(s):")
        for name, url in to_show:
            print(f"  {name}  {url}")


if __name__ == "__main__":
    main()
