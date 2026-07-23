"""Runtime configuration, read from environment variables.

Everything the pipelines need to know about *which* repo to work on, *which*
model to drive the agents with, and *how* the agent identifies itself lives
here so the rest of the code stays declarative.

The GitHub token and the model provider key are injected as Flyte
:class:`flyte.Secret` s (see :mod:`flyte_agent_loop.environments`); they surface
here as ordinary environment variables at task runtime.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# Env var names. These double as the ``as_env_var`` targets of the Flyte secrets.
ENV_REPO = "FLYTE_AGENT_REPO"
ENV_GITHUB_TOKEN = "GITHUB_TOKEN"
ENV_MODEL = "FLYTE_AGENT_MODEL"
ENV_AGENT_ID = "FLYTE_AGENT_ID"
ENV_DIBS_TTL_MINUTES = "FLYTE_AGENT_DIBS_TTL_MINUTES"
ENV_MEMORY_KEY = "FLYTE_AGENT_MEMORY_KEY"
ENV_GITHUB_API = "GITHUB_API_URL"
ENV_MAX_TOKENS = "FLYTE_AGENT_MAX_TOKENS"
ENV_MAX_TRIES = "FLYTE_AGENT_MAX_TRIES"

# Default model. litellm routes this to Anthropic when ANTHROPIC_API_KEY is set.
DEFAULT_MODEL = "claude-sonnet-4-5"
DEFAULT_AGENT_ID = "flyte-agent-loop"
DEFAULT_DIBS_TTL_MINUTES = 30
DEFAULT_MEMORY_KEY = "agent-loop-shared-v0"
DEFAULT_GITHUB_API = "https://api.github.com"
# Max output tokens per LLM call. The builder emits full file contents inline, so
# this must be generous — litellm otherwise defaults to only 4096, truncating the
# plan into invalid JSON. Clamped to the model's real max at call time.
DEFAULT_MAX_TOKENS = 32000
# How many build->verify attempts the issue builder gets to satisfy the verifier.
DEFAULT_MAX_TRIES = 3


@dataclass(frozen=True)
class Settings:
    """Resolved runtime settings for a single pipeline invocation."""

    repo: str
    """Target repository as ``owner/name`` (e.g. ``"unionai/flyte"``)."""

    github_token: str
    model: str
    agent_id: str
    dibs_ttl_minutes: int
    memory_key: str
    github_api_url: str
    max_tokens: int
    max_tries: int

    @property
    def owner(self) -> str:
        return self.repo.split("/", 1)[0]

    @property
    def name(self) -> str:
        return self.repo.split("/", 1)[1]


def load_settings() -> Settings:
    """Read :class:`Settings` from the environment.

    Raises:
        RuntimeError: if a required variable (repo, token) is missing.
    """
    repo = os.environ.get(ENV_REPO, "").strip()
    if not repo or "/" not in repo:
        raise RuntimeError(
            f"{ENV_REPO} must be set to a 'owner/name' repository slug, got {repo!r}"
        )
    token = os.environ.get(ENV_GITHUB_TOKEN, "").strip()
    if not token:
        raise RuntimeError(f"{ENV_GITHUB_TOKEN} must be set (inject it as a Flyte secret)")

    return Settings(
        repo=repo,
        github_token=token,
        model=os.environ.get(ENV_MODEL, DEFAULT_MODEL).strip() or DEFAULT_MODEL,
        agent_id=os.environ.get(ENV_AGENT_ID, DEFAULT_AGENT_ID).strip() or DEFAULT_AGENT_ID,
        dibs_ttl_minutes=int(os.environ.get(ENV_DIBS_TTL_MINUTES, DEFAULT_DIBS_TTL_MINUTES)),
        memory_key=os.environ.get(ENV_MEMORY_KEY, DEFAULT_MEMORY_KEY).strip() or DEFAULT_MEMORY_KEY,
        github_api_url=os.environ.get(ENV_GITHUB_API, DEFAULT_GITHUB_API).rstrip("/"),
        max_tokens=int(os.environ.get(ENV_MAX_TOKENS, DEFAULT_MAX_TOKENS)),
        max_tries=max(1, int(os.environ.get(ENV_MAX_TRIES, DEFAULT_MAX_TRIES))),
    )
