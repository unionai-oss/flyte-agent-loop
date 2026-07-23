"""flyte-agent-loop — a loop-engineering system built on Flyte 2.

Three scheduled agent pipelines cooperate over a shared, durable
:class:`flyte.ai.agents.MemoryStore`:

* :mod:`flyte_agent_loop.builder_agent` — every 5 minutes: claim an open
  GitHub issue (via a "dibs" comment), implement it with tests/examples/docs,
  have a verifier sub-agent check the work, then open a PR.
* :mod:`flyte_agent_loop.reviewer_agent` — every 5 minutes: claim an open
  agent-authored PR, address its review comments, verify the fixes, push them,
  and release the claim.
* :mod:`flyte_agent_loop.distiller_agent` — every 10 minutes: compact the run
  records from the first two pipelines into shared memory and publish an
  evaluation report; that memory is fed back as context to pipelines 1 and 2.
"""

from ._version import __version__

__all__ = ["__version__"]
