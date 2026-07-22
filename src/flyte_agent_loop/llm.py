"""Custom LLM callback for the agents.

The Flyte default callback (``flyte.ai.agents._llm._default_call_llm``) does not
set ``max_tokens``, so litellm falls back to its ``DEFAULT_MAX_TOKENS`` of 4096.
The builder/reviewer agents emit an entire implementation (full file contents) as
a single JSON object, which routinely exceeds 4096 output tokens — the model then
truncates it into invalid JSON, ``parse_plan`` fails, and the pipeline silently
falls into the ``no_work`` branch (no verification, no PR).

This callback mirrors the default but sets a generous ``max_tokens`` (clamped to
the model's real maximum), fixing the truncation.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from flyte.ai.agents import LLMCallable, LLMMessage


def build_call_llm(max_tokens: int) -> LLMCallable:
    """Return an async ``(model, system, messages, tools) -> LLMMessage`` callback
    that requests up to ``max_tokens`` output tokens (clamped to the model max)."""

    async def call_llm(
        model: str,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]] | None,
    ) -> LLMMessage:
        import litellm
        from litellm import acompletion

        effective = max_tokens
        try:
            model_max = litellm.get_max_tokens(model)
            if isinstance(model_max, int) and model_max > 0:
                effective = min(max_tokens, model_max)
        except Exception:
            pass  # unknown model: use the configured value as-is

        full_messages = [{"role": "system", "content": system}, *messages]
        kwargs: dict[str, Any] = {"model": model, "messages": full_messages, "max_tokens": effective}
        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = "auto"

        response = await acompletion(**kwargs)
        choice = response.choices[0]  # type: ignore[index]
        msg = choice.message

        tool_calls: list[dict[str, Any]] = []
        for call in getattr(msg, "tool_calls", None) or []:
            try:
                args_str = call.function.arguments
                args = json.loads(args_str) if isinstance(args_str, str) else (args_str or {})
            except json.JSONDecodeError:
                args = {"_raw": call.function.arguments}
            tool_calls.append(
                {
                    "id": getattr(call, "id", None) or f"call_{uuid.uuid4().hex[:12]}",
                    "name": call.function.name,
                    "arguments": args,
                }
            )
        return LLMMessage(content=getattr(msg, "content", None) or "", tool_calls=tool_calls, raw=response)

    return call_llm
