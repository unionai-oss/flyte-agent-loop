"""Tests for the custom LLM callback (max_tokens fix)."""

import types

import litellm

from flyte_agent_loop.llm import build_call_llm


def _patch_acompletion(monkeypatch):
    captured = {}

    async def fake_acompletion(**kwargs):
        captured.update(kwargs)

        class _Msg:
            content = "hello"
            tool_calls = []

        class _Choice:
            message = _Msg()

        return types.SimpleNamespace(choices=[_Choice()])

    monkeypatch.setattr(litellm, "acompletion", fake_acompletion)
    return captured


async def test_call_llm_sets_max_tokens(monkeypatch):
    captured = _patch_acompletion(monkeypatch)
    cb = build_call_llm(32000)
    msg = await cb("claude-sonnet-4-5", "system", [{"role": "user", "content": "hi"}], None)
    assert captured["max_tokens"] == 32000  # under the model's 64000 max
    assert msg.content == "hello"
    # system prompt is prepended
    assert captured["messages"][0] == {"role": "system", "content": "system"}


async def test_call_llm_clamps_to_model_max(monkeypatch):
    captured = _patch_acompletion(monkeypatch)
    cb = build_call_llm(10_000_000)  # absurdly high
    await cb("claude-sonnet-4-5", "system", [{"role": "user", "content": "hi"}], None)
    assert captured["max_tokens"] == litellm.get_max_tokens("claude-sonnet-4-5")


async def test_call_llm_passes_tools(monkeypatch):
    captured = _patch_acompletion(monkeypatch)
    cb = build_call_llm(8000)
    tools = [{"type": "function", "function": {"name": "x"}}]
    await cb("claude-sonnet-4-5", "s", [{"role": "user", "content": "hi"}], tools)
    assert captured["tools"] == tools
    assert captured["tool_choice"] == "auto"
