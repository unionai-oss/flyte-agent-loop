"""Tests for report styling + tab renaming + shared-memory rendering."""

from flyte_agent_loop.report_style import _md_to_html, _rename_tabs


class _Tab:
    def __init__(self, name, content=None):
        self.name = name
        self.content = content or []


class _Report:
    def __init__(self, tabs):
        self.tabs = tabs


def test_rename_tabs_main_and_agent_preserving_order():
    report = _Report({"main": _Tab("main"), "Agent": _Tab("Agent"), "Evals": _Tab("Evals")})
    _rename_tabs(report)
    assert list(report.tabs.keys()) == ["Result", "Agent Traces", "Evals"]
    assert report.tabs["Result"].name == "Result"
    assert report.tabs["Agent Traces"].name == "Agent Traces"
    assert report.tabs["Evals"].name == "Evals"  # untouched


def test_md_to_html_headings_and_lists():
    md = "# Title\n\nintro line\n\n## Section\n- [PASS] issue #1: good\n- [FAIL] issue #2: bad"
    html = _md_to_html(md)
    assert "<h2>Title</h2>" in html
    assert "<h3>Section</h3>" in html
    assert "<ul>" in html and "</ul>" in html
    assert "<li>" in html
    assert "<p>intro line</p>" in html
    # PASS/FAIL get colorized
    assert "color:#16a34a" in html and "[PASS]" in html
    assert "color:#dc2626" in html and "[FAIL]" in html


def test_md_to_html_escapes_html():
    assert "&lt;script&gt;" in _md_to_html("- <script>alert(1)</script>")


def test_link_opens_new_tab_and_escapes():
    from flyte_agent_loop.report_style import link

    html = link("https://github.com/o/r/issues/5", "#5")
    assert 'href="https://github.com/o/r/issues/5"' in html
    assert 'target="_blank"' in html
    assert 'rel="noopener noreferrer"' in html
    assert ">#5</a>" in html
    # no url -> plain escaped text, no anchor
    assert link("", "#5") == "#5"
    assert "<a" not in link("", "<x>")


def test_install_live_report_flush_registers_agent_callback():
    import asyncio

    from flyte.ai.agents import agent_progress_cb

    from flyte_agent_loop.report_style import install_live_report_flush

    try:
        install_live_report_flush()
        cb = agent_progress_cb.get()
        assert cb is not None and callable(cb)
        # Invoking it with an event (no active task report) is a safe no-op flush.
        asyncio.run(cb({"type": "tool_start", "data": {}}))
    finally:
        agent_progress_cb.set(None)  # don't leak into other tests
