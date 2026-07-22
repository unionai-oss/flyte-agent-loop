"""Report presentation: prettified CSS + tab renaming, applied at flush time.

Flyte renders the task report from a fixed template, but two things are still in
our control:

* a ``<style>`` block injected into a tab's content applies to the whole report
  document (it's not scoped to the tab div), so we can override the template CSS;
* nav labels come from the ``report.tabs`` dict keys, so re-keying the dict
  renames tabs — ``main`` -> ``Result`` and the agent harness's ``Agent`` tab ->
  ``Agent Traces``.

Call :func:`finalize_report` in place of ``flyte.report.flush`` once, at the end
of a task, after all logging is done.
"""

from __future__ import annotations

import html as _html

import flyte
import flyte.report

_STYLE_ID = "flyte-agent-loop-style"
_MEMORY_TAB = "Shared Memory"

# Nav-tab renames, keyed by the tab's current dict key.
_TAB_RENAMES = {"main": "Result", "Agent": "Agent Traces"}

_CSS = """
:root {
  --accent: #7c3aed; --accent-soft: #f3e8ff; --border: #e6e8eb;
  --muted: #6b7280; --fg: #1f2328;
}
body {
  margin: 0; background: #f6f7f9; color: var(--fg); font-size: 14px; line-height: 1.55;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
}
#flyte-frame-nav {
  background: #fff; border-bottom: 1px solid var(--border);
  padding: 4px 16px; position: sticky; top: 0; z-index: 5;
}
#flyte-frame-tabs { justify-content: flex-start; gap: 4px; }
#flyte-frame-tabs li {
  width: auto; min-width: 92px; color: var(--muted); border-radius: 8px 8px 0 0;
  border-bottom: 3px solid transparent; transition: color .15s, background .15s, border-color .15s;
}
#flyte-frame-tabs li:hover { color: var(--fg); background: var(--accent-soft); }
#flyte-frame-tabs li.active { color: var(--accent); border-bottom: 3px solid var(--accent); }
#flyte-frame-container > div.active { max-width: 980px; margin: 0 auto; padding: 24px 28px; }
h2 { font-size: 20px; font-weight: 700; margin: 0 0 14px; letter-spacing: -0.01em; }
h3 { font-size: 15px; font-weight: 600; color: #374151; margin: 14px 0 6px; }
p { margin: 6px 0; }
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
code {
  background: #eef0f3; padding: 1px 6px; border-radius: 6px; font-size: 12.5px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}
ul { padding-left: 18px; }
table {
  width: 100%; border: 1px solid var(--border); border-radius: 10px; overflow: hidden;
  font-size: 13px; margin: 10px 0; background: #fff;
}
th, td { border: none; border-bottom: 1px solid var(--border); padding: 9px 12px; text-align: left; vertical-align: top; }
tr:first-child th, thead th { background: #f9fafb; color: #374151; font-weight: 600; white-space: nowrap; }
tr:nth-child(even) { background: #fbfbfc; }
tr:last-child td { border-bottom: none; }
details pre { background: #f6f7f9; border: 1px solid var(--border); border-radius: 8px; padding: 8px; }
"""

_REPORT_CSS = f'<style id="{_STYLE_ID}">{_CSS}</style>'


def _inject_css(report) -> None:
    tab = report.tabs.get("main") or next(iter(report.tabs.values()), None)
    if tab is None:
        return
    if not any(_STYLE_ID in c for c in tab.content):
        tab.content.insert(0, _REPORT_CSS)


def _rename_tabs(report) -> None:
    renamed = {}
    for key, tab in report.tabs.items():
        new_key = _TAB_RENAMES.get(key, key)
        tab.name = new_key
        renamed[new_key] = tab
    report.tabs = renamed


async def finalize_report() -> None:
    """Apply the prettified CSS + tab renames, then flush the report (best-effort)."""
    try:
        report = flyte.report.current_report()
        _inject_css(report)
        _rename_tabs(report)
    except Exception:
        flyte.logger.warning("failed to finalize report styling")
    await flyte.report.flush.aio()


# ---------------------------------------------------------------------------
# Shared-memory context tab
# ---------------------------------------------------------------------------
def _inline_md(text: str) -> str:
    s = _html.escape(text)
    s = s.replace("[PASS]", '<span style="color:#16a34a;font-weight:600">[PASS]</span>')
    s = s.replace("[FAIL]", '<span style="color:#dc2626;font-weight:600">[FAIL]</span>')
    return s


def _md_to_html(md: str) -> str:
    """Render the small markdown subset used by the memory digest (headings/lists)."""
    out: list[str] = []
    in_list = False

    def _close() -> None:
        nonlocal in_list
        if in_list:
            out.append("</ul>")
            in_list = False

    for raw in md.splitlines():
        line = raw.rstrip()
        if not line.strip():
            _close()
        elif line.startswith("## "):
            _close()
            out.append(f"<h3>{_html.escape(line[3:])}</h3>")
        elif line.startswith("# "):
            _close()
            out.append(f"<h2>{_html.escape(line[2:])}</h2>")
        elif line.startswith("- "):
            if not in_list:
                out.append("<ul>")
                in_list = True
            out.append(f"<li>{_inline_md(line[2:])}</li>")
        else:
            _close()
            out.append(f"<p>{_inline_md(line)}</p>")
    _close()
    return "\n".join(out)


def render_memory_tab(context: str) -> None:
    """Render the shared-memory context digest into a 'Shared Memory' report tab."""
    if context and context.strip():
        body = _md_to_html(context)
    else:
        body = "<p><em>No shared memory yet — the evals pipeline populates this every 10 minutes.</em></p>"
    try:
        flyte.report.get_tab(_MEMORY_TAB).replace(f'<div class="agent-memory">{body}</div>')
    except Exception:
        flyte.logger.warning("failed to render shared-memory report tab")
