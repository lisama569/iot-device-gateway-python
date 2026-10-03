"""Test-run reporting: group cases by business area and print, at the end of
the run, which businesses are covered, which cases each has, and their
individual pass/fail status -- so the unit-test log itself is the coverage
proof sheet.
"""
from __future__ import annotations

# file (business area) -> Chinese label shown in the summary
AREA_LABELS = {
    "test_registry.py": "设备接入与凭据生命周期",
    "test_telemetry.py": "遥测摄入与设备影子合并",
    "test_fota.py": "固件升级编排与重启恢复",
    "test_backup.py": "在线备份与跨环境恢复",
    "test_restart.py": "恢复后重启持久化（本轮修复）",
    "test_restart_http.py": "恢复后重启持久化（真实进程 HTTP 演练）",
    "test_api.py": "端到端 HTTP 接口",
}

_GLYPH = {"passed": "✓", "failed": "✗", "skipped": "○", "error": "✗"}


def pytest_terminal_summary(terminalreporter):
    tr = terminalreporter
    lines = ["", "================ 业务覆盖与用例通过情况 ================"]
    total = passed = failed = skipped = 0
    # gather every case keyed by its file, keep definition order
    areas: dict[str, list[tuple[str, str]]] = {}
    outcomes: dict[str, str] = {}
    for name, reps in tr.stats.items():
        for rep in reps:
            if not hasattr(rep, "nodeid") or "::" not in rep.nodeid:
                continue
            if rep.when not in ("call", "setup"):
                continue
            nodeid = rep.nodeid
            if nodeid in outcomes and outcomes[nodeid] == "passed":
                continue
            if rep.when == "setup" and rep.passed:
                continue  # only record setup outcome if it actually failed
            outcomes[nodeid] = name
    # order areas by file name but keep the repaired-area suite prominent
    for nodeid, outcome in outcomes.items():
        fname = nodeid.split("::", 1)[0].split("/")[-1]
        case = nodeid.split("::", 1)[1]
        areas.setdefault(fname, []).append((case, outcome))
    for fname in sorted(areas, key=lambda f: (f != "test_restart.py", f)):
        cases = areas[fname]
        label = AREA_LABELS.get(fname, fname)
        lines.append(f"\n【{label}】({fname}) —— {len(cases)} 个用例")
        for case, outcome in cases:
            total += 1
            if outcome == "passed":
                passed += 1
            elif outcome == "skipped":
                skipped += 1
            else:
                failed += 1
            lines.append(f"  {_GLYPH.get(outcome, '?')} [{outcome}] {case}")
    lines.append("")
    lines.append(f"合计：{total} 个用例，通过 {passed}，失败 {failed}，跳过 {skipped}")
    lines.append("========================================================")
    tr.write_line("\n".join(lines))
