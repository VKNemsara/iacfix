"""
linters.py — ansible-lint and checkov wrappers

Both tools are called with JSON output so results are machine-parseable.
"""

import asyncio
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class LintResult:
    passed: bool
    issues: list[dict] = field(default_factory=list)
    raw_output: str = ""
    error: str = ""


@dataclass
class CheckovResult:
    passed: bool
    failed_checks: list[dict] = field(default_factory=list)
    raw_output: str = ""
    error: str = ""


# ---------------------------------------------------------------------------
# ansible-lint
# ---------------------------------------------------------------------------

async def run_ansible_lint(playbook_path: str | Path) -> LintResult:
    """
    Run ansible-lint on a playbook file.
    Returns a LintResult with parsed JSON issues.
    """
    path = str(playbook_path)

    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "ansiblelint",  # python -m ansiblelint
        "--format", "json",
        "--nocolor",
        "--offline",         # don't fetch galaxy roles during lint
        path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, "ANSIBLE_FORCE_COLOR": "0"},
    )

    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
    except asyncio.TimeoutError:
        proc.kill()
        return LintResult(passed=False, error="ansible-lint timed out")

    raw = stdout.decode(errors="replace")
    err = stderr.decode(errors="replace")

    # ansible-lint exits 0 = no issues, 2 = issues found, 1 = internal error
    if proc.returncode == 1:
        return LintResult(passed=False, raw_output=raw, error=err or raw)

    try:
        data = json.loads(raw) if raw.strip() else {}
        issues = _parse_ansible_lint_json(data)
    except json.JSONDecodeError:
        # Fallback: try to parse as SARIF or plain text
        issues = _parse_ansible_lint_fallback(raw)

    return LintResult(
        passed=len(issues) == 0,
        issues=issues,
        raw_output=raw,
    )


def _parse_ansible_lint_json(data: Any) -> list[dict]:
    """Handle multiple json output shapes from different ansible-lint versions."""
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # SARIF format
        if "runs" in data:
            issues = []
            for run in data.get("runs", []):
                for result in run.get("results", []):
                    issues.append({
                        "rule": {
                            "id": result.get("ruleId", ""),
                            "description": result.get("message", {}).get("text", ""),
                        },
                        "message": result.get("message", {}).get("text", ""),
                        "location": _extract_sarif_location(result),
                    })
            return issues
        # Direct object with warnings list
        return data.get("warnings", data.get("issues", []))
    return []


def _extract_sarif_location(result: dict) -> dict:
    locs = result.get("locations", [])
    if locs:
        loc = locs[0]
        region = loc.get("physicalLocation", {}).get("region", {})
        uri = loc.get("physicalLocation", {}).get("artifactLocation", {}).get("uri", "")
        return {
            "path": uri,
            "lines": {"begin": {"line": region.get("startLine", 0)}},
        }
    return {}


def _parse_ansible_lint_fallback(raw: str) -> list[dict]:
    """Parse plain-text output as a last resort."""
    issues = []
    for line in raw.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            issues.append({"message": line, "rule": {"id": "unknown", "description": line}})
    return issues


# ---------------------------------------------------------------------------
# checkov
# ---------------------------------------------------------------------------

async def run_checkov(playbook_path: str | Path) -> CheckovResult:
    """
    Run checkov on a playbook file using the ansible framework.
    Returns a CheckovResult with parsed JSON findings.
    """
    path = str(playbook_path)

    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "checkov.main",
        "--file", path,
        "--framework", "ansible",
        "--output", "json",
        "--quiet",
        "--compact",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ},
    )

    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
    except asyncio.TimeoutError:
        proc.kill()
        return CheckovResult(passed=False, error="checkov timed out")

    raw = stdout.decode(errors="replace")
    err = stderr.decode(errors="replace")

    # checkov exit codes: 0 = passed, 1 = failed checks, 2 = errors
    if proc.returncode == 2:
        return CheckovResult(passed=False, raw_output=raw, error=err or raw)

    try:
        # checkov may emit multiple JSON objects; grab the last valid one
        data = _parse_checkov_json_output(raw)
    except Exception as e:
        return CheckovResult(passed=False, raw_output=raw, error=str(e))

    failed = _extract_failed_checks(data)

    return CheckovResult(
        passed=len(failed) == 0,
        failed_checks=failed,
        raw_output=raw,
    )


def _parse_checkov_json_output(raw: str) -> dict:
    """checkov sometimes emits a list of result objects or a single object."""
    raw = raw.strip()
    if not raw:
        return {}
    # Try as a list first
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list) and parsed:
            return parsed[0]
        return parsed
    except json.JSONDecodeError:
        pass
    # Try line-by-line (streaming JSON)
    for line in reversed(raw.splitlines()):
        line = line.strip()
        if line.startswith("{"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    return {}


def _extract_failed_checks(data: dict) -> list[dict]:
    results = data.get("results", {})
    if isinstance(results, dict):
        return results.get("failed_checks", [])
    return []


# ---------------------------------------------------------------------------
# Convenience: run both in parallel
# ---------------------------------------------------------------------------

async def run_all_linters(playbook_path: str | Path) -> tuple[LintResult, CheckovResult]:
    """Run ansible-lint and checkov concurrently and return both results."""
    lint_task = asyncio.create_task(run_ansible_lint(playbook_path))
    checkov_task = asyncio.create_task(run_checkov(playbook_path))
    lint_result, checkov_result = await asyncio.gather(lint_task, checkov_task)
    return lint_result, checkov_result
