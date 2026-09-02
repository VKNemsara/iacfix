"""
pipeline.py — Core async pipeline state machine

Emits SSE-compatible events (JSON strings) via an async generator.
The caller (main.py) wraps this with EventSourceResponse.

Pipeline stages:
  INIT → LINT_INITIAL → CHECKOV_INITIAL → AGY_FIX →
  LINT_RECHECK → CHECKOV_RECHECK → DEPLOY_TEST →
  (loop: AGY_DEPLOY_FIX → LINT_RECHECK → ...) → DONE | FAILED
"""

import asyncio
import json
import logging
import os
import shutil
import tempfile
import time
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import AsyncIterator, Any

from agy_client import fix_playbook, fix_playbook_deploy_error
from linters import run_all_linters, LintResult, CheckovResult
from docker_runner import run_deployment_test, ensure_test_image, DeployResult

logger = logging.getLogger(__name__)

MAX_FIX_ITERATIONS = 5
MAX_DEPLOY_FIX_ITERATIONS = 3

# Storage dir for active sessions
SESSIONS_DIR = Path(os.environ.get("SESSIONS_DIR", "/tmp/playbook-fixer-sessions"))
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Event helpers
# ---------------------------------------------------------------------------

def _evt(
    stage: str,
    status: str,
    message: str = "",
    **kwargs: Any,
) -> str:
    """Return a JSON-serialised SSE data line."""
    payload = {
        "stage": stage,
        "status": status,
        "message": message,
        "ts": int(time.time() * 1000),
        **kwargs,
    }
    return json.dumps(payload)


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

@dataclass
class Session:
    session_id: str
    original_path: Path
    current_path: Path
    fixed_path: Path | None = None
    iteration: int = 0
    deploy_iteration: int = 0
    prior_attempts: list[str] = field(default_factory=list)


def create_session(uploaded_content: bytes, filename: str) -> "Session":
    sid = str(uuid.uuid4())
    session_dir = SESSIONS_DIR / sid
    session_dir.mkdir(parents=True)

    original = session_dir / "original.yml"
    original.write_bytes(uploaded_content)

    current = session_dir / "current.yml"
    shutil.copy(original, current)

    return Session(
        session_id=sid,
        original_path=original,
        current_path=current,
    )


def get_fixed_path(session: Session) -> Path | None:
    return session.fixed_path


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def run_pipeline(session: Session) -> AsyncIterator[str]:
    """
    Full pipeline as an async generator of SSE event strings.
    """

    # ── INIT: pre-build Docker test image ──────────────────────────────────
    yield _evt("INIT", "running", "Preparing Docker test environment...")
    try:
        await ensure_test_image()
        yield _evt("INIT", "done", "Docker test environment ready.")
    except Exception as e:
        yield _evt("INIT", "warning", f"Docker image build skipped: {e}")

    # ── STAGE 1: Initial lint + checkov ────────────────────────────────────
    yield _evt("STAGE_1", "running", "Running initial checks (ansible-lint + checkov in parallel)...")

    lint, checkov = await run_all_linters(session.current_path)

    yield _evt(
        "LINT_INITIAL", "done" if lint.passed else "issues_found",
        f"ansible-lint: {'✓ No issues' if lint.passed else f'{len(lint.issues)} issue(s) found'}",
        issues=lint.issues[:50],  # cap payload size
        passed=lint.passed,
    )

    yield _evt(
        "CHECKOV_INITIAL", "done" if checkov.passed else "issues_found",
        f"checkov: {'✓ No issues' if checkov.passed else f'{len(checkov.failed_checks)} check(s) failed'}",
        issues=checkov.failed_checks[:50],
        passed=checkov.passed,
    )

    # If both pass already, skip AGY fix loop
    if lint.passed and checkov.passed:
        yield _evt("STAGE_1", "done", "✓ No issues found in initial checks. Proceeding to deployment.")
        async for evt in _deploy_loop(session):
            yield evt
        return

    # ── STAGE 2: AGY fix loop ──────────────────────────────────────────────
    yield _evt("STAGE_2", "running", "Sending issues to Antigravity for semantic fixing...")

    async for evt in _agy_fix_loop(session, lint, checkov):
        yield evt


async def _agy_fix_loop(
    session: Session,
    lint: LintResult,
    checkov: CheckovResult,
) -> AsyncIterator[str]:
    """Iteratively fix lint/checkov issues using AGY, re-validate after each fix."""

    for iteration in range(1, MAX_FIX_ITERATIONS + 1):
        session.iteration = iteration
        total_issues = len(lint.issues) + len(checkov.failed_checks)

        yield _evt(
            "AGY_FIX",
            "running",
            f"[Attempt {iteration}/{MAX_FIX_ITERATIONS}] Sending {total_issues} issue(s) to Antigravity...",
            iteration=iteration,
            lint_count=len(lint.issues),
            checkov_count=len(checkov.failed_checks),
        )

        playbook_content = session.current_path.read_text()

        try:
            fixed = await fix_playbook(
                playbook_content=playbook_content,
                lint_issues=lint.issues,
                checkov_results={"results": {"failed_checks": checkov.failed_checks}},
                prior_attempts=session.prior_attempts,
            )
        except Exception as e:
            yield _evt("AGY_FIX", "error", f"Antigravity error: {e}")
            yield _evt("PIPELINE", "failed", "Pipeline failed: AGY could not fix issues.")
            return

        # Save fixed version
        session.current_path.write_text(fixed)
        session.prior_attempts.append(fixed)

        # Compute a simplified diff summary
        original_lines = playbook_content.splitlines()
        fixed_lines = fixed.splitlines()
        added = sum(1 for l in fixed_lines if l not in set(original_lines))
        removed = sum(1 for l in original_lines if l not in set(fixed_lines))

        yield _evt(
            "AGY_FIX",
            "applied",
            f"[Attempt {iteration}] Fix applied (+{added} lines, -{removed} lines)",
            iteration=iteration,
            diff=_simple_diff(playbook_content, fixed),
        )

        # Re-validate
        yield _evt(
            "STAGE_3_VALIDATE", "running",
            f"[Attempt {iteration}] Re-validating fixed playbook...",
        )

        lint, checkov = await run_all_linters(session.current_path)

        yield _evt(
            "LINT_RECHECK",
            "done" if lint.passed else "issues_found",
            f"ansible-lint: {'✓ Clean' if lint.passed else f'{len(lint.issues)} issue(s) remain'}",
            issues=lint.issues[:50],
            passed=lint.passed,
            iteration=iteration,
        )

        yield _evt(
            "CHECKOV_RECHECK",
            "done" if checkov.passed else "issues_found",
            f"checkov: {'✓ Clean' if checkov.passed else f'{len(checkov.failed_checks)} check(s) remain'}",
            issues=checkov.failed_checks[:50],
            passed=checkov.passed,
            iteration=iteration,
        )

        if lint.passed and checkov.passed:
            yield _evt(
                "STAGE_2", "done",
                f"✓ All checks passed after {iteration} fix attempt(s). Proceeding to deployment.",
            )
            async for evt in _deploy_loop(session):
                yield evt
            return

    # Exhausted iterations
    yield _evt(
        "STAGE_2", "failed",
        f"✗ Could not resolve all issues after {MAX_FIX_ITERATIONS} attempts.",
        lint_issues=lint.issues[:20],
        checkov_issues=checkov.failed_checks[:20],
    )
    yield _evt("PIPELINE", "failed", "Pipeline stopped: maximum fix iterations reached.")


async def _deploy_loop(session: Session) -> AsyncIterator[str]:
    """Run deployment test, loop with AGY fixes if it fails."""

    for d_iter in range(1, MAX_DEPLOY_FIX_ITERATIONS + 1):
        session.deploy_iteration = d_iter

        yield _evt(
            "DEPLOY_TEST",
            "running",
            f"[Deploy {d_iter}/{MAX_DEPLOY_FIX_ITERATIONS}] Running Docker deployment test...",
            iteration=d_iter,
        )

        result: DeployResult = await run_deployment_test(session.current_path)

        if result.success:
            session.fixed_path = session.current_path
            yield _evt(
                "DEPLOY_TEST",
                "passed",
                "✓ Deployment test passed! Playbook is verified and ready.",
                logs=result.logs[:4000],
                iteration=d_iter,
            )
            yield _evt("PIPELINE", "success", "🎉 Pipeline complete! Download your fixed playbook.")
            return

        # Deployment failed
        truncated_errors = (result.error or result.logs)[:3000]
        yield _evt(
            "DEPLOY_TEST",
            "failed",
            f"[Deploy {d_iter}] Deployment failed (exit {result.exit_code}). Sending errors to Antigravity...",
            logs=truncated_errors,
            exit_code=result.exit_code,
            iteration=d_iter,
        )

        if d_iter == MAX_DEPLOY_FIX_ITERATIONS:
            break

        # Send errors to AGY for a fix
        yield _evt(
            "AGY_DEPLOY_FIX",
            "running",
            f"[Deploy fix {d_iter}] Antigravity is analysing deployment errors...",
            iteration=d_iter,
        )

        playbook_content = session.current_path.read_text()
        try:
            fixed = await fix_playbook_deploy_error(
                playbook_content=playbook_content,
                deploy_errors=truncated_errors,
            )
        except Exception as e:
            yield _evt("AGY_DEPLOY_FIX", "error", f"Antigravity error: {e}")
            break

        session.current_path.write_text(fixed)

        yield _evt(
            "AGY_DEPLOY_FIX",
            "applied",
            f"[Deploy fix {d_iter}] Fix applied. Re-running validation before next deploy...",
            iteration=d_iter,
        )

        # Re-validate after deploy fix
        lint, checkov = await run_all_linters(session.current_path)
        if not lint.passed or not checkov.passed:
            yield _evt(
                "VALIDATE_AFTER_DEPLOY_FIX",
                "issues_found",
                "Validation found new issues after deploy fix — running another AGY fix cycle...",
            )
            # Re-enter the fix loop for the new issues
            async for evt in _agy_fix_loop(session, lint, checkov):
                yield evt
            return

        yield _evt(
            "VALIDATE_AFTER_DEPLOY_FIX",
            "done",
            "✓ Post-deploy-fix validation passed.",
        )

    yield _evt(
        "DEPLOY_TEST",
        "failed_final",
        f"✗ Deployment could not be fixed after {MAX_DEPLOY_FIX_ITERATIONS} attempt(s).",
    )
    yield _evt("PIPELINE", "failed", "Pipeline stopped: deployment test consistently failing.")


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _simple_diff(original: str, fixed: str) -> list[dict]:
    """Return a condensed line-by-line diff for the frontend diff viewer."""
    import difflib
    diff = list(difflib.unified_diff(
        original.splitlines(keepends=True),
        fixed.splitlines(keepends=True),
        fromfile="original.yml",
        tofile="fixed.yml",
        n=3,
    ))
    # Return at most 200 diff lines to keep SSE payload reasonable
    result = []
    for line in diff[:200]:
        if line.startswith("+"):
            result.append({"type": "add", "content": line[1:].rstrip()})
        elif line.startswith("-"):
            result.append({"type": "remove", "content": line[1:].rstrip()})
        elif line.startswith("@@"):
            result.append({"type": "hunk", "content": line.strip()})
        else:
            result.append({"type": "context", "content": line[1:].rstrip()})
    return result
