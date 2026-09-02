"""
agy_client.py — Antigravity CLI wrapper

Auth flow:
  agy auth login  →  prints an OAuth URL to stdout
  We stream that URL back to the browser so the user can click it.
  We poll `agy auth status` (or check the token file) until authenticated.

Fix flow:
  echo "<prompt>" | agy  →  agy reads from stdin in non-interactive mode
  We capture stdout as the fixed playbook (YAML).
"""

import asyncio
import os
import re
import json
import subprocess
from pathlib import Path
from typing import AsyncIterator

# agy binary — try PATH first, then common install locations
_AGY_CANDIDATES = [
    "agy",
    "/root/.local/bin/agy",
    "/usr/local/bin/agy",
    os.path.expanduser("~/.local/bin/agy"),
]


def _find_agy() -> str:
    for candidate in _AGY_CANDIDATES:
        try:
            result = subprocess.run(
                [candidate, "--version"],
                capture_output=True,
                timeout=5,
            )
            if result.returncode == 0:
                return candidate
        except (FileNotFoundError, subprocess.TimeoutExpired):
            continue
    return "agy"  # fall back; will raise FileNotFoundError at runtime


AGY_BIN = _find_agy()

# Where agy persists its session
_TOKEN_PATHS = [
    Path.home() / ".gemini" / "oauth_creds.json",
    Path.home() / ".config" / "Antigravity" / "auth.json",
    Path.home() / ".gemini" / "antigravity-cli" / "auth.json",
]


async def is_authenticated() -> bool:
    """Return True if agy already has a valid cached session."""
    # Quick file-based check first
    for p in _TOKEN_PATHS:
        if p.exists():
            try:
                data = json.loads(p.read_text())
                if data.get("access_token") or data.get("token"):
                    return True
            except Exception:
                pass

    # Subprocess check
    try:
        proc = await asyncio.create_subprocess_exec(
            AGY_BIN, "auth", "status",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
        text = stdout.decode()
        return "logged in" in text.lower() or "authenticated" in text.lower()
    except Exception:
        return False


async def login_url_stream() -> AsyncIterator[str]:
    """
    Spawn `agy auth login`, yield the OAuth URL when it appears,
    then yield 'AUTH_COMPLETE' once auth succeeds.

    Reads stdout AND stderr concurrently (agy may write the URL to either).
    Sends periodic HEARTBEAT pings so the SSE connection stays alive.
    """
    agy_bin = _find_agy()
    if not _agy_exists(agy_bin):
        yield "AUTH_ERROR:agy CLI not found. Install: curl -fsSL https://antigravity.google/cli/install.sh | bash"
        return

    try:
        proc = await asyncio.create_subprocess_exec(
            agy_bin, "auth", "login",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,     # capture stderr separately
            env={**os.environ, "TERM": "dumb", "NO_COLOR": "1"},
        )
    except FileNotFoundError:
        yield "AUTH_ERROR:agy CLI not found in PATH inside the container."
        return

    url_re = re.compile(r"https?://\S{10,}")    # at least 10 chars to avoid noise
    ansi_re = re.compile(r"\x1b\[[0-9;]*m")    # strip ANSI colour codes
    accumulated = ""
    url_emitted = False

    async def _read_stream(stream) -> None:
        """Read a stream in 256-byte chunks, appending to accumulated."""
        nonlocal accumulated
        while True:
            chunk = await stream.read(256)
            if not chunk:
                break
            accumulated += ansi_re.sub("", chunk.decode(errors="replace"))

    stdout_task = asyncio.create_task(_read_stream(proc.stdout))
    stderr_task = asyncio.create_task(_read_stream(proc.stderr))

    loop = asyncio.get_event_loop()
    url_deadline = loop.time() + 60        # 60 s to see the URL
    auth_deadline = loop.time() + 360      # 6 min total
    last_heartbeat = 0.0

    try:
        while True:
            now = loop.time()

            # Heartbeat every 3 s — keeps the SSE connection alive
            if now - last_heartbeat >= 3:
                last_heartbeat = now
                yield "HEARTBEAT:"

            # Emit URL as soon as it appears in the buffer
            if not url_emitted:
                m = url_re.search(accumulated)
                if m:
                    url_emitted = True
                    yield f"AUTH_URL:{m.group(0)}"
                    url_deadline = loop.time() + 300   # extend: user needs time to click

            # Both streams done → process has exited
            if stdout_task.done() and stderr_task.done():
                break

            if now > url_deadline and not url_emitted:
                proc.kill()
                yield "AUTH_ERROR:agy did not output a login URL within 60 s. " \
                      "Check container logs for details."
                return

            if now > auth_deadline:
                proc.kill()
                yield "AUTH_TIMEOUT"
                return

            await asyncio.sleep(0.3)
    finally:
        stdout_task.cancel()
        stderr_task.cancel()

    await proc.wait()

    # Final scan of accumulated buffer in case URL arrived at the very end
    if not url_emitted:
        m = url_re.search(accumulated)
        if m:
            url_emitted = True
            yield f"AUTH_URL:{m.group(0)}"

    if not url_emitted:
        yield "AUTH_ERROR:agy exited without printing a login URL. " \
              f"Output was: {accumulated[:300]!r}"
        return

    # Poll until authenticated (max 5 min)
    for _ in range(60):
        await asyncio.sleep(5)
        yield "HEARTBEAT:"
        if await is_authenticated():
            yield "AUTH_COMPLETE"
            return

    yield "AUTH_TIMEOUT"


def _agy_exists(bin_path: str) -> bool:
    """Return True if bin_path resolves to an executable."""
    import shutil
    return bool(shutil.which(bin_path)) or Path(bin_path).is_file()


# ---------------------------------------------------------------------------
# Fix prompt builder
# ---------------------------------------------------------------------------

_FIX_PROMPT_TEMPLATE = """\
You are an expert Ansible automation engineer.
Below is an Ansible playbook that has been flagged with errors by ansible-lint and checkov.
Fix ALL the listed issues semantically — do not introduce new issues while fixing existing ones.
Check that each fix does not break other tasks or variables in the playbook.

## Ansible Playbook (current version)
```yaml
{playbook}
```

## Errors to fix

### ansible-lint findings ({lint_count} issues)
{lint_issues}

### checkov findings ({checkov_count} issues)
{checkov_issues}

{prior_attempts_section}

## Instructions
- Return ONLY the fixed, complete YAML playbook.
- Do NOT include any explanation, markdown fences, or commentary.
- Do NOT truncate the playbook.
- The output must be valid YAML that can be saved directly as a .yml file.
"""

_DEPLOY_FIX_PROMPT_TEMPLATE = """\
You are an expert Ansible automation engineer.
An Ansible playbook that passed lint and security checks has FAILED during Docker-based deployment testing.
Fix the playbook so the deployment succeeds.

## Ansible Playbook (current version)
```yaml
{playbook}
```

## Deployment error output
```
{deploy_errors}
```

## Instructions
- Return ONLY the fixed, complete YAML playbook.
- Do NOT include any explanation, markdown fences, or commentary.
- Do NOT truncate the playbook.
- The output must be valid YAML that can be saved directly as a .yml file.
"""


def _format_lint_issues(issues: list[dict]) -> str:
    lines = []
    for i, issue in enumerate(issues, 1):
        rule = issue.get("rule", {})
        lines.append(
            f"{i}. [{rule.get('id', '?')}] {rule.get('description', issue.get('message', ''))}"
            f" — {issue.get('location', {}).get('path', '?')}:"
            f"{issue.get('location', {}).get('lines', {}).get('begin', {}).get('line', '?')}"
        )
    return "\n".join(lines) if lines else "None"


def _format_checkov_issues(results: dict) -> str:
    lines = []
    failed = results.get("results", {}).get("failed_checks", [])
    for i, check in enumerate(failed, 1):
        lines.append(
            f"{i}. [{check.get('check_id', '?')}] {check.get('check_result', {}).get('result', '?')}"
            f" — {check.get('resource', '?')} ({check.get('check_class', '?')})"
        )
    return "\n".join(lines) if lines else "None"


def _format_prior_attempts(attempts: list[str]) -> str:
    if not attempts:
        return ""
    section = "## Prior fix attempts that still had issues\n"
    for i, attempt in enumerate(attempts, 1):
        section += f"\n### Attempt {i}\n```yaml\n{attempt}\n```\n"
    return section


async def fix_playbook(
    playbook_content: str,
    lint_issues: list[dict],
    checkov_results: dict,
    prior_attempts: list[str] | None = None,
) -> str:
    """Send playbook + errors to agy and return the fixed YAML string."""
    prompt = _FIX_PROMPT_TEMPLATE.format(
        playbook=playbook_content,
        lint_count=len(lint_issues),
        lint_issues=_format_lint_issues(lint_issues),
        checkov_count=len(checkov_results.get("results", {}).get("failed_checks", [])),
        checkov_issues=_format_checkov_issues(checkov_results),
        prior_attempts_section=_format_prior_attempts(prior_attempts or []),
    )
    return await _run_agy_prompt(prompt)


async def fix_playbook_deploy_error(
    playbook_content: str,
    deploy_errors: str,
) -> str:
    """Send playbook + deployment errors to agy and return the fixed YAML string."""
    prompt = _DEPLOY_FIX_PROMPT_TEMPLATE.format(
        playbook=playbook_content,
        deploy_errors=deploy_errors,
    )
    return await _run_agy_prompt(prompt)


async def _run_agy_prompt(prompt: str) -> str:
    """
    Pipe a prompt to `agy` on stdin and capture its response from stdout.

    agy reads from stdin when not connected to a terminal (non-interactive mode).
    """
    proc = await asyncio.create_subprocess_exec(
        AGY_BIN,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, "NO_COLOR": "1", "TERM": "dumb"},
    )

    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(input=prompt.encode()),
            timeout=300,  # 5 min max for agy to respond
        )
    except asyncio.TimeoutError:
        proc.kill()
        raise RuntimeError("agy timed out after 5 minutes")

    if proc.returncode != 0:
        err = stderr.decode(errors="replace")
        raise RuntimeError(f"agy exited with code {proc.returncode}: {err[:500]}")

    result = stdout.decode(errors="replace").strip()

    # Strip markdown code fences if agy wrapped the output
    result = _strip_code_fences(result)

    return result


def _strip_code_fences(text: str) -> str:
    """Remove ```yaml ... ``` or ``` ... ``` wrapper if present."""
    lines = text.splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines).strip()
