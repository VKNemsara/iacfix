"""
agy_client.py — Antigravity CLI wrapper

Auth flow:
  agy only prints the OAuth URL when it detects a real terminal (TTY).
  When run with piped stdin, it just says "authentication required" and exits.

  Fix: use Python's pty module to open a pseudo-terminal, run agy inside it,
  then read the PTY master to capture the OAuth URL from the output.
  The URL is streamed back to the browser as an SSE event.

Fix flow:
  echo "<prompt>" | agy  →  agy reads from stdin in non-interactive mode.
  We capture stdout as the fixed playbook (YAML).
"""

import asyncio
import fcntl
import json
import os
import pty
import re
import subprocess as _subprocess
import sys
from pathlib import Path
from typing import AsyncIterator

# ---------------------------------------------------------------------------
# agy binary discovery
# ---------------------------------------------------------------------------

_AGY_CANDIDATES = [
    "agy",
    "/root/.local/bin/agy",
    "/root/.antigravity/bin/agy",
    "/usr/local/bin/agy",
    os.path.expanduser("~/.local/bin/agy"),
    os.path.expanduser("~/.antigravity/bin/agy"),
]


def _find_agy() -> str:
    import shutil
    for candidate in _AGY_CANDIDATES:
        if shutil.which(candidate):
            return candidate
        p = Path(candidate)
        if p.is_file() and os.access(str(p), os.X_OK):
            return str(p)
    return "agy"   # fall back; will surface a clear error at runtime


def _agy_exists(bin_path: str) -> bool:
    import shutil
    return bool(shutil.which(bin_path)) or (
        Path(bin_path).is_file() and os.access(bin_path, os.X_OK)
    )


AGY_BIN = _find_agy()

# ---------------------------------------------------------------------------
# Token file locations agy may use
# ---------------------------------------------------------------------------

_TOKEN_PATHS = [
    Path.home() / ".gemini" / "oauth_creds.json",
    Path.home() / ".gemini" / "credentials.json",
    Path.home() / ".config" / "Antigravity" / "auth.json",
    Path.home() / ".gemini" / "antigravity-cli" / "auth.json",
]


async def is_authenticated() -> bool:
    """
    Return True if agy already has a valid cached session.

    agy has no 'auth status' subcommand — authentication is automatic on first
    use. We scan known token file locations for credential data.
    """
    # Check specific known paths first
    for p in _TOKEN_PATHS:
        if p.exists():
            try:
                text = p.read_text().strip()
                if not text:
                    continue
                try:
                    data = json.loads(text)
                    if isinstance(data, dict) and (
                        data.get("access_token")
                        or data.get("token")
                        or data.get("refresh_token")
                    ):
                        return True
                except json.JSONDecodeError:
                    if len(text) > 10:   # plain token string
                        return True
            except Exception:
                pass

    # Broad scan: any .json under ~/.gemini that looks like a credential
    gemini_dir = Path.home() / ".gemini"
    if gemini_dir.exists():
        for p in gemini_dir.rglob("*.json"):
            try:
                data = json.loads(p.read_text())
                if isinstance(data, dict) and (
                    data.get("access_token") or data.get("refresh_token")
                ):
                    return True
            except Exception:
                pass

    return False


# ---------------------------------------------------------------------------
# Auth login via PTY
# ---------------------------------------------------------------------------

async def login_url_stream() -> AsyncIterator[str]:
    """
    Run `agy` inside a pseudo-terminal (PTY) so it enters interactive auth mode
    and prints an OAuth URL.

    When agy detects a real terminal it prints the auth URL and waits for the
    user to complete login in their browser. Without a TTY it just prints
    "authentication required" and exits — so we MUST use a PTY.

    Yields:
      "HEARTBEAT:"          — keep-alive (ignored by browser)
      "AUTH_URL:<url>"      — the OAuth URL for the user to open
      "AUTH_COMPLETE"       — tokens found, authentication done
      "AUTH_TIMEOUT"        — 7-minute overall timeout
      "AUTH_ERROR:<detail>" — something went wrong
    """
    agy_bin = _find_agy()
    if not _agy_exists(agy_bin):
        yield (
            "AUTH_ERROR:agy CLI not found in the container. "
            "Rebuild the image: docker compose up --build"
        )
        return

    # Open a pseudo-terminal pair
    try:
        master_fd, slave_fd = pty.openpty()
    except Exception as e:
        yield f"AUTH_ERROR:Could not open PTY: {e}"
        return

    proc = None
    url_re  = re.compile(r"https?://\S{20,}")
    ansi_re = re.compile(r"\x1b\[[^a-zA-Z]*[a-zA-Z]|\x1b[=>]|\r")

    try:
        # ── Spawn agy with the slave PTY as its controlling terminal ──────
        try:
            proc = _subprocess.Popen(
                [agy_bin],
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                close_fds=True,
                env={
                    **os.environ,
                    "TERM": "xterm-256color",
                    "COLUMNS": "220",
                    "LINES": "50",
                    "NO_COLOR": "1",
                },
            )
        except FileNotFoundError:
            yield "AUTH_ERROR:agy binary not executable inside the container."
            return
        finally:
            # Parent never writes to slave; close our copy immediately
            os.close(slave_fd)
            slave_fd = -1

        # ── Make master non-blocking for async reading ────────────────────
        fl = fcntl.fcntl(master_fd, fcntl.F_GETFL)
        fcntl.fcntl(master_fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

        loop        = asyncio.get_event_loop()
        accumulated = ""
        url_emitted = False
        eof_seen    = False

        def _on_readable():
            nonlocal accumulated, eof_seen
            try:
                chunk = os.read(master_fd, 4096)
                if chunk:
                    text = ansi_re.sub("", chunk.decode(errors="replace"))
                    accumulated += text
            except OSError:
                # EIO fires when the slave side has closed (process exited)
                eof_seen = True
                try:
                    loop.remove_reader(master_fd)
                except Exception:
                    pass

        loop.add_reader(master_fd, _on_readable)

        url_deadline  = loop.time() + 90     # 90 s to see the URL
        auth_deadline = loop.time() + 420    # 7 min total
        last_heartbeat = 0.0

        # ── Main read loop ────────────────────────────────────────────────
        try:
            while True:
                now = loop.time()

                # Heartbeat every 3 s
                if now - last_heartbeat >= 3:
                    last_heartbeat = now
                    yield "HEARTBEAT:"

                # Emit URL as soon as it appears
                if not url_emitted:
                    m = url_re.search(accumulated)
                    if m:
                        url_emitted = True
                        yield f"AUTH_URL:{m.group(0)}"
                        url_deadline = now + 300  # extend after URL found

                # Check if agy has exited
                if eof_seen or proc.poll() is not None:
                    break

                if now > url_deadline and not url_emitted:
                    proc.kill()
                    snippet = repr(accumulated[:500])
                    yield f"AUTH_ERROR:agy did not output a login URL within 90 s. Output: {snippet}"
                    return

                if now > auth_deadline:
                    proc.kill()
                    yield "AUTH_TIMEOUT"
                    return

                await asyncio.sleep(0.3)
        finally:
            try:
                loop.remove_reader(master_fd)
            except Exception:
                pass

        # Wait for process (non-blocking poll since it likely already exited)
        await loop.run_in_executor(None, proc.wait)
        proc = None

    finally:
        # Always clean up the master PTY fd
        try:
            if master_fd >= 0:
                os.close(master_fd)
                master_fd = -1
        except OSError:
            pass
        # Kill process if still running
        if proc and proc.poll() is None:
            proc.kill()

    # ── Final URL scan (in case URL came in the last chunk before exit) ───
    if not url_emitted:
        m = url_re.search(accumulated)
        if m:
            url_emitted = True
            yield f"AUTH_URL:{m.group(0)}"

    if not url_emitted:
        snippet = repr(accumulated[:500])
        yield f"AUTH_ERROR:agy exited without printing a login URL. Output: {snippet}"
        return

    # ── Poll token files until auth completes (max 5 min) ────────────────
    for _ in range(60):
        await asyncio.sleep(5)
        yield "HEARTBEAT:"
        if await is_authenticated():
            yield "AUTH_COMPLETE"
            return

    yield "AUTH_TIMEOUT"


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
