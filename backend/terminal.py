"""
terminal.py — WebSocket ↔ PTY bridge for the embedded agy auth terminal.

Protocol:
  Binary WS frames  → raw PTY bytes  (xterm.js renders these directly)
  Text WS frames    → JSON control messages

  Server → Client:
    {"type": "auth_url",     "url":     "https://..."}  — URL detected, show as button
    {"type": "auth_complete"}                            — tokens found, proceed
    {"type": "auth_timeout"}                             — gave up waiting
    {"type": "error",        "message": "..."}           — startup error
    {"type": "closed"}                                   — agy process exited

  Client → Server:
    {"type": "resize", "cols": N, "rows": N}             — terminal resize
    (binary frames)                                      — keyboard input → PTY
"""

import asyncio
import fcntl
import json
import os
import pty
import re
import struct
import subprocess
import termios

from fastapi import WebSocket, WebSocketDisconnect

from agy_client import _find_agy, _agy_exists, is_authenticated

_URL_RE  = re.compile(r"https?://\S{20,}")
_ANSI_RE = re.compile(r"\x1b\[[^a-zA-Z]*[a-zA-Z]|\x1b[=>O]|\r")


async def run_auth_terminal(websocket: WebSocket) -> None:
    """Bridge a WebSocket client to a live `agy` PTY session."""

    agy_bin = _find_agy()
    if not _agy_exists(agy_bin):
        await websocket.send_text(json.dumps({
            "type": "error",
            "message": (
                "\r\n\x1b[31m✗ agy CLI not found inside the container.\x1b[0m\r\n"
                "Rebuild the image:  docker compose up --build\r\n"
            ),
        }))
        return

    master_fd: int = -1
    slave_fd:  int = -1
    proc = None
    loop = asyncio.get_event_loop()

    try:
        master_fd, slave_fd = pty.openpty()

        proc = subprocess.Popen(
            [agy_bin],
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            close_fds=True,
            env={
                **os.environ,
                "TERM": "xterm-256color",
                "COLUMNS": "120",
                "LINES": "30",
                "COLORTERM": "truecolor",
            },
        )
        # Parent doesn't need the slave end
        os.close(slave_fd)
        slave_fd = -1

        # Make PTY master non-blocking for async reads
        fl = fcntl.fcntl(master_fd, fcntl.F_GETFL)
        fcntl.fcntl(master_fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

        send_q:    asyncio.Queue = asyncio.Queue()
        url_sent   = False
        pty_closed = asyncio.Event()
        text_buf   = ""           # accumulates decoded output for URL scanning

        # ── PTY reader (event-loop callback) ──────────────────────────────
        def _on_pty_data():
            nonlocal url_sent, text_buf
            try:
                data = os.read(master_fd, 4096)
            except (BlockingIOError, OSError):
                pty_closed.set()
                _safe_remove_reader()
                return

            if not data:
                return

            send_q.put_nowait(("bytes", data))

            if not url_sent:
                text_buf += _ANSI_RE.sub("", data.decode(errors="replace"))
                m = _URL_RE.search(text_buf)
                if m:
                    url_sent = True
                    send_q.put_nowait(("json", {"type": "auth_url", "url": m.group(0)}))

        def _safe_remove_reader():
            try:
                loop.remove_reader(master_fd)
            except Exception:
                pass

        loop.add_reader(master_fd, _on_pty_data)

        # ── Coroutines ────────────────────────────────────────────────────

        async def _sender():
            """Flush send_q → WebSocket."""
            TERMINAL_TYPES = {"closed", "error", "auth_complete", "auth_timeout"}
            while True:
                kind, payload = await send_q.get()
                try:
                    if kind == "bytes":
                        await websocket.send_bytes(payload)
                    else:
                        await websocket.send_text(json.dumps(payload))
                        if payload.get("type") in TERMINAL_TYPES:
                            break
                except Exception:
                    break

        async def _receiver():
            """WebSocket input → PTY master."""
            try:
                while True:
                    msg = await websocket.receive()
                    if msg["type"] == "websocket.disconnect":
                        break

                    if msg.get("bytes"):
                        try:
                            os.write(master_fd, msg["bytes"])
                        except OSError:
                            break
                    elif msg.get("text"):
                        try:
                            ctrl = json.loads(msg["text"])
                            if ctrl.get("type") == "resize":
                                cols = max(1, int(ctrl.get("cols", 120)))
                                rows = max(1, int(ctrl.get("rows", 30)))
                                packed = struct.pack("HHHH", rows, cols, 0, 0)
                                fcntl.ioctl(master_fd, termios.TIOCSWINSZ, packed)
                        except Exception:
                            pass
            except (WebSocketDisconnect, Exception):
                pass

        async def _auth_poller():
            """Poll token files and signal when auth completes (max 7 min)."""
            for _ in range(84):   # 84 × 5 s = 7 min
                await asyncio.sleep(5)
                try:
                    if await is_authenticated():
                        send_q.put_nowait(("json", {"type": "auth_complete"}))
                        return
                except Exception:
                    pass
            send_q.put_nowait(("json", {"type": "auth_timeout"}))

        async def _pty_exit_watcher():
            """When PTY closes, drain remaining output then signal closed."""
            await pty_closed.wait()
            await asyncio.sleep(0.5)          # let last bytes drain
            send_q.put_nowait(("json", {"type": "closed"}))

        # ── Run all concurrently; stop when any one finishes ──────────────
        tasks = [
            asyncio.create_task(_sender(),          name="sender"),
            asyncio.create_task(_receiver(),        name="receiver"),
            asyncio.create_task(_auth_poller(),     name="auth_poller"),
            asyncio.create_task(_pty_exit_watcher(),name="pty_watcher"),
        ]
        _done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in _pending:
            t.cancel()
        await asyncio.gather(*_pending, return_exceptions=True)

    finally:
        _safe_remove_reader() if master_fd >= 0 else None
        for fd in (master_fd, slave_fd):
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
        if proc and proc.poll() is None:
            try:
                proc.kill()
            except Exception:
                pass
