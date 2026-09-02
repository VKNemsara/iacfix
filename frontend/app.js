/**
 * app.js — Antigravity Playbook Fixer
 * Handles auth (xterm.js + WebSocket PTY), upload, and pipeline SSE streaming.
 */

'use strict';

// ─── State ────────────────────────────────────────────────────────────────────
const state = {
  sessionId:       null,
  termWs:          null,   // WebSocket for auth terminal
  pipelineEs:      null,   // EventSource for pipeline
  term:            null,   // xterm.js Terminal instance
  fitAddon:        null,
  selectedFile:    null,
  pipelineDone:    false,
};

// ─── Helpers ──────────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const fmt_time = () => new Date().toLocaleTimeString('en', { hour12: false });

function showView(id) {
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  $(id).classList.add('active');
  if (id === 'view-signin') fitTerminal();
}

function setAuthBadge(status) {
  const badge = $('auth-badge');
  badge.className = 'badge';
  if (status === 'online')    { badge.classList.add('badge-online');  badge.textContent = 'Signed in'; }
  else if (status === 'checking') { badge.classList.add('badge-checking'); badge.textContent = 'Checking…'; }
  else                        { badge.classList.add('badge-offline'); badge.textContent = 'Not signed in'; }
}

function setTermStatus(text, cls = '') {
  const el = $('term-status');
  el.textContent = text;
  el.className = 'term-status' + (cls ? ' ' + cls : '');
}

// ─── xterm.js terminal ────────────────────────────────────────────────────────
function initTerminal() {
  if (state.term) return;   // already initialised

  state.term = new Terminal({
    cursorBlink:   true,
    fontSize:      13,
    fontFamily:    '"Fira Code", "Cascadia Code", "SF Mono", monospace',
    theme: {
      background:  '#000000',
      foreground:  '#e6edf3',
      cursor:      '#58a6ff',
      black:       '#0d1117',
      brightBlack: '#6e7681',
      red:         '#f85149',
      green:       '#3fb950',
      yellow:      '#d29922',
      blue:        '#58a6ff',
      magenta:     '#bc8cff',
      cyan:        '#39d353',
      white:       '#b1bac4',
    },
    scrollback:    1000,
    allowProposedApi: true,
  });

  state.fitAddon = new FitAddon.FitAddon();
  state.term.loadAddon(state.fitAddon);
  state.term.open($('terminal-container'));
  fitTerminal();

  // Handle terminal resize → send to server
  state.term.onResize(({ cols, rows }) => {
    if (state.termWs && state.termWs.readyState === WebSocket.OPEN) {
      state.termWs.send(JSON.stringify({ type: 'resize', cols, rows }));
    }
  });
}

function fitTerminal() {
  if (state.fitAddon) {
    try { state.fitAddon.fit(); } catch (_) {}
  }
}

window.addEventListener('resize', fitTerminal);

// ─── Auth ─────────────────────────────────────────────────────────────────────
async function checkAuth() {
  setAuthBadge('checking');
  try {
    const res = await fetch('/auth/status');
    const { authenticated } = await res.json();
    if (authenticated) {
      setAuthBadge('online');
      showView('view-upload');
    } else {
      setAuthBadge('offline');
      showView('view-signin');
      initTerminal();
    }
  } catch {
    setAuthBadge('offline');
    showView('view-signin');
    initTerminal();
  }
}

function startAuthTerminal() {
  const btn = $('btn-start-terminal');
  btn.disabled = true;
  btn.textContent = 'Connecting…';

  $('auth-url-banner').classList.add('hidden');
  $('auth-error').classList.add('hidden');
  $('auth-status-msg').classList.add('hidden');

  initTerminal();
  state.term.clear();
  state.term.writeln('\x1b[90m── Starting Antigravity CLI auth session… ──\x1b[0m\r\n');

  // Close any existing WebSocket
  if (state.termWs) {
    state.termWs.close();
    state.termWs = null;
  }

  const wsProto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const ws = new WebSocket(`${wsProto}//${location.host}/auth/terminal`);
  ws.binaryType = 'arraybuffer';
  state.termWs = ws;

  ws.onopen = () => {
    setTermStatus('Connected', 'connected');
    btn.textContent = 'Terminal Running…';
    // Send initial terminal size
    const { cols, rows } = state.term;
    ws.send(JSON.stringify({ type: 'resize', cols, rows }));
  };

  ws.onmessage = (e) => {
    if (e.data instanceof ArrayBuffer) {
      // Raw PTY bytes → feed directly to xterm.js
      state.term.write(new Uint8Array(e.data));
    } else {
      // JSON control message
      let msg;
      try { msg = JSON.parse(e.data); } catch { return; }
      handleAuthControl(msg, btn);
    }
  };

  ws.onerror = () => {
    setTermStatus('Error');
    state.term.writeln('\r\n\x1b[31mWebSocket connection error.\x1b[0m');
    btn.disabled = false;
    btn.textContent = 'Retry';
  };

  ws.onclose = () => {
    if (!state.pipelineDone) {
      setTermStatus('Closed');
    }
    btn.disabled = false;
    btn.textContent = 'Start Terminal & Sign In';
  };

  // Keyboard input → PTY
  state.term.onData(data => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(new TextEncoder().encode(data));
    }
  });
}

function handleAuthControl(msg, btn) {
  switch (msg.type) {
    case 'auth_url': {
      // Show the URL prominently as a big clickable button
      const banner = $('auth-url-banner');
      const link   = $('auth-url-link');
      link.href        = msg.url;
      link.textContent = '🌐 Open Login Page';
      link.title       = msg.url;
      banner.classList.remove('hidden');
      // Also write to terminal for visibility
      state.term.writeln(`\r\n\x1b[32m✓ Login URL detected — click the button above!\x1b[0m`);
      break;
    }
    case 'auth_complete':
      setTermStatus('Auth complete', 'done');
      setAuthBadge('online');
      showStatusMsg('✓ Signed in successfully! Redirecting…');
      if (state.termWs) { state.termWs.close(); state.termWs = null; }
      setTimeout(() => showView('view-upload'), 1200);
      break;

    case 'auth_timeout':
      setTermStatus('Timeout');
      showError('auth-error', 'Authentication timed out (7 min). Please try again.');
      btn.disabled = false;
      btn.textContent = 'Retry';
      break;

    case 'error':
      setTermStatus('Error');
      if (msg.message) state.term.write(msg.message);
      showError('auth-error', 'See terminal for details.');
      btn.disabled = false;
      btn.textContent = 'Retry';
      break;

    case 'closed':
      setTermStatus('Closed');
      state.term.writeln('\r\n\x1b[90m── agy session ended ──\x1b[0m');
      break;
  }
}

function showStatusMsg(text) {
  const el = $('auth-status-msg');
  el.textContent = text;
  el.classList.remove('hidden');
}

// ─── File Upload ──────────────────────────────────────────────────────────────
function onDragOver(e)  { e.preventDefault(); $('drop-zone').classList.add('dragover'); }
function onDragLeave()  { $('drop-zone').classList.remove('dragover'); }
function onDrop(e) {
  e.preventDefault();
  $('drop-zone').classList.remove('dragover');
  const file = e.dataTransfer.files[0];
  if (file) applyFile(file);
}
function onFileSelected(e) {
  const file = e.target.files[0];
  if (file) applyFile(file);
}

function applyFile(file) {
  if (!file.name.match(/\.(ya?ml)$/i)) {
    showError('upload-error', 'Only .yml / .yaml files are supported.');
    return;
  }
  state.selectedFile = file;
  $('file-name').textContent = file.name;
  $('file-size').textContent = `${(file.size / 1024).toFixed(1)} KB`;
  $('file-preview').classList.remove('hidden');
  $('btn-analyze').disabled = false;
  $('upload-error').classList.add('hidden');
}

function clearFile() {
  state.selectedFile = null;
  $('file-input').value = '';
  $('file-preview').classList.add('hidden');
  $('btn-analyze').disabled = true;
}

async function startPipeline() {
  if (!state.selectedFile) return;

  const btn = $('btn-analyze');
  btn.disabled = true;
  btn.textContent = 'Uploading…';
  $('upload-error').classList.add('hidden');

  const form = new FormData();
  form.append('file', state.selectedFile);

  let res;
  try {
    res = await fetch('/upload', { method: 'POST', body: form });
  } catch (e) {
    showError('upload-error', `Upload failed: ${e.message}`);
    btn.disabled = false;
    btn.textContent = 'Analyze & Fix';
    return;
  }

  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: 'Unknown error' }));
    showError('upload-error', err.detail || 'Upload failed');
    btn.disabled = false;
    btn.textContent = 'Analyze & Fix';
    return;
  }

  const { session_id } = await res.json();
  state.sessionId    = session_id;
  state.pipelineDone = false;

  resetPipelineUI();
  showView('view-pipeline');
  openPipelineStream(session_id);

  btn.disabled = false;
  btn.textContent = 'Analyze & Fix';
}

// ─── Pipeline Streaming ───────────────────────────────────────────────────────
function openPipelineStream(sessionId) {
  if (state.pipelineEs) state.pipelineEs.close();

  const es = new EventSource(`/pipeline/${sessionId}/stream`);
  state.pipelineEs = es;

  es.onmessage = (e) => {
    if (!e.data || e.data.trim() === '') return;
    let evt;
    try { evt = JSON.parse(e.data); } catch { return; }
    if (evt.stage === 'STREAM_END') { es.close(); return; }
    handlePipelineEvent(evt);
  };

  es.onerror = () => {
    if (!state.pipelineDone) {
      appendLog('ERROR', 'error', 'Connection lost. Check the server.');
      es.close();
    }
  };
}

// ─── Pipeline Event Handler ───────────────────────────────────────────────────
function handlePipelineEvent(evt) {
  const { stage, status, message } = evt;

  appendLog(stage, mapStatus(status), message || status);
  updateStage(stage, status);

  if (evt.issues && evt.issues.length > 0) {
    appendIssues(evt.issues, stage.includes('CHECKOV') ? 'checkov' : 'lint', stage);
  }
  if (evt.diff && evt.diff.length > 0) {
    renderDiff(evt.diff);
    switchTab('diff');
  }
  if (evt.logs && stage === 'DEPLOY_TEST') {
    appendDeployLogs(evt.logs);
  }

  if (stage === 'PIPELINE') {
    state.pipelineDone = true;
    if (state.pipelineEs) state.pipelineEs.close();
    if (status === 'success') {
      setStageState('PIPELINE', 'done');
      $('download-section').classList.remove('hidden');
      $('new-run-section').classList.remove('hidden');
    } else if (status === 'failed') {
      setStageState('PIPELINE', 'failed');
      $('new-run-section').classList.remove('hidden');
    }
  }
}

function mapStatus(status) {
  if (!status) return 'info';
  if (status.includes('run')) return 'running';
  if (status === 'done' || status === 'passed' || status === 'success') return 'done';
  if (status.includes('issue') || status.includes('warn')) return 'issues';
  if (status === 'applied') return 'applied';
  if (status.includes('fail')) return 'failed';
  if (status.includes('error')) return 'error';
  return 'info';
}

// ─── Log Feed ─────────────────────────────────────────────────────────────────
function appendLog(stage, type, message) {
  const feed = $('log-feed');
  const entry = document.createElement('div');
  entry.className = `log-entry type-${type}`;
  entry.innerHTML = `
    <span class="log-time">${fmt_time()}</span>
    <span class="log-stage">${escHtml(stage)}</span>
    <span class="log-msg">${escHtml(message)}</span>
  `;
  feed.appendChild(entry);
  if ($('scroll-lock').checked) feed.scrollTop = feed.scrollHeight;
}

function appendDeployLogs(logs) {
  const feed = $('log-feed');
  const block = document.createElement('div');
  block.className = 'log-entry type-info';
  block.style.cssText = 'flex-direction:column;background:var(--surface2);border-radius:6px;padding:8px 12px;margin:4px 0;font-family:monospace;';
  block.innerHTML = `<span style="color:var(--text-muted);font-size:11px;margin-bottom:4px;">── Deploy output ──</span>
    <pre style="white-space:pre-wrap;color:var(--text);font-size:11px;">${escHtml(logs.slice(0, 2000))}</pre>`;
  feed.appendChild(block);
  if ($('scroll-lock').checked) feed.scrollTop = feed.scrollHeight;
}

// ─── Stage Stepper ────────────────────────────────────────────────────────────
const STAGE_MAP = {
  'INIT': 'INIT', 'STAGE_1': 'STAGE_1',
  'LINT_INITIAL': 'STAGE_1', 'CHECKOV_INITIAL': 'STAGE_1',
  'STAGE_2': 'STAGE_2', 'AGY_FIX': 'STAGE_2',
  'STAGE_3_VALIDATE': 'STAGE_3_VALIDATE',
  'LINT_RECHECK': 'STAGE_3_VALIDATE', 'CHECKOV_RECHECK': 'STAGE_3_VALIDATE',
  'VALIDATE_AFTER_DEPLOY_FIX': 'STAGE_3_VALIDATE',
  'DEPLOY_TEST': 'DEPLOY_TEST', 'AGY_DEPLOY_FIX': 'DEPLOY_TEST',
  'PIPELINE': 'PIPELINE',
};

function updateStage(backendStage, status) {
  const mapped = STAGE_MAP[backendStage];
  if (!mapped) return;
  if      (status === 'running')                       setStageState(mapped, 'running');
  else if (status === 'done' || status === 'passed')   setStageState(mapped, 'done');
  else if (status.includes('issue') || status === 'applied') setStageState(mapped, 'warning');
  else if (status.includes('fail') || status.includes('error')) setStageState(mapped, 'failed');
  else if (status === 'success')                       setStageState(mapped, 'done');
}

function setStageState(key, cls) {
  const item = document.querySelector(`.stage-item[data-stage="${key}"]`);
  if (!item) return;
  item.classList.remove('running', 'done', 'warning', 'failed');
  item.classList.add(cls);
}

// ─── Issues Panel ─────────────────────────────────────────────────────────────
function appendIssues(issues, tool, stage) {
  const container = $('issues-content');
  const placeholder = container.querySelector('.muted');
  if (placeholder) placeholder.remove();

  const header = document.createElement('div');
  header.className = 'issue-count';
  header.innerHTML = `<strong>${issues.length}</strong> ${tool === 'checkov' ? 'Checkov' : 'Lint'} issue(s) — ${stage}`;
  container.appendChild(header);

  issues.slice(0, 30).forEach(issue => {
    const card = document.createElement('div');
    card.className = `issue-card ${tool}`;
    let id = '', desc = '', loc = '';
    if (tool === 'lint') {
      id   = issue.rule?.id || '';
      desc = issue.rule?.description || issue.message || '';
      const l = issue.location || {};
      loc  = l.path ? `${l.path}:${l.lines?.begin?.line || '?'}` : '';
    } else {
      id   = issue.check_id || '';
      desc = issue.check_class || issue.resource || '';
      loc  = issue.file_path || '';
    }
    card.innerHTML = `
      <div style="display:flex;justify-content:space-between;align-items:flex-start;">
        <span class="issue-id">${escHtml(id)}</span>
        <span class="issue-tool">${tool}</span>
      </div>
      <div class="issue-msg">${escHtml(desc)}</div>
      ${loc ? `<div class="issue-loc">${escHtml(loc)}</div>` : ''}
    `;
    container.appendChild(card);
  });

  switchTab('issues');
  container.scrollTop = container.scrollHeight;
}

// ─── Diff Viewer ──────────────────────────────────────────────────────────────
function renderDiff(diffLines) {
  const container = $('diff-content');
  container.innerHTML = '';
  diffLines.forEach(line => {
    const el = document.createElement('div');
    el.className = `diff-line ${line.type}`;
    const prefix = line.type === 'add' ? '+' : line.type === 'remove' ? '-' : line.type === 'hunk' ? '@' : ' ';
    el.innerHTML = `<span class="diff-prefix">${prefix}</span><span>${escHtml(line.content)}</span>`;
    container.appendChild(el);
  });
}

// ─── Tabs ─────────────────────────────────────────────────────────────────────
function switchTab(name) {
  document.querySelectorAll('.tab-btn').forEach((b, i) => {
    b.classList.toggle('active', (i === 0 && name === 'issues') || (i === 1 && name === 'diff'));
  });
  $('tab-issues').classList.toggle('active', name === 'issues');
  $('tab-diff').classList.toggle('active',   name === 'diff');
}

// ─── Download ─────────────────────────────────────────────────────────────────
function downloadFixed() {
  if (!state.sessionId) return;
  const a = document.createElement('a');
  a.href = `/pipeline/${state.sessionId}/download`;
  a.download = 'fixed_playbook.yml';
  a.click();
}

// ─── Reset ────────────────────────────────────────────────────────────────────
function resetToUpload() {
  if (state.pipelineEs) state.pipelineEs.close();
  state.sessionId    = null;
  state.pipelineDone = false;
  clearFile();
  showView('view-upload');
}

function resetPipelineUI() {
  $('log-feed').innerHTML = '';
  $('issues-content').innerHTML = '<p class="muted center">No issues yet.</p>';
  $('diff-content').innerHTML   = '<p class="muted center">No diff yet.</p>';
  $('download-section').classList.add('hidden');
  $('new-run-section').classList.add('hidden');
  document.querySelectorAll('.stage-item').forEach(el =>
    el.classList.remove('running', 'done', 'warning', 'failed'));
}

// ─── Utilities ────────────────────────────────────────────────────────────────
function showError(id, msg) {
  const el = $(id);
  el.textContent = msg;
  el.classList.remove('hidden');
}

function escHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// ─── Init ─────────────────────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  checkAuth();
});
