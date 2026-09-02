/**
 * app.js — Antigravity Playbook Fixer
 * Handles auth, upload, pipeline SSE streaming, and all UI updates.
 */

'use strict';

// ─── State ────────────────────────────────────────────────────────────────────
const state = {
  sessionId: null,
  authEventSource: null,
  pipelineEventSource: null,
  selectedFile: null,
  pipelineDone: false,
};

// ─── Helpers ──────────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const fmt_time = () => new Date().toLocaleTimeString('en', { hour12: false });

function showView(id) {
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  $(id).classList.add('active');
}

function setAuthBadge(status) {
  const badge = $('auth-badge');
  badge.className = 'badge';
  if (status === 'online') {
    badge.classList.add('badge-online');
    badge.textContent = 'Signed in';
  } else if (status === 'checking') {
    badge.classList.add('badge-checking');
    badge.textContent = 'Checking…';
  } else {
    badge.classList.add('badge-offline');
    badge.textContent = 'Not signed in';
  }
}

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
    }
  } catch (e) {
    setAuthBadge('offline');
    showView('view-signin');
  }
}

function startSignIn() {
  const btn = $('btn-signin');
  btn.disabled = true;
  btn.textContent = 'Opening auth stream…';
  $('auth-url-box').classList.add('hidden');
  $('auth-error').classList.add('hidden');

  // Close any existing stream
  if (state.authEventSource) state.authEventSource.close();

  const es = new EventSource('/auth/login/stream');
  state.authEventSource = es;

  es.onmessage = (e) => {
    let data;
    try { data = JSON.parse(e.data); } catch { return; }

    if (data.stage === 'STREAM_END') return;

    if (data.type === 'AUTH_URL') {
      $('auth-url-box').classList.remove('hidden');
      const link = $('auth-url-link');
      link.href = data.value;
      link.textContent = data.value;
      btn.textContent = 'Waiting for sign-in…';
    } else if (data.type === 'AUTH_COMPLETE') {
      es.close();
      setAuthBadge('online');
      btn.disabled = false;
      btn.textContent = 'Sign in with Antigravity CLI';
      showView('view-upload');
    } else if (data.type === 'AUTH_TIMEOUT') {
      es.close();
      showError('auth-error', 'Authentication timed out. Please try again.');
      btn.disabled = false;
      btn.textContent = 'Sign in with Antigravity CLI';
    }
  };

  es.onerror = () => {
    es.close();
    showError('auth-error', 'Connection error. Is the server running?');
    btn.disabled = false;
    btn.textContent = 'Sign in with Antigravity CLI';
  };
}

// ─── File Upload ──────────────────────────────────────────────────────────────
function onDragOver(e) {
  e.preventDefault();
  $('drop-zone').classList.add('dragover');
}
function onDragLeave(e) {
  $('drop-zone').classList.remove('dragover');
}
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
  state.sessionId = session_id;
  state.pipelineDone = false;

  // Switch to pipeline view and start streaming
  resetPipelineUI();
  showView('view-pipeline');
  openPipelineStream(session_id);

  btn.disabled = false;
  btn.textContent = 'Analyze & Fix';
}

// ─── Pipeline Streaming ───────────────────────────────────────────────────────
function openPipelineStream(sessionId) {
  if (state.pipelineEventSource) state.pipelineEventSource.close();

  const es = new EventSource(`/pipeline/${sessionId}/stream`);
  state.pipelineEventSource = es;

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

// ─── Event Handler ────────────────────────────────────────────────────────────
function handlePipelineEvent(evt) {
  const { stage, status, message } = evt;

  // ── Log entry ──────────────────────────────────────────────────────────────
  const logType = mapStatus(status);
  appendLog(stage, logType, message || status);

  // ── Stage stepper ──────────────────────────────────────────────────────────
  updateStage(stage, status);

  // ── Issues panel ───────────────────────────────────────────────────────────
  if (evt.issues && evt.issues.length > 0) {
    const tool = stage.includes('CHECKOV') ? 'checkov' : 'lint';
    appendIssues(evt.issues, tool, stage);
  }

  // ── Diff panel ─────────────────────────────────────────────────────────────
  if (evt.diff && evt.diff.length > 0) {
    renderDiff(evt.diff);
    switchTab('diff');
  }

  // ── Deploy logs ────────────────────────────────────────────────────────────
  if (evt.logs && (stage === 'DEPLOY_TEST')) {
    appendDeployLogs(evt.logs);
  }

  // ── Final states ───────────────────────────────────────────────────────────
  if (stage === 'PIPELINE') {
    state.pipelineDone = true;
    if (state.pipelineEventSource) state.pipelineEventSource.close();

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
  if (status === 'success') return 'success';
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

  if ($('scroll-lock').checked) {
    feed.scrollTop = feed.scrollHeight;
  }
}

function appendDeployLogs(logs) {
  const feed = $('log-feed');
  const block = document.createElement('div');
  block.className = 'log-entry type-info';
  block.style.cssText = 'flex-direction:column; background: var(--surface2); border-radius: 6px; padding: 8px 12px; margin: 4px 0; font-family: monospace;';
  block.innerHTML = `<span style="color:var(--text-muted);font-size:11px;margin-bottom:4px;">── Deploy output ──</span>
    <pre style="white-space:pre-wrap;color:var(--text);font-size:11px;">${escHtml(logs.slice(0, 2000))}</pre>`;
  feed.appendChild(block);
  if ($('scroll-lock').checked) feed.scrollTop = feed.scrollHeight;
}

// ─── Stage Stepper ────────────────────────────────────────────────────────────
// Map backend stage names → stepper item data-stage values
const STAGE_MAP = {
  'INIT':                  'INIT',
  'STAGE_1':               'STAGE_1',
  'LINT_INITIAL':          'STAGE_1',
  'CHECKOV_INITIAL':       'STAGE_1',
  'STAGE_2':               'STAGE_2',
  'AGY_FIX':               'STAGE_2',
  'STAGE_3_VALIDATE':      'STAGE_3_VALIDATE',
  'LINT_RECHECK':          'STAGE_3_VALIDATE',
  'CHECKOV_RECHECK':       'STAGE_3_VALIDATE',
  'VALIDATE_AFTER_DEPLOY_FIX': 'STAGE_3_VALIDATE',
  'DEPLOY_TEST':           'DEPLOY_TEST',
  'AGY_DEPLOY_FIX':        'DEPLOY_TEST',
  'PIPELINE':              'PIPELINE',
};

function updateStage(backendStage, status) {
  const mapped = STAGE_MAP[backendStage];
  if (!mapped) return;

  const item = document.querySelector(`.stage-item[data-stage="${mapped}"]`);
  if (!item) return;

  if (status === 'running') {
    setStageState(mapped, 'running');
  } else if (status === 'done' || status === 'passed') {
    setStageState(mapped, 'done');
  } else if (status === 'issues_found' || status === 'applied') {
    setStageState(mapped, 'warning');
  } else if (status.includes('fail') || status.includes('error')) {
    setStageState(mapped, 'failed');
  } else if (status === 'success') {
    setStageState(mapped, 'done');
  }
}

function setStageState(stageKey, state) {
  const item = document.querySelector(`.stage-item[data-stage="${stageKey}"]`);
  if (!item) return;
  item.classList.remove('running', 'done', 'warning', 'failed');
  item.classList.add(state);
}

// ─── Issues Panel ─────────────────────────────────────────────────────────────
function appendIssues(issues, tool, stage) {
  const container = $('issues-content');

  // Clear placeholder
  const placeholder = container.querySelector('.muted');
  if (placeholder) placeholder.remove();

  // Section header
  const header = document.createElement('div');
  header.className = 'issue-count';
  header.innerHTML = `<strong>${issues.length}</strong> ${tool === 'checkov' ? 'Checkov' : 'Lint'} issue(s) — ${stage}`;
  container.appendChild(header);

  issues.slice(0, 30).forEach(issue => {
    const card = document.createElement('div');
    card.className = `issue-card ${tool}`;

    let id = '', desc = '', loc = '';
    if (tool === 'lint') {
      id = issue.rule?.id || '';
      desc = issue.rule?.description || issue.message || '';
      const l = issue.location || {};
      loc = l.path ? `${l.path}:${l.lines?.begin?.line || '?'}` : '';
    } else {
      id = issue.check_id || '';
      desc = issue.check_class || issue.resource || '';
      loc = issue.file_path || '';
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

// ─── Tab switching ────────────────────────────────────────────────────────────
function switchTab(name) {
  document.querySelectorAll('.tab-btn').forEach((b, i) => {
    b.classList.toggle('active', (i === 0 && name === 'issues') || (i === 1 && name === 'diff'));
  });
  $('tab-issues').classList.toggle('active', name === 'issues');
  $('tab-diff').classList.toggle('active', name === 'diff');
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
  if (state.pipelineEventSource) state.pipelineEventSource.close();
  state.sessionId = null;
  state.pipelineDone = false;
  clearFile();
  showView('view-upload');
}

function resetPipelineUI() {
  $('log-feed').innerHTML = '';
  $('issues-content').innerHTML = '<p class="muted center">No issues yet.</p>';
  $('diff-content').innerHTML = '<p class="muted center">No diff yet.</p>';
  $('download-section').classList.add('hidden');
  $('new-run-section').classList.add('hidden');
  document.querySelectorAll('.stage-item').forEach(el => {
    el.classList.remove('running', 'done', 'warning', 'failed');
  });
}

// ─── Utility ──────────────────────────────────────────────────────────────────
function showError(id, msg) {
  const el = $(id);
  el.textContent = msg;
  el.classList.remove('hidden');
}

function escHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ─── Init ─────────────────────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  checkAuth();
});
