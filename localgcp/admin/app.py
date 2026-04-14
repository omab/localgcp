"""Admin UI for LocalGCP.

Provides a web dashboard showing the state of all emulated services,
allowing data to be browsed, interacted with, and reset.
"""
from __future__ import annotations

import base64
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from localgcp.config import settings
from localgcp.core.middleware import add_request_logging

app = FastAPI(title="LocalGCP Admin", version="v1")
add_request_logging(app, "admin")

_STATIC = Path(__file__).parent / "static"
if _STATIC.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")


# ---------------------------------------------------------------------------
# Stats helper (used by overview + /api/stats)
# ---------------------------------------------------------------------------


def _get_services() -> dict:
    from localgcp.services.gcs.store import get_store as gcs_store
    from localgcp.services.pubsub.store import get_store as pubsub_store
    from localgcp.services.firestore.store import get_store as firestore_store
    from localgcp.services.secretmanager.store import get_store as sm_store
    from localgcp.services.tasks.store import get_store as tasks_store

    from localgcp.services.bigquery.engine import get_engine as bq_get_engine
    bq = bq_get_engine()
    bq_datasets = bq.list_datasets(settings.default_project)
    bq_table_count = sum(
        len(bq.list_tables(settings.default_project, d["datasetReference"]["datasetId"]))
        for d in bq_datasets
    )

    return {
        "gcs": {
            "port": settings.gcs_port,
            "stats": gcs_store().stats(),
            "docs_url": f"http://localhost:{settings.gcs_port}/docs",
        },
        "pubsub": {
            "port": settings.pubsub_rest_port,
            "stats": pubsub_store().stats(),
            "docs_url": f"http://localhost:{settings.pubsub_rest_port}/docs",
            "grpc_port": settings.pubsub_port,
        },
        "firestore": {
            "port": settings.firestore_port,
            "stats": firestore_store().stats(),
            "docs_url": f"http://localhost:{settings.firestore_port}/docs",
        },
        "secretmanager": {
            "port": settings.secretmanager_port,
            "stats": sm_store().stats(),
            "docs_url": f"http://localhost:{settings.secretmanager_port}/docs",
        },
        "tasks": {
            "port": settings.tasks_port,
            "stats": tasks_store().stats(),
            "docs_url": f"http://localhost:{settings.tasks_port}/docs",
        },
        "bigquery": {
            "port": settings.bigquery_port,
            "stats": {"datasets": len(bq_datasets), "tables": bq_table_count},
            "docs_url": f"http://localhost:{settings.bigquery_port}/docs",
        },
    }


# ---------------------------------------------------------------------------
# Dashboard HTML (static — no f-string escaping needed)
# ---------------------------------------------------------------------------

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>LocalGCP Admin</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: system-ui, sans-serif; background: #f0f4f9; color: #202124; min-height: 100vh; }

    header {
      background: #1a73e8; color: white;
      padding: .85rem 2rem; display: flex; align-items: center; gap: 1rem;
    }
    header h1 { font-size: 1.25rem; font-weight: 500; }
    header .sub { font-size: .8rem; opacity: .85; margin-top: .15rem; }
    .badge { background: #34a853; border-radius: 10px; padding: .15rem .65rem; font-size: .75rem; font-weight: 600; }

    nav {
      background: white; border-bottom: 1px solid #dadce0;
      padding: 0 2rem; display: flex; overflow-x: auto;
    }
    nav button {
      padding: .8rem 1.1rem; border: none; background: none; cursor: pointer;
      font-size: .875rem; color: #5f6368; border-bottom: 3px solid transparent;
      white-space: nowrap; transition: color .15s, border-color .15s;
    }
    nav button:hover { color: #1a73e8; background: #f8f9fa; }
    nav button.active { color: #1a73e8; border-bottom-color: #1a73e8; font-weight: 500; }

    main { padding: 1.5rem 2rem; max-width: 1200px; }

    .panel { display: none; }
    .panel.active { display: block; }

    .card {
      background: white; border-radius: 8px;
      box-shadow: 0 1px 3px rgba(0,0,0,.1); overflow: hidden; margin-bottom: 1.5rem;
    }
    .card-header {
      padding: .75rem 1rem; border-bottom: 1px solid #f1f3f4;
      display: flex; align-items: center; gap: .75rem;
    }
    .card-header h2 { font-size: .95rem; font-weight: 500; }

    table { width: 100%; border-collapse: collapse; }
    th, td { padding: .6rem 1rem; text-align: left; border-bottom: 1px solid #f1f3f4; font-size: .875rem; }
    th { background: #f8f9fa; color: #5f6368; font-weight: 500; font-size: .78rem; text-transform: uppercase; letter-spacing: .04em; }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: #fafafa; }

    .btn {
      display: inline-flex; align-items: center; gap: .3rem;
      padding: .28rem .7rem; border: 1px solid; border-radius: 4px;
      cursor: pointer; font-size: .8rem; font-weight: 500; font-family: inherit;
      text-decoration: none; transition: background .12s, color .12s;
    }
    .btn-primary { background: #1a73e8; color: white; border-color: #1a73e8; }
    .btn-primary:hover { background: #1557b0; }
    .btn-danger { background: white; color: #ea4335; border-color: #ea4335; }
    .btn-danger:hover { background: #fce8e6; }
    .btn-secondary { background: white; color: #1a73e8; border-color: #1a73e8; }
    .btn-secondary:hover { background: #e8f0fe; }
    .btn-ghost { background: white; color: #5f6368; border-color: #dadce0; }
    .btn-ghost:hover { background: #f1f3f4; }
    .btn-link {
      background: none; border: none; color: #1a73e8; cursor: pointer;
      font-size: .875rem; font-family: inherit; padding: 0; font-weight: 500;
    }
    .btn-link:hover { text-decoration: underline; }

    .cnt { display: inline-block; background: #e8f0fe; color: #1a73e8; border-radius: 10px; padding: .1rem .5rem; font-size: .75rem; font-weight: 600; }
    .state { display: inline-block; border-radius: 10px; padding: .1rem .5rem; font-size: .75rem; font-weight: 500; }
    .s-running, .s-enabled  { background: #e6f4ea; color: #137333; }
    .s-paused, .s-disabled  { background: #fef3e2; color: #e37400; }
    .s-destroyed            { background: #fce8e6; color: #c5221f; }

    .breadcrumb { font-size: .85rem; color: #5f6368; margin-bottom: 1rem; display: flex; align-items: center; gap: .3rem; }
    .breadcrumb a { color: #1a73e8; cursor: pointer; }
    .breadcrumb a:hover { text-decoration: underline; }

    .empty  { padding: 2rem; text-align: center; color: #9aa0a6; font-size: .875rem; }
    .loading { padding: 2rem; text-align: center; color: #9aa0a6; }

    .actions { display: flex; gap: .35rem; flex-wrap: wrap; }

    .mono { font-family: 'Roboto Mono', monospace, monospace; font-size: .82rem; }
    .dim  { color: #5f6368; font-size: .8rem; }

    .sub-table { background: #f8f9fa; }
    .sub-table td { border-bottom: 1px solid #ececec; }
    .sub-table tr:last-child td { border-bottom: none; }
    .sub-table th { background: #f0f0f0; }

    .pre { background: #f8f9fa; border: 1px solid #dadce0; border-radius: 4px; padding: .6rem .8rem; font-family: monospace; font-size: .8rem; white-space: pre-wrap; word-break: break-all; max-height: 160px; overflow-y: auto; }
    .secret-val { margin: .5rem 1rem .75rem; }

    /* Modal */
    .overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,.45); z-index: 200; align-items: center; justify-content: center; }
    .overlay.open { display: flex; }
    .modal { background: white; border-radius: 8px; padding: 1.5rem; width: min(520px, 95vw); box-shadow: 0 4px 24px rgba(0,0,0,.2); }
    .modal h3 { margin-bottom: .75rem; font-size: 1rem; }
    .modal label { display: block; font-size: .82rem; color: #5f6368; margin: .75rem 0 .3rem; }
    .modal input, .modal textarea {
      width: 100%; padding: .45rem .75rem; border: 1px solid #dadce0; border-radius: 4px;
      font-size: .875rem; font-family: inherit;
    }
    .modal textarea { min-height: 80px; resize: vertical; }
    .modal-actions { margin-top: 1.25rem; display: flex; gap: .5rem; justify-content: flex-end; }

    footer { margin-top: 1.5rem; color: #9aa0a6; font-size: .8rem; }
  </style>
</head>
<body>

<header>
  <div>
    <h1>&#x1F680; LocalGCP Admin</h1>
    <div class="sub">Project: <code id="project-id">loading&hellip;</code></div>
  </div>
  <span style="margin-left:auto" class="badge">running</span>
</header>

<nav>
  <button class="tab-btn active" data-tab="overview">Overview</button>
  <button class="tab-btn" data-tab="gcs">Cloud Storage</button>
  <button class="tab-btn" data-tab="pubsub">Pub/Sub</button>
  <button class="tab-btn" data-tab="firestore">Firestore</button>
  <button class="tab-btn" data-tab="secrets">Secret Manager</button>
  <button class="tab-btn" data-tab="tasks">Cloud Tasks</button>
  <button class="tab-btn" data-tab="bigquery">BigQuery</button>
</nav>

<main>

<div id="panel-overview" class="panel active">
  <div id="overview-content"><div class="loading">Loading&hellip;</div></div>
  <div style="margin-top:1rem">
    <button class="btn btn-ghost" onclick="resetAll()">Reset All Data</button>
  </div>
  <footer>LocalGCP &mdash; local emulator for GCP services</footer>
</div>

<div id="panel-gcs" class="panel">
  <div id="gcs-nav" class="breadcrumb" style="display:none"></div>
  <div id="gcs-content"><div class="loading">Loading&hellip;</div></div>
</div>

<div id="panel-pubsub" class="panel">
  <div id="pubsub-content"><div class="loading">Loading&hellip;</div></div>
</div>

<div id="panel-firestore" class="panel">
  <div id="firestore-nav" class="breadcrumb" style="display:none"></div>
  <div id="firestore-content"><div class="loading">Loading&hellip;</div></div>
</div>

<div id="panel-secrets" class="panel">
  <div id="secrets-content"><div class="loading">Loading&hellip;</div></div>
</div>

<div id="panel-tasks" class="panel">
  <div id="tasks-nav" class="breadcrumb" style="display:none"></div>
  <div id="tasks-content"><div class="loading">Loading&hellip;</div></div>
</div>

<div id="panel-bigquery" class="panel">
  <div id="bq-nav" class="breadcrumb" style="display:none"></div>
  <div id="bq-content"><div class="loading">Loading&hellip;</div></div>
</div>

</main>

<!-- Publish modal -->
<div class="overlay" id="publish-overlay">
  <div class="modal">
    <h3>Publish Message</h3>
    <div class="dim" style="margin-bottom:.5rem">Topic: <code id="pub-topic-label"></code></div>
    <label>Message data (plain text)</label>
    <textarea id="pub-data" rows="3" placeholder="Enter message content&hellip;"></textarea>
    <label>Attributes (JSON object, optional)</label>
    <textarea id="pub-attrs" rows="2" placeholder='{"key": "value"}'></textarea>
    <div class="modal-actions">
      <button class="btn btn-ghost" onclick="closeOverlay('publish-overlay')">Cancel</button>
      <button class="btn btn-primary" onclick="doPublish()">Publish</button>
    </div>
  </div>
</div>

<script>
// ── State ────────────────────────────────────────────────────────────────────
const loaded = {};
let _publishTopic = '';

// ── Utilities ────────────────────────────────────────────────────────────────
const $  = id => document.getElementById(id);
const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

function shortName(path) {
  const parts = String(path).split('/');
  return parts[parts.length - 1];
}

function humanSize(bytes) {
  const n = parseInt(bytes, 10);
  if (isNaN(n)) return bytes || '0 B';
  if (n < 1024)    return n + ' B';
  if (n < 1048576) return (n / 1024).toFixed(1) + ' KB';
  return (n / 1048576).toFixed(1) + ' MB';
}

function stateClass(s) {
  const m = { RUNNING:'running', PAUSED:'paused', ENABLED:'enabled', DISABLED:'disabled', DESTROYED:'destroyed' };
  return 's-' + (m[String(s).toUpperCase()] || 'disabled');
}

async function api(url, opts = {}) {
  const r = await fetch(url, opts);
  if (!r.ok) {
    const txt = await r.text().catch(() => r.statusText);
    throw new Error(txt || r.statusText);
  }
  return r.status === 204 ? null : r.json();
}

// ── Overlay / Modal ──────────────────────────────────────────────────────────
function openOverlay(id)  { $(id).classList.add('open'); }
function closeOverlay(id) { $(id).classList.remove('open'); }

// ── Tabs ─────────────────────────────────────────────────────────────────────
const loaders = {
  overview: loadOverview,
  gcs:      loadGCS,
  pubsub:   loadPubSub,
  firestore: loadFirestore,
  secrets:  loadSecrets,
  tasks:    loadTasks,
  bigquery: loadBigQuery,
};

document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    const tab = btn.dataset.tab;
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    $('panel-' + tab).classList.add('active');
    if (!loaded[tab] || tab === 'overview') {
      loaded[tab] = true;
      loaders[tab]();
    }
  });
});

// ── Overview ─────────────────────────────────────────────────────────────────
async function loadOverview() {
  try {
    const d = await api('/api/stats');
    $('project-id').textContent = d.project || 'local-project';
    const labels = {
      gcs: 'Cloud Storage', pubsub: 'Cloud Pub/Sub',
      firestore: 'Cloud Firestore', secretmanager: 'Secret Manager', tasks: 'Cloud Tasks',
      bigquery: 'BigQuery',
    };
    let rows = '';
    for (const [svc, info] of Object.entries(d.services || {})) {
      const statStr = Object.entries(info.stats || {})
        .map(([k, v]) => `${k}: <b>${v}</b>`).join(', ') || '<em style="color:#9aa0a6">empty</em>';
      let port = `:${info.port}`;
      if (info.grpc_port) port += ` (REST) / :${info.grpc_port} (gRPC)`;
      rows += `<tr>
        <td><strong>${labels[svc] || svc}</strong></td>
        <td class="mono dim">${port}</td>
        <td>${statStr}</td>
        <td class="actions">
          <a href="${info.docs_url}" target="_blank" class="btn btn-ghost">OpenAPI</a>
          <button class="btn btn-danger" onclick="resetService('${svc}')">Reset</button>
        </td>
      </tr>`;
    }
    $('overview-content').innerHTML = `
      <div class="card">
        <div class="card-header"><h2>Services</h2></div>
        <table>
          <thead><tr><th>Service</th><th>Port</th><th>Resources</th><th>Actions</th></tr></thead>
          <tbody>${rows || '<tr><td colspan="4" class="empty">No services</td></tr>'}</tbody>
        </table>
      </div>`;
  } catch (e) {
    $('overview-content').innerHTML = `<div class="empty">Error loading stats: ${esc(e.message)}</div>`;
  }
}

async function resetService(svc) {
  if (!confirm(`Reset all data for ${svc}?`)) return;
  try {
    await api(`/reset/${svc}`, { method: 'POST' });
    loaded[svc] = false;
    loadOverview();
  } catch (e) { alert('Reset failed: ' + e.message); }
}

async function resetAll() {
  if (!confirm('Reset ALL LocalGCP data?')) return;
  try {
    await api('/reset', { method: 'POST' });
    Object.keys(loaded).forEach(k => { loaded[k] = false; });
    loadOverview();
  } catch (e) { alert('Reset failed: ' + e.message); }
}

// ── GCS ──────────────────────────────────────────────────────────────────────
async function loadGCS() {
  $('gcs-nav').style.display = 'none';
  $('gcs-content').innerHTML = '<div class="loading">Loading buckets&hellip;</div>';
  try {
    const buckets = await api('/api/gcs/buckets');
    renderGCSBuckets(buckets);
  } catch (e) {
    $('gcs-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function renderGCSBuckets(buckets) {
  $('gcs-nav').style.display = 'none';
  let rows = '';
  for (const b of buckets) {
    const n = JSON.stringify(b.name);
    rows += `<tr>
      <td><button class="btn-link" onclick="loadGCSObjects(${n})">${esc(b.name)}</button></td>
      <td><span class="cnt">${b.objectCount}</span></td>
      <td class="dim">${b.timeCreated ? b.timeCreated.substring(0, 10) : '&mdash;'}</td>
      <td class="actions">
        <button class="btn btn-danger" onclick="deleteGCSBucket(${n})">Delete</button>
      </td>
    </tr>`;
  }
  $('gcs-content').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>Buckets</h2><span class="cnt">${buckets.length}</span></div>
      <table>
        <thead><tr><th>Bucket</th><th>Objects</th><th>Created</th><th>Actions</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="4" class="empty">No buckets</td></tr>'}</tbody>
      </table>
    </div>`;
}

async function loadGCSObjects(bucket) {
  $('gcs-nav').style.display = 'flex';
  $('gcs-nav').innerHTML = `<a onclick="loadGCS()">Buckets</a> <span>&#8250;</span> ${esc(bucket)}`;
  $('gcs-content').innerHTML = '<div class="loading">Loading objects&hellip;</div>';
  try {
    const objects = await api('/api/gcs/objects?' + new URLSearchParams({ bucket }));
    renderGCSObjects(bucket, objects);
  } catch (e) {
    $('gcs-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function renderGCSObjects(bucket, objects) {
  let rows = '';
  for (const o of objects) {
    const bn = JSON.stringify(bucket);
    const on = JSON.stringify(o.name);
    rows += `<tr>
      <td class="mono">${esc(o.name)}</td>
      <td class="dim">${humanSize(o.size)}</td>
      <td class="dim">${esc(o.contentType || '')}</td>
      <td class="dim">${o.updated ? o.updated.substring(0, 19).replace('T', ' ') : '&mdash;'}</td>
      <td class="actions">
        <button class="btn btn-danger" onclick="deleteGCSObject(${bn}, ${on})">Delete</button>
      </td>
    </tr>`;
  }
  $('gcs-content').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>${esc(bucket)}</h2><span class="cnt">${objects.length} objects</span></div>
      <table>
        <thead><tr><th>Name</th><th>Size</th><th>Content-Type</th><th>Updated</th><th>Actions</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="5" class="empty">No objects</td></tr>'}</tbody>
      </table>
    </div>`;
}

async function deleteGCSBucket(name) {
  if (!confirm(`Delete bucket "${name}" and all its objects?`)) return;
  try {
    await api('/api/gcs/buckets?' + new URLSearchParams({ bucket: name }), { method: 'DELETE' });
    loaded.gcs = false;
    loadGCS();
  } catch (e) { alert('Delete failed: ' + e.message); }
}

async function deleteGCSObject(bucket, name) {
  if (!confirm(`Delete object "${name}"?`)) return;
  try {
    await api('/api/gcs/objects?' + new URLSearchParams({ bucket, name }), { method: 'DELETE' });
    loadGCSObjects(bucket);
  } catch (e) { alert('Delete failed: ' + e.message); }
}

// ── Pub/Sub ───────────────────────────────────────────────────────────────────
async function loadPubSub() {
  $('pubsub-content').innerHTML = '<div class="loading">Loading&hellip;</div>';
  try {
    const [topics, subs] = await Promise.all([
      api('/api/pubsub/topics'),
      api('/api/pubsub/subscriptions'),
    ]);
    renderPubSub(topics, subs);
  } catch (e) {
    $('pubsub-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function renderPubSub(topics, subs) {
  let topicRows = '';
  for (const t of topics) {
    const n = JSON.stringify(t.name);
    topicRows += `<tr>
      <td class="mono">${esc(shortName(t.name))}</td>
      <td><span class="cnt">${t.subscriptionCount}</span></td>
      <td class="dim">${esc(t.name)}</td>
      <td class="actions">
        <button class="btn btn-secondary" onclick="openPublishModal(${n})">Publish</button>
        <button class="btn btn-danger"    onclick="deletePubSubTopic(${n})">Delete</button>
      </td>
    </tr>`;
  }
  let subRows = '';
  for (const s of subs) {
    const n = JSON.stringify(s.name);
    subRows += `<tr>
      <td class="mono">${esc(shortName(s.name))}</td>
      <td class="dim mono">${esc(shortName(s.topic))}</td>
      <td><span class="cnt">${s.queueDepth}</span></td>
      <td class="dim">${s.ackDeadlineSeconds}s</td>
      <td class="actions">
        <button class="btn btn-danger" onclick="deletePubSubSub(${n})">Delete</button>
      </td>
    </tr>`;
  }
  $('pubsub-content').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>Topics</h2><span class="cnt">${topics.length}</span></div>
      <table>
        <thead><tr><th>Name</th><th>Subscriptions</th><th>Full Name</th><th>Actions</th></tr></thead>
        <tbody>${topicRows || '<tr><td colspan="4" class="empty">No topics</td></tr>'}</tbody>
      </table>
    </div>
    <div class="card">
      <div class="card-header"><h2>Subscriptions</h2><span class="cnt">${subs.length}</span></div>
      <table>
        <thead><tr><th>Name</th><th>Topic</th><th>Queue Depth</th><th>Ack Deadline</th><th>Actions</th></tr></thead>
        <tbody>${subRows || '<tr><td colspan="5" class="empty">No subscriptions</td></tr>'}</tbody>
      </table>
    </div>`;
}

function openPublishModal(topic) {
  _publishTopic = topic;
  $('pub-topic-label').textContent = shortName(topic);
  $('pub-data').value = '';
  $('pub-attrs').value = '';
  openOverlay('publish-overlay');
}

async function doPublish() {
  const data = $('pub-data').value;
  const attrsRaw = $('pub-attrs').value.trim();
  let attributes = {};
  if (attrsRaw) {
    try { attributes = JSON.parse(attrsRaw); }
    catch { alert('Attributes must be valid JSON'); return; }
  }
  try {
    const res = await api('/api/pubsub/publish', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ topic: _publishTopic, data, attributes }),
    });
    closeOverlay('publish-overlay');
    alert(`Published!\nMessage ID: ${res.messageId}\nDelivered to ${res.deliveredToSubscriptions} subscription(s)`);
    loaded.pubsub = false;
    loadPubSub();
  } catch (e) { alert('Publish failed: ' + e.message); }
}

async function deletePubSubTopic(name) {
  if (!confirm(`Delete topic "${shortName(name)}" and its subscriptions?`)) return;
  try {
    await api('/api/pubsub/topics?' + new URLSearchParams({ topic: name }), { method: 'DELETE' });
    loaded.pubsub = false;
    loadPubSub();
  } catch (e) { alert('Delete failed: ' + e.message); }
}

async function deletePubSubSub(name) {
  if (!confirm(`Delete subscription "${shortName(name)}"?`)) return;
  try {
    await api('/api/pubsub/subscriptions?' + new URLSearchParams({ subscription: name }), { method: 'DELETE' });
    loaded.pubsub = false;
    loadPubSub();
  } catch (e) { alert('Delete failed: ' + e.message); }
}

// ── Firestore ─────────────────────────────────────────────────────────────────
async function loadFirestore() {
  $('firestore-nav').style.display = 'none';
  $('firestore-content').innerHTML = '<div class="loading">Loading collections&hellip;</div>';
  try {
    const cols = await api('/api/firestore/collections');
    renderFirestoreCollections(cols);
  } catch (e) {
    $('firestore-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function renderFirestoreCollections(cols) {
  $('firestore-nav').style.display = 'none';
  let rows = '';
  for (const c of cols) {
    const n = JSON.stringify(c.name);
    rows += `<tr>
      <td><button class="btn-link" onclick="loadFirestoreDocs(${n})">${esc(c.name)}</button></td>
      <td><span class="cnt">${c.documentCount}</span></td>
    </tr>`;
  }
  $('firestore-content').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>Collections</h2><span class="cnt">${cols.length}</span></div>
      <table>
        <thead><tr><th>Collection</th><th>Documents</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="2" class="empty">No documents stored</td></tr>'}</tbody>
      </table>
    </div>`;
}

async function loadFirestoreDocs(collection) {
  $('firestore-nav').style.display = 'flex';
  $('firestore-nav').innerHTML = `<a onclick="loadFirestore()">Collections</a> <span>&#8250;</span> ${esc(collection)}`;
  $('firestore-content').innerHTML = '<div class="loading">Loading documents&hellip;</div>';
  try {
    const docs = await api('/api/firestore/documents?' + new URLSearchParams({ collection }));
    renderFirestoreDocs(collection, docs);
  } catch (e) {
    $('firestore-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function renderFirestoreDocs(collection, docs) {
  let rows = '';
  for (let i = 0; i < docs.length; i++) {
    const doc = docs[i];
    const docName = doc.name || '';
    const docId = shortName(docName);
    const fieldCount = Object.keys(doc.fields || {}).length;
    const n = JSON.stringify(docName);
    const c = JSON.stringify(collection);
    const fieldsJson = JSON.stringify(doc.fields || {}, null, 2);
    rows += `<tr id="dr-${i}">
      <td class="mono">${esc(docId)}</td>
      <td><span class="cnt">${fieldCount}</span></td>
      <td class="dim">${doc.updateTime ? doc.updateTime.substring(0, 19).replace('T', ' ') : '&mdash;'}</td>
      <td class="actions">
        <button class="btn btn-ghost" id="view-btn-${i}" onclick="toggleDocView(${i}, ${JSON.stringify(fieldsJson)})">View</button>
        <button class="btn btn-danger" onclick="deleteFirestoreDoc(${n}, ${c})">Delete</button>
      </td>
    </tr>
    <tr id="dv-${i}" style="display:none">
      <td colspan="4" style="padding:.5rem 1rem .75rem">
        <div class="pre" id="df-${i}"></div>
      </td>
    </tr>`;
  }
  $('firestore-content').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>${esc(collection)}</h2><span class="cnt">${docs.length} documents</span></div>
      <table>
        <thead><tr><th>Document ID</th><th>Fields</th><th>Updated</th><th>Actions</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="4" class="empty">No documents</td></tr>'}</tbody>
      </table>
    </div>`;
}

function toggleDocView(i, fieldsJson) {
  const viewRow = $('dv-' + i);
  const btn = $('view-btn-' + i);
  if (viewRow.style.display === 'none') {
    $('df-' + i).textContent = fieldsJson;
    viewRow.style.display = '';
    btn.textContent = 'Hide';
  } else {
    viewRow.style.display = 'none';
    btn.textContent = 'View';
  }
}

async function deleteFirestoreDoc(docPath, collection) {
  if (!confirm(`Delete document "${shortName(docPath)}"?`)) return;
  try {
    await api('/api/firestore/documents?' + new URLSearchParams({ path: docPath }), { method: 'DELETE' });
    loadFirestoreDocs(collection);
  } catch (e) { alert('Delete failed: ' + e.message); }
}

// ── Secret Manager ────────────────────────────────────────────────────────────
async function loadSecrets() {
  $('secrets-content').innerHTML = '<div class="loading">Loading secrets&hellip;</div>';
  try {
    const secrets = await api('/api/secretmanager/secrets');
    renderSecrets(secrets);
  } catch (e) {
    $('secrets-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function renderSecrets(secrets) {
  let rows = '';
  for (let i = 0; i < secrets.length; i++) {
    const s = secrets[i];
    const short = shortName(s.name);
    const n = JSON.stringify(short);
    rows += `<tr id="sr-${i}">
      <td class="mono">${esc(short)}</td>
      <td><span class="cnt">${s.versionCount}</span></td>
      <td class="dim">${s.createTime ? s.createTime.substring(0, 10) : '&mdash;'}</td>
      <td class="actions">
        <button class="btn btn-ghost" id="sv-btn-${i}" onclick="toggleSecretVersions(${i}, ${n})">Versions</button>
        <button class="btn btn-danger" onclick="deleteSecret(${n})">Delete</button>
      </td>
    </tr>
    <tr id="sv-${i}" style="display:none">
      <td colspan="4" style="padding:.5rem 1rem .75rem">
        <div id="sv-content-${i}"><div class="loading" style="padding:.5rem">Loading&hellip;</div></div>
      </td>
    </tr>`;
  }
  $('secrets-content').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>Secrets</h2><span class="cnt">${secrets.length}</span></div>
      <table>
        <thead><tr><th>Name</th><th>Versions</th><th>Created</th><th>Actions</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="4" class="empty">No secrets</td></tr>'}</tbody>
      </table>
    </div>`;
}

async function toggleSecretVersions(i, secretName) {
  const row = $('sv-' + i);
  const btn = $('sv-btn-' + i);
  if (row.style.display === 'none') {
    row.style.display = '';
    btn.textContent = 'Hide';
    const container = $('sv-content-' + i);
    try {
      const versions = await api('/api/secretmanager/versions?' + new URLSearchParams({ secret: secretName }));
      if (!versions.length) {
        container.innerHTML = '<div class="empty" style="padding:.5rem">No versions</div>';
        return;
      }
      let vRows = '';
      for (let j = 0; j < versions.length; j++) {
        const v = versions[j];
        const vNum = v.name.split('/versions/').pop();
        const sn = JSON.stringify(secretName);
        const vn = JSON.stringify(vNum);
        vRows += `<tr>
          <td class="mono">v${esc(vNum)}</td>
          <td><span class="state ${stateClass(v.state)}">${v.state}</span></td>
          <td class="dim">${v.createTime ? v.createTime.substring(0, 19).replace('T', ' ') : '&mdash;'}</td>
          <td class="actions">
            ${v.state === 'ENABLED'
              ? `<button class="btn btn-ghost" id="val-btn-${i}-${j}" onclick="toggleSecretValue(${i}, ${j}, ${sn}, ${vn})">Reveal</button>`
              : ''}
          </td>
        </tr>
        <tr id="val-row-${i}-${j}" style="display:none">
          <td colspan="4" class="secret-val"><div class="pre" id="val-pre-${i}-${j}"></div></td>
        </tr>`;
      }
      container.innerHTML = `<table class="sub-table">
        <thead><tr><th>Version</th><th>State</th><th>Created</th><th>Actions</th></tr></thead>
        <tbody>${vRows}</tbody>
      </table>`;
    } catch (e) {
      container.innerHTML = `<div class="empty" style="padding:.5rem">Error: ${esc(e.message)}</div>`;
    }
  } else {
    row.style.display = 'none';
    btn.textContent = 'Versions';
  }
}

async function toggleSecretValue(i, j, secretName, versionId) {
  const valRow = $(`val-row-${i}-${j}`);
  const btn = $(`val-btn-${i}-${j}`);
  if (valRow.style.display === 'none') {
    valRow.style.display = '';
    btn.textContent = 'Hide';
    const pre = $(`val-pre-${i}-${j}`);
    pre.textContent = 'Loading\u2026';
    try {
      const res = await api('/api/secretmanager/value?' + new URLSearchParams({ secret: secretName, version: versionId }));
      pre.textContent = res.value;
    } catch (e) {
      pre.textContent = 'Error: ' + e.message;
    }
  } else {
    valRow.style.display = 'none';
    btn.textContent = 'Reveal';
  }
}

async function deleteSecret(name) {
  if (!confirm(`Delete secret "${name}" and all its versions?`)) return;
  try {
    await api('/api/secretmanager/secrets?' + new URLSearchParams({ secret: name }), { method: 'DELETE' });
    loaded.secrets = false;
    loadSecrets();
  } catch (e) { alert('Delete failed: ' + e.message); }
}

// ── Cloud Tasks ───────────────────────────────────────────────────────────────
async function loadTasks() {
  $('tasks-nav').style.display = 'none';
  $('tasks-content').innerHTML = '<div class="loading">Loading queues&hellip;</div>';
  try {
    const queues = await api('/api/tasks/queues');
    renderTaskQueues(queues);
  } catch (e) {
    $('tasks-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function renderTaskQueues(queues) {
  $('tasks-nav').style.display = 'none';
  let rows = '';
  for (const q of queues) {
    const short = shortName(q.name);
    const n = JSON.stringify(q.name);
    rows += `<tr>
      <td><button class="btn-link" onclick="loadQueueTasks(${n})">${esc(short)}</button></td>
      <td><span class="state ${stateClass(q.state)}">${q.state}</span></td>
      <td><span class="cnt">${q.taskCount}</span></td>
      <td class="dim">${q.rateLimits ? q.rateLimits.maxDispatchesPerSecond + '/s' : '&mdash;'}</td>
    </tr>`;
  }
  $('tasks-content').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>Queues</h2><span class="cnt">${queues.length}</span></div>
      <table>
        <thead><tr><th>Queue</th><th>State</th><th>Tasks</th><th>Rate Limit</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="4" class="empty">No queues</td></tr>'}</tbody>
      </table>
    </div>`;
}

async function loadQueueTasks(queueName) {
  const short = shortName(queueName);
  $('tasks-nav').style.display = 'flex';
  $('tasks-nav').innerHTML = `<a onclick="loadTasks()">Queues</a> <span>&#8250;</span> ${esc(short)}`;
  $('tasks-content').innerHTML = '<div class="loading">Loading tasks&hellip;</div>';
  try {
    const tasks = await api('/api/tasks/tasks?' + new URLSearchParams({ queue: queueName }));
    renderQueueTasks(queueName, tasks);
  } catch (e) {
    $('tasks-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function renderQueueTasks(queueName, tasks) {
  const short = shortName(queueName);
  let rows = '';
  for (const t of tasks) {
    const taskId = shortName(t.name);
    const url = t.httpRequest ? t.httpRequest.url : '&mdash;';
    const method = t.httpRequest ? t.httpRequest.httpMethod : '';
    const tn = JSON.stringify(t.name);
    const qn = JSON.stringify(queueName);
    rows += `<tr>
      <td class="mono">${esc(taskId)}</td>
      <td class="dim">${method ? `<code>${method}</code> ` : ''}${esc(url)}</td>
      <td class="dim">${t.scheduleTime ? t.scheduleTime.substring(0, 19).replace('T', ' ') : '&mdash;'}</td>
      <td class="dim">${t.dispatchCount || 0}</td>
      <td class="actions">
        <button class="btn btn-danger" onclick="deleteTask(${tn}, ${qn})">Delete</button>
      </td>
    </tr>`;
  }
  $('tasks-content').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>${esc(short)}</h2><span class="cnt">${tasks.length} tasks</span></div>
      <table>
        <thead><tr><th>Task ID</th><th>HTTP Request</th><th>Schedule Time</th><th>Dispatches</th><th>Actions</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="5" class="empty">No tasks</td></tr>'}</tbody>
      </table>
    </div>`;
}

async function deleteTask(taskName, queueName) {
  if (!confirm(`Delete task "${shortName(taskName)}"?`)) return;
  try {
    await api('/api/tasks/task?' + new URLSearchParams({ task: taskName }), { method: 'DELETE' });
    loadQueueTasks(queueName);
  } catch (e) { alert('Delete failed: ' + e.message); }
}

// ── BigQuery ──────────────────────────────────────────────────────────────────
async function loadBigQuery() {
  $('bq-nav').style.display = 'none';
  $('bq-content').innerHTML = '<div class="loading">Loading datasets&hellip;</div>';
  try {
    const datasets = await api('/api/bigquery/datasets');
    renderBQDatasets(datasets);
  } catch (e) {
    $('bq-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function renderBQDatasets(datasets) {
  $('bq-nav').style.display = 'none';
  let rows = '';
  for (const d of datasets) {
    const id = d.datasetId;
    const n = JSON.stringify(id);
    rows += `<tr>
      <td><button class="btn-link" onclick="loadBQTables(${n})">${esc(id)}</button></td>
      <td><span class="cnt">${d.tableCount}</span></td>
      <td class="dim">${d.location || 'US'}</td>
      <td class="actions">
        <button class="btn btn-danger" onclick="deleteBQDataset(${n})">Delete</button>
      </td>
    </tr>`;
  }
  $('bq-content').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>Datasets</h2><span class="cnt">${datasets.length}</span></div>
      <table>
        <thead><tr><th>Dataset</th><th>Tables</th><th>Location</th><th>Actions</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="4" class="empty">No datasets</td></tr>'}</tbody>
      </table>
    </div>`;
}

async function loadBQTables(dataset) {
  $('bq-nav').style.display = 'flex';
  $('bq-nav').innerHTML = `<a onclick="loadBigQuery()">Datasets</a> <span>&#8250;</span> ${esc(dataset)}`;
  $('bq-content').innerHTML = '<div class="loading">Loading tables&hellip;</div>';
  try {
    const tables = await api('/api/bigquery/tables?' + new URLSearchParams({ dataset }));
    renderBQTables(dataset, tables);
  } catch (e) {
    $('bq-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function renderBQTables(dataset, tables) {
  const ds = JSON.stringify(dataset);
  let rows = '';
  for (const t of tables) {
    const id = t.tableId;
    const tn = JSON.stringify(id);
    const fieldCount = (t.schema && t.schema.fields) ? t.schema.fields.length : 0;
    rows += `<tr>
      <td><button class="btn-link" onclick="loadBQPreview(${ds}, ${tn})">${esc(id)}</button></td>
      <td class="dim">${fieldCount} field${fieldCount !== 1 ? 's' : ''}</td>
      <td><span class="cnt">${t.numRows || 0}</span></td>
      <td class="actions">
        <button class="btn btn-ghost" onclick="loadBQPreview(${ds}, ${tn})">Preview</button>
        <button class="btn btn-danger" onclick="deleteBQTable(${ds}, ${tn})">Delete</button>
      </td>
    </tr>`;
  }
  $('bq-content').innerHTML = `
    <div class="card">
      <div class="card-header">
        <h2>${esc(dataset)}</h2><span class="cnt">${tables.length} table${tables.length !== 1 ? 's' : ''}</span>
      </div>
      <table>
        <thead><tr><th>Table</th><th>Schema</th><th>Rows</th><th>Actions</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="4" class="empty">No tables</td></tr>'}</tbody>
      </table>
    </div>`;
}

async function loadBQPreview(dataset, table) {
  const ds = JSON.stringify(dataset);
  $('bq-nav').style.display = 'flex';
  $('bq-nav').innerHTML = `<a onclick="loadBigQuery()">Datasets</a> <span>&#8250;</span> <a onclick="loadBQTables(${ds})">${esc(dataset)}</a> <span>&#8250;</span> ${esc(table)}`;
  $('bq-content').innerHTML = '<div class="loading">Loading preview&hellip;</div>';
  try {
    const data = await api('/api/bigquery/preview?' + new URLSearchParams({ dataset, table }));
    renderBQPreview(dataset, table, data);
  } catch (e) {
    $('bq-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function renderBQPreview(dataset, table, data) {
  const fields = (data.schema && data.schema.fields) || [];
  const rows = data.rows || [];
  const headCols = fields.map(f =>
    `<th>${esc(f.name)}<br><span class="dim" style="font-weight:400;text-transform:none">${esc(f.type)}</span></th>`
  ).join('');
  let bodyRows = '';
  for (const row of rows) {
    const cells = (row.f || []).map(cell =>
      `<td class="mono">${cell.v === null ? '<em style="color:#9aa0a6">null</em>' : esc(String(cell.v))}</td>`
    ).join('');
    bodyRows += `<tr>${cells}</tr>`;
  }
  $('bq-content').innerHTML = `
    <div class="card">
      <div class="card-header">
        <h2>${esc(table)}</h2>
        <span class="cnt">${data.totalRows || 0} rows total</span>
        <span class="dim" style="margin-left:.5rem">showing first ${rows.length}</span>
      </div>
      <div style="overflow-x:auto">
        <table>
          <thead><tr>${headCols || '<th>(no schema)</th>'}</tr></thead>
          <tbody>${bodyRows || `<tr><td colspan="${fields.length || 1}" class="empty">No rows</td></tr>`}</tbody>
        </table>
      </div>
    </div>`;
}

async function deleteBQDataset(dataset) {
  if (!confirm(`Delete dataset "${dataset}" and all its tables?`)) return;
  try {
    await api('/api/bigquery/dataset?' + new URLSearchParams({ dataset }), { method: 'DELETE' });
    loaded.bigquery = false;
    loadBigQuery();
  } catch (e) { alert('Delete failed: ' + e.message); }
}

async function deleteBQTable(dataset, table) {
  if (!confirm(`Delete table "${dataset}.${table}"?`)) return;
  try {
    await api('/api/bigquery/table?' + new URLSearchParams({ dataset, table }), { method: 'DELETE' });
    loadBQTables(dataset);
  } catch (e) { alert('Delete failed: ' + e.message); }
}

// ── Boot ──────────────────────────────────────────────────────────────────────
loadOverview();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(content=_HTML)


@app.get("/api/stats")
async def api_stats():
    return JSONResponse(content={
        "project": settings.default_project,
        "services": _get_services(),
    })


# ── GCS ──────────────────────────────────────────────────────────────────────

@app.get("/api/gcs/buckets")
async def api_gcs_buckets():
    from localgcp.services.gcs.store import get_store
    store = get_store()
    buckets = store.list("buckets")
    result = []
    for b in sorted(buckets, key=lambda x: x["name"]):
        count = sum(1 for k in store.keys("objects") if k.startswith(f"{b['name']}/"))
        result.append({**b, "objectCount": count})
    return result


@app.get("/api/gcs/objects")
async def api_gcs_objects(bucket: str = Query(...)):
    from localgcp.services.gcs.store import get_store
    store = get_store()
    prefix = f"{bucket}/"
    items = []
    for k in sorted(k for k in store.keys("objects") if k.startswith(prefix)):
        data = store.get("objects", k)
        if data:
            items.append(data)
    return items


@app.delete("/api/gcs/buckets")
async def api_gcs_delete_bucket(bucket: str = Query(...)):
    from localgcp.services.gcs.store import get_store
    store = get_store()
    for k in list(store.keys("objects")):
        if k.startswith(f"{bucket}/"):
            store.delete("objects", k)
            store.delete("bodies", k)
    store.delete("buckets", bucket)
    return {"deleted": bucket}


@app.delete("/api/gcs/objects")
async def api_gcs_delete_object(bucket: str = Query(...), name: str = Query(...)):
    from localgcp.services.gcs.store import get_store
    store = get_store()
    key = f"{bucket}/{name}"
    store.delete("objects", key)
    store.delete("bodies", key)
    return {"deleted": key}


# ── Pub/Sub ───────────────────────────────────────────────────────────────────

@app.get("/api/pubsub/topics")
async def api_pubsub_topics():
    from localgcp.services.pubsub.store import get_store
    store = get_store()
    topics = store.list("topics")
    subs = store.list("subscriptions")
    result = []
    for t in sorted(topics, key=lambda x: x["name"]):
        count = sum(1 for s in subs if s["topic"] == t["name"])
        result.append({**t, "subscriptionCount": count})
    return result


@app.get("/api/pubsub/subscriptions")
async def api_pubsub_subscriptions():
    from localgcp.services.pubsub.store import get_store, queue_depth
    store = get_store()
    subs = store.list("subscriptions")
    result = []
    for s in sorted(subs, key=lambda x: x["name"]):
        result.append({**s, "queueDepth": queue_depth(s["name"])})
    return result


@app.post("/api/pubsub/publish")
async def api_pubsub_publish(request: Request):
    import uuid
    from datetime import datetime, timezone
    from localgcp.services.pubsub.store import get_store, ensure_queue, enqueue

    body = await request.json()
    topic = body.get("topic", "")
    data = body.get("data", "")
    attributes = body.get("attributes", {})

    store = get_store()
    if not store.exists("topics", topic):
        return JSONResponse(status_code=404, content={"error": "Topic not found"})

    encoded = base64.b64encode(data.encode()).decode() if data else ""
    msg_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    message = {
        "data": encoded,
        "attributes": attributes,
        "messageId": msg_id,
        "publishTime": now,
        "orderingKey": "",
    }

    count = 0
    for s in store.list("subscriptions"):
        if s["topic"] == topic:
            ensure_queue(s["name"])
            enqueue(s["name"], message)
            count += 1

    return {"messageId": msg_id, "deliveredToSubscriptions": count}


@app.delete("/api/pubsub/topics")
async def api_pubsub_delete_topic(topic: str = Query(...)):
    from localgcp.services.pubsub.store import get_store, remove_queue
    store = get_store()
    store.delete("topics", topic)
    for s in store.list("subscriptions"):
        if s["topic"] == topic:
            store.delete("subscriptions", s["name"])
            remove_queue(s["name"])
    return {"deleted": topic}


@app.delete("/api/pubsub/subscriptions")
async def api_pubsub_delete_subscription(subscription: str = Query(...)):
    from localgcp.services.pubsub.store import get_store, remove_queue
    store = get_store()
    store.delete("subscriptions", subscription)
    remove_queue(subscription)
    return {"deleted": subscription}


# ── Firestore ─────────────────────────────────────────────────────────────────

@app.get("/api/firestore/collections")
async def api_firestore_collections():
    from localgcp.services.firestore.store import get_store
    store = get_store()
    counts: dict[str, int] = {}
    for key in store.keys("documents"):
        parts = key.split("/documents/", 1)
        if len(parts) == 2:
            segments = parts[1].split("/")
            if len(segments) >= 2:
                counts[segments[0]] = counts.get(segments[0], 0) + 1
    return [{"name": c, "documentCount": n} for c, n in sorted(counts.items())]


@app.get("/api/firestore/documents")
async def api_firestore_documents(collection: str = Query(...)):
    from localgcp.services.firestore.store import get_store
    store = get_store()
    docs = []
    for key in store.keys("documents"):
        parts = key.split("/documents/", 1)
        if len(parts) == 2:
            segments = parts[1].split("/")
            if len(segments) == 2 and segments[0] == collection:
                data = store.get("documents", key)
                if data:
                    docs.append(data)
    return sorted(docs, key=lambda x: x.get("name", ""))


@app.delete("/api/firestore/documents")
async def api_firestore_delete_document(path: str = Query(...)):
    from localgcp.services.firestore.store import get_store
    get_store().delete("documents", path)
    return {"deleted": path}


# ── Secret Manager ────────────────────────────────────────────────────────────

@app.get("/api/secretmanager/secrets")
async def api_sm_secrets():
    from localgcp.services.secretmanager.store import get_store
    store = get_store()
    secrets = store.list("secrets")
    result = []
    for s in sorted(secrets, key=lambda x: x["name"]):
        count = sum(1 for k in store.keys("versions") if k.startswith(f"{s['name']}/versions/"))
        result.append({**s, "versionCount": count})
    return result


@app.get("/api/secretmanager/versions")
async def api_sm_versions(secret: str = Query(...)):
    """List versions for a secret identified by its short name (e.g. 'my-secret')."""
    from localgcp.services.secretmanager.store import get_store
    store = get_store()
    full_name = next(
        (k for k in store.keys("secrets") if k.endswith(f"/secrets/{secret}")), None
    )
    if full_name is None:
        return JSONResponse(status_code=404, content={"error": "Secret not found"})
    prefix = f"{full_name}/versions/"
    versions = [
        store.get("versions", k)
        for k in sorted(k for k in store.keys("versions") if k.startswith(prefix))
    ]
    return [v for v in versions if v]


@app.get("/api/secretmanager/value")
async def api_sm_value(secret: str = Query(...), version: str = Query(...)):
    """Return the decoded payload for a secret version."""
    from localgcp.services.secretmanager.store import get_store
    store = get_store()
    full_name = next(
        (k for k in store.keys("secrets") if k.endswith(f"/secrets/{secret}")), None
    )
    if full_name is None:
        return JSONResponse(status_code=404, content={"error": "Secret not found"})
    version_key = f"{full_name}/versions/{version}"
    payload = store.get("payloads", version_key)
    if payload is None:
        return JSONResponse(status_code=404, content={"error": "Version not found"})
    try:
        decoded = base64.b64decode(payload).decode("utf-8")
    except Exception:
        decoded = payload
    return {"value": decoded}


@app.delete("/api/secretmanager/secrets")
async def api_sm_delete_secret(secret: str = Query(...)):
    from localgcp.services.secretmanager.store import get_store
    store = get_store()
    full_name = next(
        (k for k in store.keys("secrets") if k.endswith(f"/secrets/{secret}")), None
    )
    if full_name is None:
        return JSONResponse(status_code=404, content={"error": "Secret not found"})
    store.delete("secrets", full_name)
    for k in list(store.keys("versions")):
        if k.startswith(f"{full_name}/versions/"):
            store.delete("versions", k)
            store.delete("payloads", k)
    return {"deleted": secret}


# ── Cloud Tasks ───────────────────────────────────────────────────────────────

@app.get("/api/tasks/queues")
async def api_tasks_queues():
    from localgcp.services.tasks.store import get_store
    store = get_store()
    queues = store.list("queues")
    result = []
    for q in sorted(queues, key=lambda x: x["name"]):
        count = sum(1 for k in store.keys("tasks") if k.startswith(f"{q['name']}/tasks/"))
        result.append({**q, "taskCount": count})
    return result


@app.get("/api/tasks/tasks")
async def api_tasks_list(queue: str = Query(...)):
    from localgcp.services.tasks.store import get_store
    store = get_store()
    prefix = f"{queue}/tasks/"
    tasks = [
        store.get("tasks", k)
        for k in sorted(k for k in store.keys("tasks") if k.startswith(prefix))
    ]
    return [t for t in tasks if t]


@app.delete("/api/tasks/task")
async def api_tasks_delete_task(task: str = Query(...)):
    from localgcp.services.tasks.store import get_store
    get_store().delete("tasks", task)
    return {"deleted": task}


# ── BigQuery ──────────────────────────────────────────────────────────────────

@app.get("/api/bigquery/datasets")
async def api_bq_datasets():
    from localgcp.services.bigquery.engine import get_engine
    engine = get_engine()
    project = settings.default_project
    datasets = engine.list_datasets(project)
    result = []
    for d in sorted(datasets, key=lambda x: x["datasetReference"]["datasetId"]):
        ds_id = d["datasetReference"]["datasetId"]
        tables = engine.list_tables(project, ds_id)
        result.append({
            "datasetId": ds_id,
            "location": d.get("location", "US"),
            "tableCount": len(tables),
        })
    return result


@app.get("/api/bigquery/tables")
async def api_bq_tables(dataset: str = Query(...)):
    from localgcp.services.bigquery.engine import get_engine
    engine = get_engine()
    tables = engine.list_tables(settings.default_project, dataset)
    return [
        {
            "tableId": t["tableReference"]["tableId"],
            "schema": t.get("schema"),
            "numRows": t.get("numRows", "0"),
        }
        for t in sorted(tables, key=lambda x: x["tableReference"]["tableId"])
    ]


@app.get("/api/bigquery/preview")
async def api_bq_preview(
    dataset: str = Query(...),
    table: str = Query(...),
    maxResults: int = Query(default=50),
):
    from localgcp.services.bigquery.engine import get_engine
    engine = get_engine()
    try:
        return engine.list_rows(settings.default_project, dataset, table, max_results=maxResults)
    except ValueError as e:
        return JSONResponse(status_code=404, content={"error": str(e)})


@app.delete("/api/bigquery/dataset")
async def api_bq_delete_dataset(dataset: str = Query(...)):
    from localgcp.services.bigquery.engine import get_engine
    engine = get_engine()
    try:
        engine.delete_dataset(settings.default_project, dataset, delete_contents=True)
    except ValueError as e:
        return JSONResponse(status_code=404, content={"error": str(e)})
    return {"deleted": dataset}


@app.delete("/api/bigquery/table")
async def api_bq_delete_table(dataset: str = Query(...), table: str = Query(...)):
    from localgcp.services.bigquery.engine import get_engine
    engine = get_engine()
    found = engine.delete_table(settings.default_project, dataset, table)
    if not found:
        return JSONResponse(status_code=404, content={"error": "Table not found"})
    return {"deleted": f"{dataset}.{table}"}


# ---------------------------------------------------------------------------
# Reset endpoints
# ---------------------------------------------------------------------------


@app.post("/reset/{service}")
async def reset_service(service: str):
    _reset_one(service)
    return {"reset": service}


@app.post("/reset")
async def reset_all():
    for svc in ("gcs", "pubsub", "firestore", "secretmanager", "tasks", "bigquery"):
        _reset_one(svc)
    return {"reset": "all"}


def _reset_one(service: str) -> None:
    if service == "gcs":
        from localgcp.services.gcs.store import get_store
        get_store().reset()
    elif service == "pubsub":
        from localgcp.services.pubsub.store import get_store, _queues, _unacked
        get_store().reset()
        _queues.clear()
        _unacked.clear()
    elif service == "firestore":
        from localgcp.services.firestore.store import get_store
        get_store().reset()
    elif service == "secretmanager":
        from localgcp.services.secretmanager.store import get_store
        get_store().reset()
    elif service == "tasks":
        from localgcp.services.tasks.store import get_store
        get_store().reset()
    elif service == "bigquery":
        from localgcp.services.bigquery.engine import get_engine
        get_engine().reset()


@app.get("/health")
async def health():
    return {"status": "ok", "project": settings.default_project}
