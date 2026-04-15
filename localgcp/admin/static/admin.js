// ── State ────────────────────────────────────────────────────────────────────
const loaded = {};
let _publishTopic = '';

// ── Utilities ────────────────────────────────────────────────────────────────
const $  = id => document.getElementById(id);
const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');

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

// ── Sort / Pagination helpers ────────────────────────────────────────────────
// Render a sortable <th>. clickFn is a global function name; field is passed as arg.
function _sth(label, field, state, clickFn, extra = '') {
  const active = state.f === field;
  const cls = active ? ` sort-${state.d === 1 ? 'asc' : 'desc'}` : '';
  const icon = `<span class="sort-icon">${active ? (state.d === 1 ? '▲' : '▼') : '⇅'}</span>`;
  return `<th class="sortable${cls}"${extra} onclick="${clickFn}('${field}')">${label} ${icon}</th>`;
}
// Sort items by state.f/d; numFields lists fields that should sort numerically.
function _srt(items, state, numFields = []) {
  return [...items].sort((a, b) => {
    let av = a[state.f], bv = b[state.f];
    if (numFields.includes(state.f)) return ((parseFloat(av) || 0) - (parseFloat(bv) || 0)) * state.d;
    av = String(av ?? '').toLowerCase(); bv = String(bv ?? '').toLowerCase();
    return (av < bv ? -1 : av > bv ? 1 : 0) * state.d;
  });
}
// Paginate items; mutates pgSt.page to clamp it; returns { slice, total, start, ps, maxPg }.
function _pg(items, pgSt) {
  const total = items.length;
  const ps = pgSt.size === 'all' ? Math.max(1, total) : pgSt.size;
  const maxPg = Math.max(0, Math.ceil(total / ps) - 1);
  pgSt.page = Math.min(pgSt.page, maxPg);
  const start = pgSt.page * ps;
  return { slice: items.slice(start, start + ps), total, start, ps, maxPg };
}
// Advance page by delta, clamped to [0, maxPg].
function _pgChg(pgSt, total, delta) {
  const ps = pgSt.size === 'all' ? Math.max(1, total) : pgSt.size;
  pgSt.page = Math.max(0, Math.min(pgSt.page + delta, Math.max(0, Math.ceil(total / ps) - 1)));
}
// Render pagination bar; only shown when total > 25.
function _pgBar(total, pgSt, changeFn, sizeFn) {
  if (total <= 25) return '';
  const ps = pgSt.size === 'all' ? Math.max(1, total) : pgSt.size;
  const maxPg = Math.max(0, Math.ceil(total / ps) - 1);
  const start = pgSt.page * ps;
  const from = total === 0 ? 0 : start + 1;
  const to = Math.min(start + ps, total);
  const opts = [25, 50, 100, 'all'].map(v => {
    const val = v === 'all' ? 'all' : v;
    const cur = pgSt.size === val;
    return `<option value="${val}"${cur ? ' selected' : ''}>${v === 'all' ? 'All' : v} / page</option>`;
  }).join('');
  return `<div class="pagination">
    <span class="page-info">${from}–${to} of ${total}</span>
    <button class="btn btn-ghost" onclick="${changeFn}(-1)"${pgSt.page <= 0 ? ' disabled' : ''}>‹ Prev</button>
    <button class="btn btn-ghost" onclick="${changeFn}(1)"${pgSt.page >= maxPg ? ' disabled' : ''}>Next ›</button>
    <select onchange="${sizeFn}(this.value)">${opts}</select>
  </div>`;
}

// Per-service list state
const _gcs = {
  buckets: [],
  bSort: { f: 'name', d: 1 },
  bucket: null,
  objects: [],
  oSort: { f: 'name', d: 1 },
  page: 0,
  pageSize: 50,
};
const _ps = { topics: [], tSort: {f:'name',d:1}, tPg: {page:0,size:50},
              subs:   [], sSort: {f:'name',d:1}, sPg: {page:0,size:50} };
const _fs = { cols: [], cSort: {f:'name',d:1},
              col: null, docs: [], dSort: {f:'name',d:1}, dPg: {page:0,size:50} };
const _sm = { secrets: [], sort: {f:'name',d:1}, pg: {page:0,size:50} };
const _tk = { queues: [], qSort: {f:'name',d:1},
              queue: null, tasks: [], tSort: {f:'name',d:1}, tPg: {page:0,size:50} };
const _bq = { datasets: [], dsSort: {f:'datasetId',d:1},
              dataset: null, tables: [], tbSort: {f:'tableId',d:1} };
const _sc = { jobs: [], sort: {f:'id',d:1}, pg: {page:0,size:50} };

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
  overview:  loadOverview,
  gcs:       loadGCS,
  pubsub:    loadPubSub,
  firestore: loadFirestore,
  secrets:   loadSecrets,
  tasks:     loadTasks,
  bigquery:  loadBigQuery,
  scheduler: loadScheduler,
};

function showTab(tab, pushState = true) {
  const btn = document.querySelector(`.tab-btn[data-tab="${tab}"]`);
  if (!btn) return;
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
  btn.classList.add('active');
  $('panel-' + tab).classList.add('active');
  if (pushState) history.pushState({ tab }, '', '#' + tab);
  if (!loaded[tab] || tab === 'overview') {
    loaded[tab] = true;
    loaders[tab]();
  }
}

document.querySelectorAll('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => showTab(btn.dataset.tab));
});

function _navigateHash(hash, pushState) {
  if (hash.startsWith('gcs/')) {
    const bucket = decodeURIComponent(hash.slice(4));
    showTab('gcs', false);
    loadGCSObjects(bucket, pushState);
  } else {
    showTab(hash || 'overview', false);
  }
}

window.addEventListener('popstate', e => {
  if (e.state && e.state.bucket) {
    showTab('gcs', false);
    loadGCSObjects(e.state.bucket, false);
  } else {
    const tab = (e.state && e.state.tab) || location.hash.slice(1) || 'overview';
    showTab(tab, false);
  }
});

// Honour hash on initial load
{ const initial = location.hash.slice(1); if (initial) _navigateHash(initial, false); }

// ── Overview ─────────────────────────────────────────────────────────────────
async function loadOverview() {
  try {
    const d = await api('/api/stats');
    $('project-id').textContent = d.project || 'local-project';
    const labels = {
      gcs: 'Cloud Storage', pubsub: 'Cloud Pub/Sub',
      firestore: 'Cloud Firestore', secretmanager: 'Secret Manager', tasks: 'Cloud Tasks',
      bigquery: 'BigQuery', scheduler: 'Cloud Scheduler',
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

function _gcsCmp(a, b, field, dir) {
  let av = a[field], bv = b[field];
  if (field === 'size' || field === 'objectCount') {
    av = parseInt(av, 10) || 0; bv = parseInt(bv, 10) || 0;
    return (av - bv) * dir;
  }
  av = String(av || '').toLowerCase(); bv = String(bv || '').toLowerCase();
  return (av < bv ? -1 : av > bv ? 1 : 0) * dir;
}

function _gcsSortIcon(state, field) {
  const active = state.f === field;
  const cls = active ? (state.d === 1 ? 'sort-asc' : 'sort-desc') : '';
  const sym = active ? (state.d === 1 ? '▲' : '▼') : '⇅';
  return { cls, icon: `<span class="sort-icon">${sym}</span>` };
}

// ── Buckets ──

async function loadGCS(pushState = true) {
  _gcs.bucket = null;
  if (pushState) history.pushState({ tab: 'gcs' }, '', '#gcs');
  $('gcs-nav').style.display = 'none';
  $('gcs-content').innerHTML = '<div class="loading">Loading buckets&hellip;</div>';
  try {
    _gcs.buckets = await api('/api/gcs/buckets');
    renderGCSBuckets();
  } catch (e) {
    $('gcs-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function sortGCSBuckets(field) {
  _gcs.bSort = _gcs.bSort.f === field
    ? { f: field, d: _gcs.bSort.d * -1 }
    : { f: field, d: 1 };
  renderGCSBuckets();
}

function renderGCSBuckets() {
  $('gcs-nav').style.display = 'none';
  const s = _gcs.bSort;
  const sorted = [..._gcs.buckets].sort((a, b) => _gcsCmp(a, b, s.f, s.d));

  const th = (label, field) => {
    const { cls, icon } = _gcsSortIcon(s, field);
    return `<th class="sortable${cls ? ' ' + cls : ''}" onclick="sortGCSBuckets('${field}')">${label} ${icon}</th>`;
  };

  let rows = '';
  for (const b of sorted) {
    const n = JSON.stringify(b.name);
    rows += `<tr>
      <td><button class="btn-link" onclick="loadGCSObjects(${n})">${esc(b.name)}</button></td>
      <td><span class="cnt">${b.objectCount}</span></td>
      <td class="dim">${b.timeCreated ? b.timeCreated.substring(0, 10) : '&mdash;'}</td>
      <td class="actions"><button class="btn btn-danger" onclick="deleteGCSBucket(${n})">Delete</button></td>
    </tr>`;
  }
  $('gcs-content').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>Buckets</h2><span class="cnt">${sorted.length}</span></div>
      <table>
        <thead><tr>
          ${th('Bucket', 'name')}
          ${th('Objects', 'objectCount')}
          ${th('Created', 'timeCreated')}
          <th>Actions</th>
        </tr></thead>
        <tbody>${rows || '<tr><td colspan="4" class="empty">No buckets</td></tr>'}</tbody>
      </table>
    </div>`;
}

// ── Objects ──

async function loadGCSObjects(bucket, pushState = true) {
  _gcs.bucket = bucket;
  _gcs.page = 0;
  _gcs.oSort = { f: 'name', d: 1 };
  if (pushState) history.pushState({ tab: 'gcs', bucket }, '', '#gcs/' + encodeURIComponent(bucket));
  $('gcs-nav').style.display = 'flex';
  $('gcs-nav').innerHTML = `<a onclick="loadGCS()">Buckets</a> <span>&#8250;</span> ${esc(bucket)}`;
  $('gcs-content').innerHTML = '<div class="loading">Loading objects&hellip;</div>';
  try {
    _gcs.objects = await api('/api/gcs/objects?' + new URLSearchParams({ bucket }));
    renderGCSObjects();
  } catch (e) {
    $('gcs-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function sortGCSObjects(field) {
  _gcs.oSort = _gcs.oSort.f === field
    ? { f: field, d: _gcs.oSort.d * -1 }
    : { f: field, d: 1 };
  _gcs.page = 0;
  renderGCSObjects();
}

function gcsChangePage(delta) {
  const ps = _gcs.pageSize === 'all' ? _gcs.objects.length || 1 : _gcs.pageSize;
  const maxPage = Math.max(0, Math.ceil(_gcs.objects.length / ps) - 1);
  _gcs.page = Math.max(0, Math.min(_gcs.page + delta, maxPage));
  renderGCSObjects();
}

function gcsSetPageSize(val) {
  _gcs.pageSize = val === 'all' ? 'all' : parseInt(val, 10);
  _gcs.page = 0;
  renderGCSObjects();
}

function renderGCSObjects() {
  const bucket = _gcs.bucket;
  const s = _gcs.oSort;
  const sorted = [..._gcs.objects].sort((a, b) => _gcsCmp(a, b, s.f, s.d));
  const total = sorted.length;

  const ps = _gcs.pageSize === 'all' ? (total || 1) : _gcs.pageSize;
  const maxPage = Math.max(0, Math.ceil(total / ps) - 1);
  _gcs.page = Math.min(_gcs.page, maxPage);
  const start = _gcs.page * ps;
  const page  = sorted.slice(start, start + ps);

  const th = (label, field, extra = '') => {
    const { cls, icon } = _gcsSortIcon(s, field);
    return `<th class="sortable${cls ? ' ' + cls : ''}" onclick="sortGCSObjects('${field}')"${extra}>${label} ${icon}</th>`;
  };

  const bn = JSON.stringify(bucket);
  let rows = '';
  for (const o of page) {
    const on = JSON.stringify(o.name);
    rows += `<tr>
      <td class="mono">${esc(o.name)}</td>
      <td class="dim" style="white-space:nowrap">${humanSize(o.size)}</td>
      <td class="dim">${esc(o.contentType || '')}</td>
      <td class="dim" style="white-space:nowrap">${o.updated ? o.updated.substring(0, 19).replace('T', ' ') : '&mdash;'}</td>
      <td class="actions"><button class="btn btn-danger" onclick="deleteGCSObject(${bn}, ${on})">Delete</button></td>
    </tr>`;
  }

  const pageSizeOpts = ['25', '50', '100', 'all'].map(v => {
    const cur = _gcs.pageSize === (v === 'all' ? 'all' : parseInt(v, 10));
    return `<option value="${v}"${cur ? ' selected' : ''}>${v === 'all' ? 'All' : v} / page</option>`;
  }).join('');

  const from = total === 0 ? 0 : start + 1;
  const to   = Math.min(start + ps, total);
  const pageInfo = total === 0 ? 'No objects' : `${from}–${to} of ${total} object${total !== 1 ? 's' : ''}`;
  const showPager = total > 25;

  const pagination = showPager ? `
    <div class="pagination">
      <span class="page-info">${pageInfo}</span>
      <button class="btn btn-ghost" onclick="gcsChangePage(-1)"${_gcs.page === 0 ? ' disabled' : ''}>‹ Prev</button>
      <button class="btn btn-ghost" onclick="gcsChangePage(1)"${_gcs.page >= maxPage ? ' disabled' : ''}>Next ›</button>
      <select onchange="gcsSetPageSize(this.value)">${pageSizeOpts}</select>
    </div>` : '';

  $('gcs-content').innerHTML = `
    <div class="card">
      <div class="card-header">
        <h2>${esc(bucket)}</h2>
        <span class="cnt">${total} object${total !== 1 ? 's' : ''}</span>
        ${!showPager && total > 0 ? `<span class="dim" style="margin-left:.5rem;font-size:.8rem">${pageInfo}</span>` : ''}
      </div>
      <div style="overflow-x:auto">
        <table>
          <thead><tr>
            ${th('Name', 'name')}
            ${th('Size', 'size')}
            ${th('Content-Type', 'contentType', ' style="min-width:130px"')}
            ${th('Updated', 'updated')}
            <th>Actions</th>
          </tr></thead>
          <tbody>${rows || '<tr><td colspan="5" class="empty">No objects</td></tr>'}</tbody>
        </table>
      </div>
      ${pagination}
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
    // Re-fetch objects, preserving current sort and page
    _gcs.objects = await api('/api/gcs/objects?' + new URLSearchParams({ bucket }));
    renderGCSObjects();
  } catch (e) { alert('Delete failed: ' + e.message); }
}

// ── Pub/Sub ───────────────────────────────────────────────────────────────────
async function loadPubSub() {
  $('pubsub-content').innerHTML = '<div class="loading">Loading&hellip;</div>';
  try {
    [_ps.topics, _ps.subs] = await Promise.all([
      api('/api/pubsub/topics'),
      api('/api/pubsub/subscriptions'),
    ]);
    $('pubsub-content').innerHTML = '<div id="ps-topics"></div><div id="ps-subs"></div>';
    renderPsTopics(); renderPsSubs();
  } catch (e) {
    $('pubsub-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

async function _reloadPs() {
  [_ps.topics, _ps.subs] = await Promise.all([
    api('/api/pubsub/topics'), api('/api/pubsub/subscriptions'),
  ]);
  renderPsTopics(); renderPsSubs();
}

function sortPsTopic(f) { _ps.tSort = _ps.tSort.f===f ? {f,d:_ps.tSort.d*-1} : {f,d:1}; _ps.tPg.page=0; renderPsTopics(); }
function pgPsTopic(d)   { _pgChg(_ps.tPg, _ps.topics.length, d); renderPsTopics(); }
function szPsTopic(v)   { _ps.tPg.size=v==='all'?'all':+v; _ps.tPg.page=0; renderPsTopics(); }

function renderPsTopics() {
  const sorted = _srt(_ps.topics, _ps.tSort, ['subscriptionCount']);
  const {slice, total} = _pg(sorted, _ps.tPg);
  const sth = (l,f) => _sth(l, f, _ps.tSort, 'sortPsTopic');
  let rows = '';
  for (const t of slice) {
    const tn = JSON.stringify(t.name);
    rows += `<tr>
      <td class="mono">${esc(shortName(t.name))}</td>
      <td><span class="cnt">${t.subscriptionCount}</span></td>
      <td class="dim">${esc(t.name)}</td>
      <td class="actions">
        <button class="btn btn-secondary" onclick="openPublishModal(${tn})">Publish</button>
        <button class="btn btn-danger"    onclick="deletePubSubTopic(${tn})">Delete</button>
      </td>
    </tr>`;
  }
  $('ps-topics').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>Topics</h2><span class="cnt">${total}</span></div>
      <table>
        <thead><tr>${sth('Name','name')}${sth('Subscriptions','subscriptionCount')}<th>Full Name</th><th>Actions</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="4" class="empty">No topics</td></tr>'}</tbody>
      </table>
      ${_pgBar(total, _ps.tPg, 'pgPsTopic', 'szPsTopic')}
    </div>`;
}

function sortPsSub(f) { _ps.sSort = _ps.sSort.f===f ? {f,d:_ps.sSort.d*-1} : {f,d:1}; _ps.sPg.page=0; renderPsSubs(); }
function pgPsSub(d)   { _pgChg(_ps.sPg, _ps.subs.length, d); renderPsSubs(); }
function szPsSub(v)   { _ps.sPg.size=v==='all'?'all':+v; _ps.sPg.page=0; renderPsSubs(); }

function renderPsSubs() {
  const sorted = _srt(_ps.subs, _ps.sSort, ['queueDepth','ackDeadlineSeconds']);
  const {slice, total} = _pg(sorted, _ps.sPg);
  const sth = (l,f) => _sth(l, f, _ps.sSort, 'sortPsSub');
  let rows = '';
  for (const s of slice) {
    const sn = JSON.stringify(s.name);
    rows += `<tr>
      <td class="mono">${esc(shortName(s.name))}</td>
      <td class="dim mono">${esc(shortName(s.topic))}</td>
      <td><span class="cnt">${s.queueDepth}</span></td>
      <td class="dim">${s.ackDeadlineSeconds}s</td>
      <td class="actions"><button class="btn btn-danger" onclick="deletePubSubSub(${sn})">Delete</button></td>
    </tr>`;
  }
  $('ps-subs').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>Subscriptions</h2><span class="cnt">${total}</span></div>
      <table>
        <thead><tr>
          ${sth('Name','name')}${sth('Topic','topic')}
          ${sth('Queue Depth','queueDepth')}${sth('Ack Deadline','ackDeadlineSeconds')}
          <th>Actions</th>
        </tr></thead>
        <tbody>${rows || '<tr><td colspan="5" class="empty">No subscriptions</td></tr>'}</tbody>
      </table>
      ${_pgBar(total, _ps.sPg, 'pgPsSub', 'szPsSub')}
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
    await _reloadPs();
  } catch (e) { alert('Publish failed: ' + e.message); }
}

async function deletePubSubTopic(name) {
  if (!confirm(`Delete topic "${shortName(name)}" and its subscriptions?`)) return;
  try {
    await api('/api/pubsub/topics?' + new URLSearchParams({ topic: name }), { method: 'DELETE' });
    await _reloadPs();
  } catch (e) { alert('Delete failed: ' + e.message); }
}

async function deletePubSubSub(name) {
  if (!confirm(`Delete subscription "${shortName(name)}"?`)) return;
  try {
    await api('/api/pubsub/subscriptions?' + new URLSearchParams({ subscription: name }), { method: 'DELETE' });
    await _reloadPs();
  } catch (e) { alert('Delete failed: ' + e.message); }
}

// ── Firestore ─────────────────────────────────────────────────────────────────
async function loadFirestore() {
  _fs.col = null;
  $('firestore-nav').style.display = 'none';
  $('firestore-content').innerHTML = '<div class="loading">Loading collections&hellip;</div>';
  try {
    _fs.cols = await api('/api/firestore/collections');
    renderFirestoreCollections();
  } catch (e) {
    $('firestore-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function sortFsCol(f) { _fs.cSort = _fs.cSort.f===f ? {f,d:_fs.cSort.d*-1} : {f,d:1}; renderFirestoreCollections(); }

function renderFirestoreCollections() {
  $('firestore-nav').style.display = 'none';
  const sorted = _srt(_fs.cols, _fs.cSort, ['documentCount']);
  const sth = (l,f) => _sth(l, f, _fs.cSort, 'sortFsCol');
  let rows = '';
  for (const c of sorted) {
    const cn = JSON.stringify(c.name);
    rows += `<tr>
      <td><button class="btn-link" onclick="loadFirestoreDocs(${cn})">${esc(c.name)}</button></td>
      <td><span class="cnt">${c.documentCount}</span></td>
    </tr>`;
  }
  $('firestore-content').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>Collections</h2><span class="cnt">${sorted.length}</span></div>
      <table>
        <thead><tr>${sth('Collection','name')}${sth('Documents','documentCount')}</tr></thead>
        <tbody>${rows || '<tr><td colspan="2" class="empty">No documents stored</td></tr>'}</tbody>
      </table>
    </div>`;
}

async function loadFirestoreDocs(collection) {
  _fs.col = collection;
  _fs.dPg.page = 0;
  _fs.dSort = {f:'name', d:1};
  $('firestore-nav').style.display = 'flex';
  $('firestore-nav').innerHTML = `<a onclick="loadFirestore()">Collections</a> <span>&#8250;</span> ${esc(collection)}`;
  $('firestore-content').innerHTML = '<div class="loading">Loading documents&hellip;</div>';
  try {
    _fs.docs = (await api('/api/firestore/documents?' + new URLSearchParams({ collection })))
      .map(d => ({...d, _id: shortName(d.name||''), _fields: Object.keys(d.fields||{}).length}));
    renderFirestoreDocs();
  } catch (e) {
    $('firestore-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function sortFsDoc(f) { _fs.dSort = _fs.dSort.f===f ? {f,d:_fs.dSort.d*-1} : {f,d:1}; _fs.dPg.page=0; renderFirestoreDocs(); }
function pgFsDoc(d)   { _pgChg(_fs.dPg, _fs.docs.length, d); renderFirestoreDocs(); }
function szFsDoc(v)   { _fs.dPg.size=v==='all'?'all':+v; _fs.dPg.page=0; renderFirestoreDocs(); }

function renderFirestoreDocs() {
  const collection = _fs.col;
  const sorted = _srt(_fs.docs, _fs.dSort, ['_fields']);
  const {slice, total} = _pg(sorted, _fs.dPg);
  const sth = (l,f) => _sth(l, f, _fs.dSort, 'sortFsDoc');
  const cn = JSON.stringify(collection);
  let rows = '';
  for (let i = 0; i < slice.length; i++) {
    const doc = slice[i];
    const dn = JSON.stringify(doc.name || '');
    const fieldsJson = JSON.stringify(doc.fields || {}, null, 2);
    rows += `<tr id="dr-${i}">
      <td class="mono">${esc(doc._id)}</td>
      <td><span class="cnt">${doc._fields}</span></td>
      <td class="dim">${doc.updateTime ? doc.updateTime.substring(0, 19).replace('T', ' ') : '&mdash;'}</td>
      <td class="actions">
        <button class="btn btn-ghost" id="view-btn-${i}" onclick="toggleDocView(${i}, ${JSON.stringify(fieldsJson)})">View</button>
        <button class="btn btn-danger" onclick="deleteFirestoreDoc(${dn}, ${cn})">Delete</button>
      </td>
    </tr>
    <tr id="dv-${i}" style="display:none">
      <td colspan="4" style="padding:.5rem 1rem .75rem"><div class="pre" id="df-${i}"></div></td>
    </tr>`;
  }
  $('firestore-content').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>${esc(collection)}</h2><span class="cnt">${total} document${total!==1?'s':''}</span></div>
      <table>
        <thead><tr>${sth('Document ID','_id')}${sth('Fields','_fields')}${sth('Updated','updateTime')}<th>Actions</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="4" class="empty">No documents</td></tr>'}</tbody>
      </table>
      ${_pgBar(total, _fs.dPg, 'pgFsDoc', 'szFsDoc')}
    </div>`;
}

function toggleDocView(i, fieldsJson) {
  const viewRow = $('dv-' + i), btn = $('view-btn-' + i);
  if (viewRow.style.display === 'none') {
    $('df-' + i).textContent = fieldsJson;
    viewRow.style.display = ''; btn.textContent = 'Hide';
  } else { viewRow.style.display = 'none'; btn.textContent = 'View'; }
}

async function deleteFirestoreDoc(docPath, collection) {
  if (!confirm(`Delete document "${shortName(docPath)}"?`)) return;
  try {
    await api('/api/firestore/documents?' + new URLSearchParams({ path: docPath }), { method: 'DELETE' });
    _fs.docs = (await api('/api/firestore/documents?' + new URLSearchParams({ collection })))
      .map(d => ({...d, _id: shortName(d.name||''), _fields: Object.keys(d.fields||{}).length}));
    renderFirestoreDocs();
  } catch (e) { alert('Delete failed: ' + e.message); }
}

// ── Secret Manager ────────────────────────────────────────────────────────────
async function loadSecrets() {
  $('secrets-content').innerHTML = '<div class="loading">Loading secrets&hellip;</div>';
  try {
    _sm.secrets = await api('/api/secretmanager/secrets');
    renderSecrets();
  } catch (e) {
    $('secrets-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function sortSm(f) { _sm.sort = _sm.sort.f===f ? {f,d:_sm.sort.d*-1} : {f,d:1}; _sm.pg.page=0; renderSecrets(); }
function pgSm(d)   { _pgChg(_sm.pg, _sm.secrets.length, d); renderSecrets(); }
function szSm(v)   { _sm.pg.size=v==='all'?'all':+v; _sm.pg.page=0; renderSecrets(); }

function renderSecrets() {
  const sorted = _srt(_sm.secrets, _sm.sort, ['versionCount']);
  const {slice, total} = _pg(sorted, _sm.pg);
  const sth = (l,f) => _sth(l, f, _sm.sort, 'sortSm');
  let rows = '';
  for (let i = 0; i < slice.length; i++) {
    const s = slice[i];
    const short = shortName(s.name);
    const sn = JSON.stringify(short);
    rows += `<tr id="sr-${i}">
      <td class="mono">${esc(short)}</td>
      <td><span class="cnt">${s.versionCount}</span></td>
      <td class="dim">${s.createTime ? s.createTime.substring(0, 10) : '&mdash;'}</td>
      <td class="actions">
        <button class="btn btn-ghost" id="sv-btn-${i}" onclick="toggleSecretVersions(${i}, ${sn})">Versions</button>
        <button class="btn btn-danger" onclick="deleteSecret(${sn})">Delete</button>
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
      <div class="card-header"><h2>Secrets</h2><span class="cnt">${total}</span></div>
      <table>
        <thead><tr>${sth('Name','name')}${sth('Versions','versionCount')}${sth('Created','createTime')}<th>Actions</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="4" class="empty">No secrets</td></tr>'}</tbody>
      </table>
      ${_pgBar(total, _sm.pg, 'pgSm', 'szSm')}
    </div>`;
}

async function toggleSecretVersions(i, secretName) {
  const row = $('sv-' + i), btn = $('sv-btn-' + i);
  if (row.style.display === 'none') {
    row.style.display = ''; btn.textContent = 'Hide';
    const container = $('sv-content-' + i);
    try {
      const versions = await api('/api/secretmanager/versions?' + new URLSearchParams({ secret: secretName }));
      if (!versions.length) { container.innerHTML = '<div class="empty" style="padding:.5rem">No versions</div>'; return; }
      let vRows = '';
      for (let j = 0; j < versions.length; j++) {
        const v = versions[j];
        const vNum = v.name.split('/versions/').pop();
        const sn = JSON.stringify(secretName), vn = JSON.stringify(vNum);
        vRows += `<tr>
          <td class="mono">v${esc(vNum)}</td>
          <td><span class="state ${stateClass(v.state)}">${v.state}</span></td>
          <td class="dim">${v.createTime ? v.createTime.substring(0, 19).replace('T', ' ') : '&mdash;'}</td>
          <td class="actions">${v.state === 'ENABLED'
            ? `<button class="btn btn-ghost" id="val-btn-${i}-${j}" onclick="toggleSecretValue(${i}, ${j}, ${sn}, ${vn})">Reveal</button>`
            : ''}</td>
        </tr>
        <tr id="val-row-${i}-${j}" style="display:none">
          <td colspan="4" class="secret-val"><div class="pre" id="val-pre-${i}-${j}"></div></td>
        </tr>`;
      }
      container.innerHTML = `<table class="sub-table">
        <thead><tr><th>Version</th><th>State</th><th>Created</th><th>Actions</th></tr></thead>
        <tbody>${vRows}</tbody></table>`;
    } catch (e) { container.innerHTML = `<div class="empty" style="padding:.5rem">Error: ${esc(e.message)}</div>`; }
  } else { row.style.display = 'none'; btn.textContent = 'Versions'; }
}

async function toggleSecretValue(i, j, secretName, versionId) {
  const valRow = $(`val-row-${i}-${j}`), btn = $(`val-btn-${i}-${j}`);
  if (valRow.style.display === 'none') {
    valRow.style.display = ''; btn.textContent = 'Hide';
    const pre = $(`val-pre-${i}-${j}`);
    pre.textContent = 'Loading\u2026';
    try {
      const res = await api('/api/secretmanager/value?' + new URLSearchParams({ secret: secretName, version: versionId }));
      pre.textContent = res.value;
    } catch (e) { pre.textContent = 'Error: ' + e.message; }
  } else { valRow.style.display = 'none'; btn.textContent = 'Reveal'; }
}

async function deleteSecret(name) {
  if (!confirm(`Delete secret "${name}" and all its versions?`)) return;
  try {
    await api('/api/secretmanager/secrets?' + new URLSearchParams({ secret: name }), { method: 'DELETE' });
    _sm.secrets = await api('/api/secretmanager/secrets');
    renderSecrets();
  } catch (e) { alert('Delete failed: ' + e.message); }
}

// ── Cloud Tasks ───────────────────────────────────────────────────────────────
async function loadTasks() {
  _tk.queue = null;
  $('tasks-nav').style.display = 'none';
  $('tasks-content').innerHTML = '<div class="loading">Loading queues&hellip;</div>';
  try {
    _tk.queues = await api('/api/tasks/queues');
    renderTaskQueues();
  } catch (e) {
    $('tasks-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function sortTkQ(f) { _tk.qSort = _tk.qSort.f===f ? {f,d:_tk.qSort.d*-1} : {f,d:1}; renderTaskQueues(); }

function renderTaskQueues() {
  $('tasks-nav').style.display = 'none';
  const sorted = _srt(_tk.queues, _tk.qSort, ['taskCount']);
  const sth = (l,f) => _sth(l, f, _tk.qSort, 'sortTkQ');
  let rows = '';
  for (const q of sorted) {
    const short = shortName(q.name);
    const qn = JSON.stringify(q.name);
    rows += `<tr>
      <td><button class="btn-link" onclick="loadQueueTasks(${qn})">${esc(short)}</button></td>
      <td><span class="state ${stateClass(q.state)}">${q.state}</span></td>
      <td><span class="cnt">${q.taskCount}</span></td>
      <td class="dim">${q.rateLimits ? q.rateLimits.maxDispatchesPerSecond + '/s' : '&mdash;'}</td>
    </tr>`;
  }
  $('tasks-content').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>Queues</h2><span class="cnt">${sorted.length}</span></div>
      <table>
        <thead><tr>${sth('Queue','name')}${sth('State','state')}${sth('Tasks','taskCount')}<th>Rate Limit</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="4" class="empty">No queues</td></tr>'}</tbody>
      </table>
    </div>`;
}

async function loadQueueTasks(queueName) {
  _tk.queue = queueName;
  _tk.tPg.page = 0;
  _tk.tSort = {f:'name', d:1};
  const short = shortName(queueName);
  $('tasks-nav').style.display = 'flex';
  $('tasks-nav').innerHTML = `<a onclick="loadTasks()">Queues</a> <span>&#8250;</span> ${esc(short)}`;
  $('tasks-content').innerHTML = '<div class="loading">Loading tasks&hellip;</div>';
  try {
    _tk.tasks = await api('/api/tasks/tasks?' + new URLSearchParams({ queue: queueName }));
    renderQueueTasks();
  } catch (e) {
    $('tasks-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function sortTkT(f) { _tk.tSort = _tk.tSort.f===f ? {f,d:_tk.tSort.d*-1} : {f,d:1}; _tk.tPg.page=0; renderQueueTasks(); }
function pgTkT(d)   { _pgChg(_tk.tPg, _tk.tasks.length, d); renderQueueTasks(); }
function szTkT(v)   { _tk.tPg.size=v==='all'?'all':+v; _tk.tPg.page=0; renderQueueTasks(); }

function renderQueueTasks() {
  const queueName = _tk.queue;
  const sorted = _srt(_tk.tasks, _tk.tSort, ['dispatchCount']);
  const {slice, total} = _pg(sorted, _tk.tPg);
  const sth = (l,f,e='') => _sth(l, f, _tk.tSort, 'sortTkT', e);
  const qn = JSON.stringify(queueName);
  let rows = '';
  for (const t of slice) {
    const taskId = shortName(t.name);
    const url = t.httpRequest ? t.httpRequest.url : '&mdash;';
    const method = t.httpRequest ? t.httpRequest.httpMethod : '';
    const tn = JSON.stringify(t.name);
    rows += `<tr>
      <td class="mono">${esc(taskId)}</td>
      <td class="dim">${method ? `<code>${method}</code> ` : ''}${esc(url)}</td>
      <td class="dim" style="white-space:nowrap">${t.scheduleTime ? t.scheduleTime.substring(0, 19).replace('T', ' ') : '&mdash;'}</td>
      <td class="dim">${t.dispatchCount || 0}</td>
      <td class="actions"><button class="btn btn-danger" onclick="deleteTask(${tn}, ${qn})">Delete</button></td>
    </tr>`;
  }
  $('tasks-content').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>${esc(shortName(queueName))}</h2><span class="cnt">${total} task${total!==1?'s':''}</span></div>
      <table>
        <thead><tr>
          ${sth('Task ID','name')}
          <th>HTTP Request</th>
          ${sth('Schedule Time','scheduleTime')}
          ${sth('Dispatches','dispatchCount')}
          <th>Actions</th>
        </tr></thead>
        <tbody>${rows || '<tr><td colspan="5" class="empty">No tasks</td></tr>'}</tbody>
      </table>
      ${_pgBar(total, _tk.tPg, 'pgTkT', 'szTkT')}
    </div>`;
}

async function deleteTask(taskName, queueName) {
  if (!confirm(`Delete task "${shortName(taskName)}"?`)) return;
  try {
    await api('/api/tasks/task?' + new URLSearchParams({ task: taskName }), { method: 'DELETE' });
    _tk.tasks = await api('/api/tasks/tasks?' + new URLSearchParams({ queue: queueName }));
    renderQueueTasks();
  } catch (e) { alert('Delete failed: ' + e.message); }
}

// ── BigQuery ──────────────────────────────────────────────────────────────────
async function loadBigQuery() {
  _bq.dataset = null;
  $('bq-nav').style.display = 'none';
  $('bq-content').innerHTML = '<div class="loading">Loading datasets&hellip;</div>';
  try {
    _bq.datasets = await api('/api/bigquery/datasets');
    renderBQDatasets();
  } catch (e) {
    $('bq-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function sortBqDs(f) { _bq.dsSort = _bq.dsSort.f===f ? {f,d:_bq.dsSort.d*-1} : {f,d:1}; renderBQDatasets(); }

function renderBQDatasets() {
  $('bq-nav').style.display = 'none';
  const sorted = _srt(_bq.datasets, _bq.dsSort, ['tableCount']);
  const sth = (l,f) => _sth(l, f, _bq.dsSort, 'sortBqDs');
  let rows = '';
  for (const d of sorted) {
    const id = d.datasetId;
    const dn = JSON.stringify(id);
    rows += `<tr>
      <td><button class="btn-link" onclick="loadBQTables(${dn})">${esc(id)}</button></td>
      <td><span class="cnt">${d.tableCount}</span></td>
      <td class="dim">${d.location || 'US'}</td>
      <td class="actions"><button class="btn btn-danger" onclick="deleteBQDataset(${dn})">Delete</button></td>
    </tr>`;
  }
  $('bq-content').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>Datasets</h2><span class="cnt">${sorted.length}</span></div>
      <table>
        <thead><tr>${sth('Dataset','datasetId')}${sth('Tables','tableCount')}${sth('Location','location')}<th>Actions</th></tr></thead>
        <tbody>${rows || '<tr><td colspan="4" class="empty">No datasets</td></tr>'}</tbody>
      </table>
    </div>`;
}

async function loadBQTables(dataset) {
  _bq.dataset = dataset;
  _bq.tbSort = {f:'tableId', d:1};
  $('bq-nav').style.display = 'flex';
  $('bq-nav').innerHTML = `<a onclick="loadBigQuery()">Datasets</a> <span>&#8250;</span> ${esc(dataset)}`;
  $('bq-content').innerHTML = '<div class="loading">Loading tables&hellip;</div>';
  try {
    _bq.tables = (await api('/api/bigquery/tables?' + new URLSearchParams({ dataset })))
      .map(t => ({...t, _fields: (t.schema && t.schema.fields) ? t.schema.fields.length : 0}));
    renderBQTables();
  } catch (e) {
    $('bq-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function sortBqTb(f) { _bq.tbSort = _bq.tbSort.f===f ? {f,d:_bq.tbSort.d*-1} : {f,d:1}; renderBQTables(); }

function renderBQTables() {
  const dataset = _bq.dataset;
  const sorted = _srt(_bq.tables, _bq.tbSort, ['_fields','numRows']);
  const ds = JSON.stringify(dataset);
  const sth = (l,f) => _sth(l, f, _bq.tbSort, 'sortBqTb');
  let rows = '';
  for (const t of sorted) {
    const tn = JSON.stringify(t.tableId);
    rows += `<tr>
      <td><button class="btn-link" onclick="loadBQPreview(${ds}, ${tn})">${esc(t.tableId)}</button></td>
      <td class="dim">${t._fields} field${t._fields !== 1 ? 's' : ''}</td>
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
        <h2>${esc(dataset)}</h2><span class="cnt">${sorted.length} table${sorted.length!==1?'s':''}</span>
      </div>
      <table>
        <thead><tr>${sth('Table','tableId')}${sth('Schema','_fields')}${sth('Rows','numRows')}<th>Actions</th></tr></thead>
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

// ── Scheduler ─────────────────────────────────────────────────────────────────
async function loadScheduler() {
  $('sched-content').innerHTML = '<div class="loading">Loading jobs&hellip;</div>';
  try {
    _sc.jobs = (await api('/api/scheduler/jobs')).map(j => {
      const id = (j.name || '').split('/').pop();
      return { ...j, _id: id };
    });
    renderSchedulerJobs();
  } catch (e) {
    $('sched-content').innerHTML = `<div class="empty">Error: ${esc(e.message)}</div>`;
  }
}

function sortSc(f) {
  if (_sc.sort.f === f) _sc.sort.d *= -1; else { _sc.sort.f = f; _sc.sort.d = 1; }
  _sc.pg.page = 0;
  renderSchedulerJobs();
}
function pgSc(d) { _pgChg(_sc.pg, _sc.jobs.length, d); renderSchedulerJobs(); }
function szSc(v) { _sc.pg.size = +v; _sc.pg.page = 0; renderSchedulerJobs(); }

function renderSchedulerJobs() {
  const sorted = _srt(_sc.jobs, _sc.sort, []);
  const { slice, total } = _pg(sorted, _sc.pg);
  let rows = '';
  for (const j of slice) {
    const n = JSON.stringify(j._id);
    const stateTag = `<span class="state ${stateClass(j.state)}">${esc(j.state || 'UNKNOWN')}</span>`;
    const last = j.lastAttemptTime ? j.lastAttemptTime.substring(0, 19).replace('T', ' ') : '&mdash;';
    const paused = (j.state || '') === 'PAUSED';
    rows += `<tr>
      <td class="mono">${esc(j._id)}</td>
      <td class="mono dim">${esc(j.schedule || '')}</td>
      <td>${stateTag}</td>
      <td class="dim">${last}</td>
      <td class="actions">
        <button class="btn btn-ghost" onclick="runSchedJob(${n})" title="Force run">Run</button>
        ${paused
          ? `<button class="btn btn-secondary" onclick="resumeSchedJob(${n})">Resume</button>`
          : `<button class="btn btn-ghost" onclick="pauseSchedJob(${n})">Pause</button>`}
        <button class="btn btn-danger" onclick="deleteSchedJob(${n})">Delete</button>
      </td>
    </tr>`;
  }
  const si = _sc.sort;
  const th = (label, f) => _sth(label, f, si, `sortSc('${f}')`);
  $('sched-content').innerHTML = `
    <div class="card">
      <div class="card-header"><h2>Jobs</h2><span class="cnt">${total}</span></div>
      <table>
        <thead><tr>
          ${th('Job ID','_id')}${th('Schedule','schedule')}${th('State','state')}${th('Last Run','lastAttemptTime')}
          <th>Actions</th>
        </tr></thead>
        <tbody>${rows || '<tr><td colspan="5" class="empty">No jobs</td></tr>'}</tbody>
      </table>
      ${_pgBar(total, _sc.pg, 'pgSc', 'szSc')}
    </div>`;
}

async function _reloadSc() {
  _sc.jobs = (await api('/api/scheduler/jobs')).map(j => {
    const id = (j.name || '').split('/').pop();
    return { ...j, _id: id };
  });
  renderSchedulerJobs();
}

async function runSchedJob(id) {
  try {
    await api(`/api/scheduler/jobs/${encodeURIComponent(id)}:run`, { method: 'POST' });
    await _reloadSc();
  } catch (e) { alert('Run failed: ' + e.message); }
}

async function pauseSchedJob(id) {
  try {
    await api(`/api/scheduler/jobs/${encodeURIComponent(id)}:pause`, { method: 'POST' });
    await _reloadSc();
  } catch (e) { alert('Pause failed: ' + e.message); }
}

async function resumeSchedJob(id) {
  try {
    await api(`/api/scheduler/jobs/${encodeURIComponent(id)}:resume`, { method: 'POST' });
    await _reloadSc();
  } catch (e) { alert('Resume failed: ' + e.message); }
}

async function deleteSchedJob(id) {
  if (!confirm(`Delete job "${id}"?`)) return;
  try {
    await api(`/api/scheduler/jobs/${encodeURIComponent(id)}`, { method: 'DELETE' });
    await _reloadSc();
  } catch (e) { alert('Delete failed: ' + e.message); }
}

// ── Boot ──────────────────────────────────────────────────────────────────────
loadOverview();
