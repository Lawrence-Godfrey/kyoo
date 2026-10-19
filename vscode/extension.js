// Runs on the machine that runs the jobs (extensionKind workspace), so over Remote SSH it sits next to the daemon.
const vscode = require('vscode');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');

const POLL_MS = 2000;
const DONE = new Set(['succeeded', 'failed', 'cancelled', 'skipped']);
const ICON = {
  running: ['play', 'testing.iconPassed'], queued: ['clock', 'testing.iconQueued'], held: ['debug-pause', 'testing.iconQueued'],
  succeeded: ['check', 'testing.iconPassed'], failed: ['error', 'testing.iconFailed'], cancelled: ['circle-slash', 'disabledForeground'], skipped: ['debug-step-over', 'disabledForeground'],
};

function dataDir() {
  if (process.env.KYOO_HOME) return process.env.KYOO_HOME;
  if (process.platform === 'darwin') return path.join(os.homedir(), 'Library', 'Application Support', 'kyoo');
  return path.join(process.env.XDG_DATA_HOME || path.join(os.homedir(), '.local', 'share'), 'kyoo');
}

class Daemon {
  constructor() { this.url = null; this.token = null; }
  locate() {
    try {
      const dir = dataDir();
      this.url = `http://127.0.0.1:${parseInt(fs.readFileSync(path.join(dir, 'daemon.port'), 'utf8').trim(), 10)}`;
      this.token = fs.readFileSync(path.join(dir, 'token'), 'utf8').trim();
      return true;
    } catch (e) { return false; }
  }
  async call(p, method = 'GET') {
    if (!this.url && !this.locate()) throw new Error('no daemon');
    const r = await fetch(this.url + p, { method, headers: { Authorization: 'Bearer ' + this.token }, signal: AbortSignal.timeout(5000) });
    if (!r.ok) throw new Error(`${method} ${p}: ${r.status}`);
    return r;
  }
  json(p, method) { return this.call(p, method).then(r => r.json()); }
  text(p) { return this.call(p).then(r => r.text()); }
  start() {
    return new Promise((resolve, reject) => {
      const p = spawn('uvx', ['kyoo', 'start'], { env: process.env, stdio: ['ignore', 'pipe', 'pipe'] });
      let err = '';
      p.stderr.on('data', b => { err += b; });
      p.on('error', e => reject(new Error(`could not run uvx: ${e.message}`)));
      p.on('exit', code => (code === 0 ? resolve() : reject(new Error(err.trim().split('\n').pop() || `uvx kyoo start exited ${code}`))));
    });
  }
}

const dur = s => s < 60 ? `${s}s` : s < 3600 ? `${Math.floor(s / 60)}m${String(s % 60).padStart(2, '0')}` : `${Math.floor(s / 3600)}h${String(Math.floor(s % 3600 / 60)).padStart(2, '0')}`;
const ago = ts => dur(Math.round(Date.now() / 1000 - ts));
const took = j => j.started_at ? dur(Math.round((j.finished_at || Date.now() / 1000) - j.started_at)) : null;
const titleOf = j => j.description || j.command;

// What a queued job waits on: unfinished dependencies, else the running jobs holding a lock it needs (the scheduler's order).
function waitingOn(j, byId, running) {
  if (j.status !== 'queued') return [];
  const deps = (j.after || []).filter(id => byId[id] && !DONE.has(byId[id].status));
  return deps.length ? deps : [...new Set(running.filter(r => (r.locks || []).some(l => (j.locks || []).includes(l))).map(r => r.id))];
}

class Jobs {  // one provider for both views; `active` decides which subset it shows
  constructor(state, active) { this.state = state; this.active = active; this._change = new vscode.EventEmitter(); this.onDidChangeTreeData = this._change.event; this.items = new Map(); }
  refresh() { this._change.fire(); }
  getParent(el) { return el.parent; }
  getTreeItem(el) { return el; }
  getChildren(el) {
    if (el) return el.details || [];
    const jobs = this.active ? this.state.jobs.filter(j => !DONE.has(j.status)) : this.state.jobs;
    this.items = new Map(jobs.map(j => [j.id, this.jobItem(j)]));
    return [...this.items.values()];
  }
  jobItem(j) {
    const { byId, running } = this.state, waits = waitingOn(j, byId, running);
    const it = new vscode.TreeItem(titleOf(j), vscode.TreeItemCollapsibleState.Collapsed);
    it.id = `${this.active ? 'a' : 'r'}-${j.id}`;
    const [icon, color] = ICON[j.status] || ['circle-outline', 'foreground'];
    it.iconPath = new vscode.ThemeIcon(icon, new vscode.ThemeColor(color));
    const bits = [`#${j.id}`];
    if (waits.length) bits.push(`after ${waits.join(', ')}`);
    else if (j.status === 'running') bits.push(took(j));
    else if (j.status === 'failed') bits.push(j.exit_code != null ? `exit ${j.exit_code}` : 'failed');
    else if (DONE.has(j.status)) bits.push(j.status === 'succeeded' ? took(j) : j.status);
    it.description = bits.filter(Boolean).join(' · ');
    it.tooltip = `${titleOf(j)}\n#${j.id} · ${j.status}`;
    it.contextValue = `job ${DONE.has(j.status) ? 'done' : 'active'}`;
    it.job = j;
    it.details = this.details(j, waits, it);
    return it;
  }
  details(j, waits, parent) {
    const { byId, running, jobs } = this.state;
    const row = (key, label, description, icon, owner, extra = {}) => Object.assign(new vscode.TreeItem(label, vscode.TreeItemCollapsibleState.None),
      { id: `${owner.id}/${key}`, description, iconPath: new vscode.ThemeIcon(icon), tooltip: description ? `${label} ${description}` : label, parent: owner, contextValue: 'detail' }, extra);
    const link = (key, word, id, owner) => { const o = byId[id]; return row(`${key}-${id}`, `${word} #${id}`, o ? titleOf(o) : 'not in the last 50', 'arrow-right', owner,
      { command: { command: 'kyoo.reveal', title: 'Go to job', arguments: [id, this.active] } }); };

    const bits = [];
    if (j.status === 'failed' && j.exit_code != null) bits.push(`exit ${j.exit_code}`);
    if (j.error && !/^exit code -?\d+$/.test(j.error)) bits.push(j.error);
    if (waits.length) bits.push(`after ${waits.join(', ')}`);
    else if (j.status === 'running') bits.push(took(j));
    const status = row('status', j.status, bits.join(' · '), (ICON[j.status] || ['circle-outline'])[0], parent,
      { collapsibleState: vscode.TreeItemCollapsibleState.Collapsed, tooltip: [j.status, ...bits].join(' · ') });
    status.details = [row('queued', 'queued', `${ago(j.created_at)} ago${j.queued_by ? ` by ${j.queued_by}` : ''}`, 'history', status)];
    if (j.started_at) status.details.push(row('ran', j.status === 'running' ? 'running for' : 'ran for', took(j), 'watch', status));
    if (j.locks && j.locks.length) status.details.push(row('locks', 'locks', j.locks.join(' '), 'lock', status));
    for (const id of [...new Set([...(j.after || []), ...waits])]) status.details.push(link('after', 'after', id, status));
    const neededBy = jobs.filter(k => k.id !== j.id && ((k.after || []).includes(j.id) || waitingOn(k, byId, running).includes(j.id)));
    for (const k of neededBy) status.details.push(link('next', 'needed by', k.id, status));

    const out = [status];
    out.push(row('cmd', 'command', j.command, 'terminal', parent, { tooltip: j.command, command: { command: 'kyoo.openCommand', title: 'Open command', arguments: [j.id] } }));
    out.push(row('cwd', 'cwd', j.cwd, 'folder', parent));
    if (j.preflight) out.push(row('preflight', 'preflight', j.preflight, 'shield', parent));
    out.push(row('log', 'Open log', '', 'output', parent, { command: { command: 'kyoo.openLog', title: 'Open log', arguments: [j.id] } }));
    return out;
  }
}

class Docs {  // read-only documents: kyoo:/job-<id>.log holds only the command's output, kyoo:/job-<id>.sh its command
  constructor(daemon) { this.daemon = daemon; this._change = new vscode.EventEmitter(); this.onDidChange = this._change.event; }
  uri(id, ext = 'log') { return vscode.Uri.parse(`kyoo:/job-${id}.${ext}`); }
  idOf(uri) { return parseInt(uri.path.match(/job-(\d+)/)[1], 10); }
  async provideTextDocumentContent(uri) {
    try {
      if (uri.path.endsWith('.sh')) return (await this.daemon.json(`/jobs/${this.idOf(uri)}`)).command + '\n';
      const log = await this.daemon.text(`/jobs/${this.idOf(uri)}/log?tail=5000`);
      return log.replace(/^# kyoo job \d+: .*\n(# cwd .*\n)?/, '');  // the daemon's own header, not the command's output
    } catch (e) { return ''; }
  }
  tick(byId) {
    const now = Date.now() / 1000;
    for (const ed of vscode.window.visibleTextEditors) {
      if (ed.document.uri.scheme !== 'kyoo' || !ed.document.uri.path.endsWith('.log')) continue;
      const job = byId[this.idOf(ed.document.uri)];
      if (!job || (DONE.has(job.status) && now - (job.finished_at || 0) > 5)) continue;  // one last read just after it finishes
      const last = ed.document.lineCount - 1, atBottom = ed.visibleRanges.some(r => r.end.line >= last - 1);
      this._change.fire(ed.document.uri);
      if (atBottom) setTimeout(() => { const n = ed.document.lineCount - 1; ed.revealRange(new vscode.Range(n, 0, n, 0)); }, 150);
    }
  }
}

function activate(context) {
  const daemon = new Daemon();
  const state = { jobs: [], byId: {}, running: [], down: true };
  const active = new Jobs(state, true), recent = new Jobs(state, false), logs = new Docs(daemon);
  const views = {
    active: vscode.window.createTreeView('kyoo.active', { treeDataProvider: active }),
    recent: vscode.window.createTreeView('kyoo.recent', { treeDataProvider: recent }),
  };
  const bar = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 50);
  bar.command = 'workbench.view.extension.kyoo'; bar.show();

  async function poll() {
    try {
      const [health, recentJobs, activeJobs] = await Promise.all([daemon.json('/health'), daemon.json('/jobs?limit=50'), daemon.json('/jobs?limit=200&status=queued,held,running')]);
      const seen = new Set(recentJobs.map(j => j.id));
      state.jobs = [...activeJobs.filter(j => !seen.has(j.id)), ...recentJobs].sort((a, b) => b.id - a.id);
      state.byId = Object.fromEntries(state.jobs.map(j => [j.id, j]));
      state.running = state.jobs.filter(j => j.status === 'running');
      state.down = false;
      const nRun = state.running.length, nWait = state.jobs.filter(j => ['queued', 'held'].includes(j.status)).length;
      const stopped = health.scheduler_alive === false;
      const summary = stopped ? 'scheduler stopped, restart the daemon' : `${nRun} running · ${nWait} waiting`;
      views.active.message = stopped ? `Scheduler stopped: ${health.last_error || 'unknown error'}. Restart the daemon.` : undefined;
      views.active.description = summary;
      views.active.badge = nRun + nWait ? { value: nRun + nWait, tooltip: `${nRun} running, ${nWait} waiting` } : undefined;
      bar.text = `$(play) ${nRun} $(clock) ${nWait}`;
      bar.tooltip = `kyoo: ${summary}`;
      bar.backgroundColor = stopped ? new vscode.ThemeColor('statusBarItem.errorBackground') : undefined;
    } catch (e) {
      state.jobs = []; state.byId = {}; state.running = []; state.down = true; daemon.url = null;
      views.active.description = 'daemon not running'; views.active.message = undefined; views.active.badge = undefined;
      bar.text = '$(circle-slash) kyoo'; bar.tooltip = 'kyoo daemon is not running'; bar.backgroundColor = new vscode.ThemeColor('statusBarItem.warningBackground');
    }
    vscode.commands.executeCommand('setContext', 'kyoo.down', state.down);
    active.refresh(); recent.refresh(); logs.tick(state.byId);
  }
  const timer = setInterval(poll, POLL_MS); poll();

  const open = async (id, ext) => {
    const doc = await vscode.workspace.openTextDocument(logs.uri(id, ext));
    const ed = await vscode.window.showTextDocument(doc, { viewColumn: vscode.ViewColumn.Active, preview: true });
    if (ext === 'log') { const n = doc.lineCount - 1; ed.revealRange(new vscode.Range(n, 0, n, 0)); }
  };

  context.subscriptions.push(
    views.active, views.recent, bar, { dispose: () => clearInterval(timer) },
    vscode.workspace.registerTextDocumentContentProvider('kyoo', logs),
    vscode.commands.registerCommand('kyoo.refresh', poll),
    vscode.commands.registerCommand('kyoo.start', async () => {
      try { await daemon.start(); await poll(); } catch (e) { vscode.window.showErrorMessage(`kyoo: ${e.message}`); }
    }),
    vscode.commands.registerCommand('kyoo.cancel', async it => {
      try { await daemon.json(`/jobs/${it.job.id}/cancel`, 'POST'); await poll(); } catch (e) { vscode.window.showErrorMessage(`kyoo: ${e.message}`); }
    }),
    vscode.commands.registerCommand('kyoo.openLog', arg => open(typeof arg === 'number' ? arg : arg.job.id, 'log')),
    vscode.commands.registerCommand('kyoo.openCommand', id => open(id, 'sh')),
    vscode.commands.registerCommand('kyoo.reveal', async (id, inActive) => {
      // Prefer the section the click came from; a finished job only exists in Recent.
      for (const [provider, view] of inActive ? [[active, views.active], [recent, views.recent]] : [[recent, views.recent], [active, views.active]]) {
        const el = provider.items.get(id);
        if (el) { await view.reveal(el, { select: true, focus: true, expand: false }); return; }
      }
    }),
  );
}

module.exports = { activate, deactivate() {} };
