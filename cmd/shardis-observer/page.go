package main

// page is a deliberately tiny live view, enough to watch a failover until
// the real website (Phase 6) exists: the topology as a table, and the
// event stream as a scrolling log.
const page = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Shardis observer</title>
<style>
:root { --bg:#fcfcfb; --ink:#0b0b0b; --muted:#52514e; --line:#e8e7e3; --bad:#e34948; --ok:#008300; }
@media (prefers-color-scheme: dark) { :root { --bg:#1a1a19; --ink:#f0efec; --muted:#c3c2b7; --line:#333331; --bad:#e66767; --ok:#3fae3f; } }
body { margin:0; padding:16px; background:var(--bg); color:var(--ink); font:14px/1.4 system-ui, sans-serif; }
h1 { font-size:18px; margin:0 0 12px; } .muted { color:var(--muted); }
table { border-collapse:collapse; width:100%; margin-bottom:16px; }
td, th { border-bottom:1px solid var(--line); padding:4px 8px; text-align:left; white-space:nowrap; }
.fail { color:var(--bad); font-weight:600; } .up { color:var(--ok); }
#log { font:12px/1.5 ui-monospace, monospace; height:50vh; overflow:auto; border:1px solid var(--line); padding:8px; }
</style></head><body>
<h1>Shardis observer <span id="status" class="muted">connecting…</span></h1>
<table><thead><tr><th>node</th><th>role</th><th>slots</th><th>keys</th><th>epoch</th><th>flags</th></tr></thead><tbody id="nodes"></tbody></table>
<div id="log"></div>
<script>
const nodes = document.getElementById('nodes'), log = document.getElementById('log'), status = document.getElementById('status');
const skip = new Set(['gossip_send', 'gossip_recv']); // too chatty for a log
function count(slots) { return (slots || []).reduce((n, r) => n + r[1] - r[0] + 1, 0); }
function connect() {
  const ws = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws');
  ws.onopen = () => status.textContent = 'live';
  ws.onclose = () => { status.textContent = 'disconnected, retrying…'; setTimeout(connect, 1000); };
  ws.onmessage = (m) => {
    const msg = JSON.parse(m.data);
    if (msg.kind === 'topology') {
      nodes.replaceChildren(...msg.nodes.sort((a, b) => a.name.localeCompare(b.name)).map(n => {
        const tr = document.createElement('tr');
        const flags = (n.flags || []).join(',');
        for (const v of [n.name, n.role + (n.primary ? ' of ' + n.primary : ''), count(n.slots), n.keys, n.epoch, flags || (n.up ? 'ok' : 'unreachable')]) {
          const td = document.createElement('td'); td.textContent = v; tr.appendChild(td);
        }
        if (flags.includes('fail') || !n.up) tr.className = 'fail';
        return tr;
      }));
    } else if (!skip.has(msg.type)) {
      const line = document.createElement('div');
      const t = new Date(msg.t).toISOString().slice(11, 23);
      line.textContent = t + '  ' + (msg.node || '') + '  ' + (msg.kind === 'action' ? 'ACTION ' + msg.action : msg.type) + '  ' + JSON.stringify(msg.data || msg.nodes || '');
      if (/fail|minority|demoted|won|takeover|ACTION/.test(line.textContent)) line.className = 'fail';
      log.prepend(line);
      while (log.childNodes.length > 500) log.lastChild.remove();
    }
  };
}
connect();
</script></body></html>`
