"""Generates dashboards/shardis.json (python3 make_dashboard.py). Kept as code
so the panels are reviewable; the JSON is what Grafana loads."""
import json

DS = {"type": "prometheus", "uid": "prometheus"}
panels, y = [], 0

def panel(title, exprs, kind="timeseries", w=12, h=7, unit="short", desc="", extra=None):
    global y
    x = 0 if not panels or panels[-1]["gridPos"]["x"] + panels[-1]["gridPos"]["w"] + w > 24 else panels[-1]["gridPos"]["x"] + panels[-1]["gridPos"]["w"]
    if x == 0 and panels:
        y += panels[-1]["gridPos"]["h"]
    p = {"type": kind, "title": title, "description": desc, "datasource": DS,
         "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "targets": [{"refId": chr(65 + i), "datasource": DS, "expr": e, "legendFormat": l} for i, (e, l) in enumerate(exprs)],
         "fieldConfig": {"defaults": {"unit": unit}, "overrides": []}, "options": {}}
    if extra:
        for k, v in extra.items():
            p[k] = v
    panels.append(p)

state_opts = {"options": {"showValue": "never", "mergeValues": True, "rowHeight": 0.8},
              "fieldConfig": {"defaults": {"unit": "short", "color": {"mode": "thresholds"},
                              "thresholds": {"mode": "absolute", "steps": [{"color": "#e34948", "value": None}, {"color": "#1baf7a", "value": 1}]}}, "overrides": []}}
role_opts = {"options": {"showValue": "never", "mergeValues": True, "rowHeight": 0.8},
             "fieldConfig": {"defaults": {"unit": "short", "color": {"mode": "thresholds"},
                             "mappings": [{"type": "value", "options": {"0": {"text": "replica"}, "1": {"text": "primary"}}}],
                             "thresholds": {"mode": "absolute", "steps": [{"color": "#a3a29c", "value": None}, {"color": "#2a78d6", "value": 1}]}}, "overrides": []}}

panel("Role per node (a failover flips a pair)", [("shardis_is_primary", "{{node}}")], "state-timeline", 12, 7,
      desc="Blue = primary, grey = replica. When a primary dies its replica turns blue; when the old one comes back it's grey.", extra=role_opts)
panel("Cluster state per node", [("shardis_cluster_state_ok", "{{node}}")], "state-timeline", 12, 7,
      desc="Green = cluster_state:ok. Red while a slot's owner is FAIL (full coverage) or the node is in a minority.", extra=state_opts)
panel("Commands per second", [("sum by (node) (rate(shardis_commands_total[10s]))", "{{node}}")], unit="ops")
panel("Command latency p99 (server side)", [("histogram_quantile(0.99, sum by (le, node) (rate(shardis_command_duration_seconds_bucket[30s])))", "{{node}}")], unit="s")
panel("Slots owned", [("shardis_cluster_slots_owned", "{{node}}")])
panel("Nodes suspected / failed (each node's view)", [("max(shardis_cluster_nodes_pfail)", "PFAIL (max over nodes)"), ("max(shardis_cluster_nodes_fail)", "FAIL (max over nodes)")])
panel("Failure handling (per minute)", [("sum(increase(shardis_cluster_failovers_won_total[1m]))", "failovers won"),
      ("sum(increase(shardis_cluster_elections_total[1m]))", "elections started"),
      ("sum(increase(shardis_cluster_takeovers_total[1m]))", "graceful takeovers"),
      ("sum(increase(shardis_cluster_fail_marks_total[1m]))", "FAIL marks")], w=8)
panel("Replication lag", [("shardis_replica_lag_bytes", "{{node}} → {{replica}}")], unit="bytes", w=8)
panel("Gossip messages sent per second", [("sum by (type) (rate(shardis_cluster_bus_messages_sent_total[10s]))", "{{type}}")], w=8)
panel("Keys", [("shardis_keys", "{{node}}")], w=8)
panel("Memory (keyspace estimate)", [("shardis_used_memory_bytes", "{{node}}")], unit="bytes", w=8)
panel("GC pause p99 (Go runtime)", [("max by (node) (go_gc_duration_seconds{quantile=\"1\"})", "{{node}}")], unit="s", w=8)

dash = {"uid": "shardis", "title": "Shardis cluster", "schemaVersion": 39, "version": 1, "refresh": "2s",
        "time": {"from": "now-5m", "to": "now"}, "tags": ["shardis"], "panels": panels,
        "description": "Live view of a Shardis cluster (Iteration 15). Kill a primary and watch the role and state timelines."}
json.dump(dash, open("dashboards/shardis.json", "w"), indent=1)
print("wrote dashboards/shardis.json with", len(panels), "panels")
