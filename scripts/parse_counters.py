#!/usr/bin/env python3
"""Reads the console log of one 'hadoop jar' run and prints one JSON line with
wall time and the useful job counters."""
import json, re, sys

logf, name, tag, rc, t0, t1, reducers, combiner, inp = sys.argv[1:10]
text = open(logf, errors="replace").read()

WANT = {
    "Map input records": "map_input_records",
    "Map output records": "map_output_records",
    "Map output bytes": "map_output_bytes",
    "Map output materialized bytes": "map_output_materialized_bytes",
    "Combine input records": "combine_input_records",
    "Combine output records": "combine_output_records",
    "Reduce shuffle bytes": "reduce_shuffle_bytes",
    "Reduce input records": "reduce_input_records",
    "Reduce output records": "reduce_output_records",
    "Launched map tasks": "map_tasks",
    "Launched reduce tasks": "reduce_tasks",
    "Data-local map tasks": "data_local_map_tasks",
    "Rack-local map tasks": "rack_local_map_tasks",
    "Total time spent by all map tasks (ms)": "map_task_ms",
    "Total time spent by all reduce tasks (ms)": "reduce_task_ms",
    "CPU time spent (ms)": "cpu_ms",
    "HDFS: Number of bytes read": "hdfs_bytes_read",
    "HDFS: Number of bytes written": "hdfs_bytes_written",
    "Spilled Records": "spilled_records",
}
rec = {"name": name, "tag": tag, "ok": rc == "0", "seconds": round(float(t1) - float(t0), 2),
       "reducers": int(reducers), "combiner": combiner != "NONE", "input": inp,
       "counters": {}, "data_quality": {}}
section = None
for line in text.splitlines():
    s = line.strip()
    if s and "=" not in s:
        section = s
        continue
    m = re.match(r"^(.+?)=(\d+)$", s)
    if not m:
        continue
    k, v = m.group(1).strip(), int(m.group(2))
    if section == "DataQuality":
        rec["data_quality"][k] = v
    elif k in WANT:
        rec["counters"][WANT[k]] = v
m = re.search(r"Running job: (job_\d+_\d+)", text)
if m:
    rec["job_id"] = m.group(1)
print(json.dumps(rec))
