#!/usr/bin/env python3
"""Summarise 'hdfs fsck -files -blocks -locations' + 'hdfs dfsadmin -report' as JSON."""
import json, re, sys
from collections import Counter

fsck = open(sys.argv[1], errors="replace").read()
report = open(sys.argv[2], errors="replace").read() if len(sys.argv) > 2 else ""

def grab(pat, text, cast=int):
    m = re.search(pat, text)
    return cast(m.group(1)) if m else None

info = {
    "total_size_bytes": grab(r"Total size:\s+(\d+)", fsck),
    "total_files": grab(r"Total files:\s+(\d+)", fsck),
    "total_blocks": grab(r"Total blocks \(validated\):\s+(\d+)", fsck),
    "default_replication": grab(r"Default replication factor:\s+(\d+)", fsck),
    "avg_replication": grab(r"Average block replication:\s+([\d.]+)", fsck, float),
    "status": "HEALTHY" if "is HEALTHY" in fsck else ("CORRUPT" if "CORRUPT" in fsck else "UNKNOWN"),
}
# Count block replicas per DataNode from the -locations output.
per_node = Counter()
for m in re.finditer(r"DatanodeInfoWithStorage\[([^\]:]+)", fsck):
    per_node[m.group(1)] += 1
# Map IPs to hostnames using the dfsadmin report.
names = {}
for block in report.split("Name: ")[1:]:
    ip = block.split(":", 1)[0].strip()
    host = grab(r"Hostname:\s+(\S+)", block, str)
    used = grab(r"DFS Used:\s+(\d+)", block)
    names[ip] = host or ip
    if host:
        info.setdefault("datanodes", {})[host] = {"dfs_used_bytes": used, "block_replicas": 0}
for ip, n in per_node.items():
    host = names.get(ip, ip)
    info.setdefault("datanodes", {}).setdefault(host, {"dfs_used_bytes": None})["block_replicas"] = n
info["live_datanodes"] = grab(r"Live datanodes \((\d+)\)", report)
print(json.dumps(info))
