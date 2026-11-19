#!/usr/bin/env python3
"""Turns the MapReduce outputs (output/results/*.tsv) + run logs into dashboard/data.js.
Nothing here re-computes the analysis: it reshapes Hadoop's results for the browser."""
import argparse, csv, json, os, re, time
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RES = os.path.join(ROOT, "output", "results")
OUT = os.path.join(ROOT, "output")
STATUS = {"balanced": 0, "need_drivers": 1, "surplus": 2}
BANDS = ["night", "morning", "midday", "evening", "late"]


def rows(name):
    p = os.path.join(RES, name)
    if not os.path.exists(p):
        return []
    with open(p) as f:
        for line in f:
            line = line.rstrip("\n")
            if line:
                k, _, v = line.partition("\t")
                yield k, v


def nums(v):
    out = []
    for x in v.split(","):
        fx = float(x)
        out.append(int(fx) if fx == int(fx) else round(fx, 2))
    return out


def jsonl(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="hadoop", choices=["hadoop", "local"])
    ap.add_argument("--quality-log", help="local mode: stderr of the J1 mapper")
    ap.add_argument("--raw-dir", help="local mode: folder of raw CSVs (for size)")
    a = ap.parse_args()

    with open(os.path.join(ROOT, "data", "zones.csv")) as f:
        zones = [dict(name=r["zone"], lat=float(r["lat"]), lng=float(r["lng"]), type=r["type"])
                 for r in csv.DictReader(f)]
    zi = {z["name"]: i for i, z in enumerate(zones)}
    nz = len(zones)
    DT = {"weekday": 0, "weekend": 1}
    W = {"clear": 0, "rain": 1, "all": 2}

    # J2: cells[zone][daytype][weather(clear,rain)][hour] = vec(9)
    cells = [[[[None] * 24 for _ in range(2)] for _ in range(2)] for _ in range(nz)]
    for k, v in rows("j2_zone_hour.tsv"):
        z, dt, h, w = k.split("|")
        cells[zi[z]][DT[dt]][W[w]][int(h)] = nums(v)

    # J5: shortage[zone][daytype][weather(clear,rain,all)][hour] = [idx,status,req,nd_rate,wait,long_rate,drivers,surge]
    shortage = [[[[None] * 24 for _ in range(3)] for _ in range(2)] for _ in range(nz)]
    for k, v in rows("j5_shortage.tsv"):
        z, dt, h, w = k.split("|")
        p = v.split(",")
        shortage[zi[z]][DT[dt]][W[w]][int(h)] = [float(p[0]), STATUS[p[1]], int(p[2]), float(p[3]),
                                                float(p[4]), float(p[5]), float(p[6]), float(p[7])]

    # J6: ranking (already sorted by Hadoop's shuffle)
    ranking = []
    for k, v in rows("j6_ranking.tsv"):
        zdh, rest = v.split(",", 1)
        z, dt, h = zdh.split("|")
        p = rest.split(",")
        ranking.append(dict(zone=z, daytype=dt, hour=int(h), index=float(p[0]), status=p[1],
                            requests=int(p[2]), nd_rate=float(p[3]), wait=float(p[4]),
                            drivers=float(p[6]), unserved=int(p[9])))

    # J3 families
    yearly, year_zone, daily, monthly, festivals = {}, {}, [], [], {}
    hourly = {"weekday": [None] * 24, "weekend": [None] * 24}
    drops = {"weekday": [0] * 24, "weekend": [0] * 24}
    weather = {"clear": [None] * 24, "rain": [None] * 24}
    for k, v in rows("j3_situations.tsv"):
        p = k.split("|")
        n = nums(v)
        if p[0] == "Y":
            yearly[p[1]] = n
        elif p[0] == "YZ":
            year_zone.setdefault(p[1], {})[p[2]] = n
        elif p[0] == "H":
            hourly[p[1]][int(p[2])] = n
        elif p[0] == "DO":
            drops[p[1]][int(p[2])] = n[8]
        elif p[0] == "W":
            weather[p[1]][int(p[2])] = n
        elif p[0] == "F":
            festivals.setdefault(p[1], [None] * 24)[int(p[2])] = n
        elif p[0] == "D":
            daily.append([p[1], n[0], n[1], n[2], n[4], n[9]])
        elif p[0] == "M":
            monthly.append([p[1]] + n)
    daily.sort()
    monthly.sort()
    days = {"weekday": 0, "weekend": 0}
    for d in daily:
        y, m, dd = map(int, d[0].split("-"))
        days["weekend" if date(y, m, dd).weekday() >= 5 else "weekday"] += 1

    # J4 routes per band
    routes = {b: [] for b in BANDS}
    for k, v in rows("j4_routes.tsv"):
        pz, dz, band = k.split("|")
        routes[band].append([zi[pz], zi[dz]] + nums(v))
    for b in BANDS:
        routes[b].sort(key=lambda r: -r[2])

    # Runs, quality, HDFS
    pipeline = jsonl(os.path.join(OUT, "pipeline_runs.jsonl")) if a.source == "hadoop" else []
    bench = jsonl(os.path.join(OUT, "benchmark_runs.jsonl")) if a.source == "hadoop" else []
    quality = {}
    for r in pipeline:
        if r["name"].startswith("J1"):
            quality = r.get("data_quality", {})
    if a.quality_log and os.path.exists(a.quality_log):
        for line in open(a.quality_log):
            m = re.match(r"reporter:counter:DataQuality,(\w+),(\d+)", line.strip())
            if m:
                quality[m.group(1)] = quality.get(m.group(1), 0) + int(m.group(2))
    hdfs = {}
    hp = os.path.join(OUT, "hdfs_info.json")
    if a.source == "hadoop" and os.path.exists(hp):
        hdfs = json.load(open(hp))
    raw_bytes = hdfs.get("total_size_bytes")
    if a.raw_dir:
        raw_bytes = sum(os.path.getsize(os.path.join(a.raw_dir, f)) for f in os.listdir(a.raw_dir)
                        if f.endswith(".csv"))
    clean_bytes = None
    cb = os.path.join(RES, "clean_bytes.txt")
    if os.path.exists(cb):
        clean_bytes = int(open(cb).read().split()[0])

    data = dict(
        meta=dict(source=a.source, generated_at=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                  date_from=daily[0][0] if daily else None, date_to=daily[-1][0] if daily else None,
                  days=days, raw_bytes=raw_bytes, clean_bytes=clean_bytes,
                  last_run_at=max((os.path.getmtime(os.path.join(OUT, f)) for f in
                                   ("pipeline_runs.jsonl",) if os.path.exists(os.path.join(OUT, f))),
                                  default=None)),
        zones=zones, cells=cells, shortage=shortage, ranking=ranking, yearly=yearly,
        yearZone=year_zone, hourly=hourly, dropoffs=drops, weather=weather, festivals=festivals,
        daily=daily, monthly=monthly, routes=routes, bands=BANDS, quality=quality,
        pipeline=pipeline, benchmarks=bench, hdfs=hdfs,
    )
    path = os.path.join(ROOT, "dashboard", "data.js")
    with open(path, "w") as f:
        f.write("// Generated by scripts/export_results.py from Hadoop MapReduce output. Do not edit.\n")
        f.write("window.RIDE_DATA = ")
        json.dump(data, f, separators=(",", ":"))
        f.write(";\n")
    print("wrote %s (%.0f KB)" % (path, os.path.getsize(path) / 1024))


if __name__ == "__main__":
    main()
