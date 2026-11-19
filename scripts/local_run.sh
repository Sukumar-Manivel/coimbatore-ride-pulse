#!/usr/bin/env bash
# No-Hadoop smoke test: runs the SAME mapper / combiner / reducer scripts with Unix pipes,
# imitating MapReduce: one "map task" per input file (mapper | sort | combiner), then
# one merge (sort | reducer). Useful to check logic without a cluster.
#   bash scripts/local_run.sh <folder-with-rides_*.csv> [parallel-tasks]
set -euo pipefail
cd "$(dirname "$0")/.."
IN=$(cd "${1:?folder with rides_*.csv}" && pwd)
P=${2:-2}
J=$PWD/jobs; O=$PWD/output/results; T=$PWD/output/local_tmp
mkdir -p "$O" output/logs; rm -rf "$T"; mkdir -p "$T/clean" "$T/map"
export LC_ALL=C J T

echo "[local] J1 clean (map-only, one task per file)"
ls "$IN"/*.csv | xargs -P "$P" -I{} sh -c 'b=$(basename {} .csv); python3 $J/j1_clean_mapper.py < {} > $T/clean/$b.csv 2> $T/clean/$b.err'
cat "$T"/clean/*.err > output/logs/J1_local.err

maptask() { # name mapper combiner-command
  ls "$T"/clean/*.csv | xargs -P "$P" -I{} sh -c "python3 $J/$2 < {} | sort -S 400M | $3 > $T/map/$1.\$(basename {}).part"
}
reduce() { # name reducer-command output
  cat "$T"/map/$1.*.part | sort -S 800M | $2 > "$3"; rm -f "$T"/map/$1.*.part
}
SUM="python3 $J/sum_reducer.py"
echo "[local] J2 zone-hour";  maptask j2 j2_zone_hour_mapper.py "$SUM"; reduce j2 "$SUM" "$O/j2_zone_hour.tsv"
echo "[local] J3 situations"; maptask j3 j3_situation_mapper.py "$SUM"; reduce j3 "$SUM" "$O/j3_situations.tsv"
echo "[local] J4 routes";     maptask j4 j4_routes_mapper.py "$SUM";   reduce j4 "python3 $J/j4_routes_reducer.py" "$O/j4_routes.tsv"
echo "[local] J5 shortage";   python3 $J/j5_shortage_mapper.py < "$O/j2_zone_hour.tsv" | sort | $SUM | python3 $J/j5_shortage_reducer.py > "$O/j5_shortage.tsv"
echo "[local] J6 ranking";    python3 $J/j6_rank_mapper.py < "$O/j5_shortage.tsv" | sort > "$O/j6_ranking.tsv"
du -sb "$T/clean" | awk '{print $1}' > "$O/clean_bytes.txt"
rm -rf "$T"
python3 scripts/export_results.py --source local --quality-log output/logs/J1_local.err --raw-dir "$IN"
