#!/usr/bin/env bash
# Step 3: Hadoop experiments - reducers, combiner on/off, data scaling.
# Each experiment re-runs Job 2 (zone x hour stats) on the raw data.
source /project/scripts/lib.sh
wait_for_cluster
rm -f $OUT/runs.jsonl

# A. Reducer count (combiner ON, full data)
for r in 1 2 4 8; do
  run_job "B reducers=$r" reducers /rides/raw /bench/r$r $r j2_zone_hour_mapper.py sum_reducer.py sum_reducer.py
done
# B. Combiner OFF (compare with reducers=4 above)
run_job "B combiner=off" combiner /rides/raw /bench/nocomb 4 j2_zone_hour_mapper.py sum_reducer.py NONE
# Same experiment on the heavier Job 3 (7 keys per ride) where the combiner matters most
run_job "B J3 combiner=on" combiner_j3 /rides/raw /bench/j3c 4 j3_situation_mapper.py sum_reducer.py sum_reducer.py
run_job "B J3 combiner=off" combiner_j3 /rides/raw /bench/j3n 4 j3_situation_mapper.py sum_reducer.py NONE
# C. Data scaling: 3, 6, 12, 24 months (reducers=4, combiner ON)
run_job "B months=3"  scaling "/rides/raw/rides_2024_0[1-3].csv" /bench/m3 4 j2_zone_hour_mapper.py sum_reducer.py sum_reducer.py
run_job "B months=6"  scaling "/rides/raw/rides_2024_0[1-6].csv" /bench/m6 4 j2_zone_hour_mapper.py sum_reducer.py sum_reducer.py
run_job "B months=12" scaling "/rides/raw/rides_2024_*.csv"      /bench/m12 4 j2_zone_hour_mapper.py sum_reducer.py sum_reducer.py
run_job "B months=24" scaling "/rides/raw"                        /bench/m24 4 j2_zone_hour_mapper.py sum_reducer.py sum_reducer.py

hdfs dfs -rm -r -f -skipTrash /bench >/dev/null 2>&1 || true
cp $OUT/runs.jsonl $OUT/benchmark_runs.jsonl
python3 $PROJECT/scripts/export_results.py --source hadoop
log "benchmarks done and added to the dashboard"
