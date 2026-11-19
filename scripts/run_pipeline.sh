#!/usr/bin/env bash
# Step 2: run the 6 MapReduce jobs on the cluster, pull results, build dashboard data.
source /project/scripts/lib.sh
R=${REDUCERS:-4}
wait_for_cluster
rm -f $OUT/runs.jsonl

# J1 clean: map-only, counters per data problem
run_job "J1 Clean" pipeline /rides/raw /rides/clean 0 j1_clean_mapper.py NONE NONE
# J2 zone x hour stats: combiner
run_job "J2 Zone-Hour Stats" pipeline /rides/clean /rides/out/j2_zone_hour $R j2_zone_hour_mapper.py sum_reducer.py sum_reducer.py
# J3 situations & time series: many keys per ride, combiner
run_job "J3 Situations" pipeline /rides/clean /rides/out/j3_situations $R j3_situation_mapper.py sum_reducer.py sum_reducer.py
# J4 routes: custom partitioner (by origin zone) so reducers can pick top destinations per origin
run_job "J4 Top Routes" pipeline /rides/clean /rides/out/j4_routes $R j4_routes_mapper.py j4_routes_reducer.py sum_reducer.py \
  -D mapreduce.map.output.key.field.separator='|' -D mapreduce.partition.keypartitioner.options=-k1,1 \
  -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner
# J5 shortage index: chained on J2 output
run_job "J5 Shortage Index" pipeline /rides/out/j2_zone_hour /rides/out/j5_shortage 2 j5_shortage_mapper.py j5_shortage_reducer.py sum_reducer.py
# J6 global ranking: chained on J5, one reducer = totally ordered list
run_job "J6 Hotspot Ranking" pipeline /rides/out/j5_shortage /rides/out/j6_ranking 1 j6_rank_mapper.py cat NONE

log "pulling results from HDFS ..."
mkdir -p $OUT/results
for j in j2_zone_hour j3_situations j4_routes j5_shortage j6_ranking; do
  rm -f $OUT/results/$j.tsv
  hdfs dfs -getmerge /rides/out/$j $OUT/results/$j.tsv
done
hdfs dfs -du -s /rides/clean | awk '{print $1}' > $OUT/results/clean_bytes.txt
cp $OUT/runs.jsonl $OUT/pipeline_runs.jsonl

python3 $PROJECT/scripts/export_results.py --source hadoop
log "dashboard data written. Open http://localhost:8000"
