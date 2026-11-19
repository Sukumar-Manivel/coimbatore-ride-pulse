#!/usr/bin/env bash
# Step 1: generate the 2-year Coimbatore ride data and load it into HDFS.
#   bash /project/scripts/generate_and_load.sh            (default ~25k rides/day, ~19M rows)
#   RIDES_PER_DAY=5000 bash /project/scripts/generate_and_load.sh   (quick small run)
source /project/scripts/lib.sh
RIDES_PER_DAY=${RIDES_PER_DAY:-25000}
wait_for_cluster

if [ -z "$(ls /staging/raw/*.csv 2>/dev/null)" ] || [ "${REGENERATE:-0}" = "1" ]; then
  rm -rf /staging/raw && mkdir -p /staging/raw
  log "generating rides (rides/day=$RIDES_PER_DAY) into /staging/raw ..."
  python3 $PROJECT/data/generate_rides.py --out /staging/raw --rides-per-day "$RIDES_PER_DAY"
else
  log "raw CSV already in /staging/raw (set REGENERATE=1 to rebuild)"
fi
du -sh /staging/raw

log "loading into HDFS /rides/raw (replication 2, 128 MB blocks) ..."
hdfs dfs -rm -r -f -skipTrash /rides/raw >/dev/null 2>&1 || true
hdfs dfs -mkdir -p /rides/raw
t0=$(date +%s)
hdfs dfs -put /staging/raw/*.csv /rides/raw/
log "HDFS load took $(( $(date +%s) - t0 ))s"
hdfs dfs -ls -h /rides/raw | head -5

log "collecting HDFS block layout ..."
hdfs fsck /rides/raw -files -blocks -locations > $LOGS/fsck.txt 2>/dev/null || true
hdfs dfsadmin -report > $LOGS/dfsadmin.txt 2>/dev/null || true
python3 $PROJECT/scripts/hdfs_info.py $LOGS/fsck.txt $LOGS/dfsadmin.txt > $OUT/hdfs_info.json
cat $OUT/hdfs_info.json
