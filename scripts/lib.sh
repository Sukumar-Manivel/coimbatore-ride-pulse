#!/usr/bin/env bash
# Shared helpers for the pipeline and benchmark scripts (run inside the 'master' container).
set -euo pipefail
PROJECT=/project
JOBS=$PROJECT/jobs
OUT=$PROJECT/output
LOGS=$OUT/logs
STREAMING_JAR=$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar | head -1)
mkdir -p "$OUT" "$LOGS"

log() { echo -e "\033[1;36m[ride-pulse]\033[0m $*"; }

wait_for_cluster() {
  log "waiting for HDFS to leave safe mode ..."
  hdfs dfsadmin -safemode wait >/dev/null
  local live=0
  for _ in $(seq 1 60); do
    live=$(yarn node -list 2>/dev/null | grep -c RUNNING || true)
    [ "$live" -ge 1 ] && break
    sleep 2
  done
  log "YARN NodeManagers running: $live"
}

# run_job <run-name> <tag> <input> <output> <reducers> <mapper.py> <reducer|NONE> <combiner|NONE> [extra -D/-option args...]
# Runs one Hadoop Streaming job, records wall time + counters to $OUT/runs.jsonl
run_job() {
  local name=$1 tag=$2 input=$3 output=$4 reducers=$5 mapper=$6 reducer=$7 combiner=$8
  shift 8
  local files="$JOBS/rideparse.py,$JOBS/$mapper"
  local args=(-D mapreduce.job.name="ride-pulse: $name" -D mapreduce.job.reduces="$reducers")
  local tail_args=()
  # generic -D options must come before -files/-mapper
  while [ $# -gt 0 ]; do
    case "$1" in
      -D) args+=(-D "$2"); shift 2 ;;
      *)  tail_args+=("$1"); shift ;;
    esac
  done
  [ "$reducer" != "NONE" ] && [ "${reducer%%.py}" != "$reducer" ] && files="$files,$JOBS/$reducer"
  [ "$combiner" != "NONE" ] && [ "$combiner" != "$reducer" ] && files="$files,$JOBS/$combiner"
  hdfs dfs -rm -r -f -skipTrash "$output" >/dev/null 2>&1 || true
  local cmd=(hadoop jar "$STREAMING_JAR" "${args[@]}" -files "$files"
             -input "$input" -output "$output" -mapper "python3 $mapper")
  if [ "$reducer" = "NONE" ]; then :
  elif [ "${reducer%%.py}" != "$reducer" ]; then cmd+=(-reducer "python3 $reducer")
  else cmd+=(-reducer "$reducer"); fi
  [ "$combiner" != "NONE" ] && cmd+=(-combiner "python3 $combiner")
  cmd+=("${tail_args[@]}")

  local logf="$LOGS/${name// /_}.log"
  log "running: $name  (reducers=$reducers, combiner=$combiner)"
  local t0=$(date +%s.%N)
  set +e
  "${cmd[@]}" >"$logf" 2>&1
  local rc=$?
  set -e
  local t1=$(date +%s.%N)
  python3 "$PROJECT/scripts/parse_counters.py" "$logf" "$name" "$tag" "$rc" "$t0" "$t1" \
          "$reducers" "$combiner" "$input" >> "$OUT/runs.jsonl"
  if [ $rc -ne 0 ]; then
    log "\033[1;31mFAILED\033[0m: $name - last lines of $logf:"; tail -25 "$logf"; return $rc
  fi
  log "done: $name in $(python3 -c "print(round($t1-$t0,1))")s"
}
