#!/usr/bin/env bash
# Everything, in order: data -> HDFS -> 6 jobs -> benchmarks -> dashboard.
#   SKIP_BENCH=1 bash /project/scripts/run_all.sh    to skip the ~15 min of experiments
set -euo pipefail
sed -i 's/\r$//' /project/scripts/*.sh 2>/dev/null || true
T0=$(date +%s)
bash /project/scripts/generate_and_load.sh
bash /project/scripts/run_pipeline.sh
if [ "${SKIP_BENCH:-0}" != "1" ]; then bash /project/scripts/benchmark.sh; fi
echo
echo "================================================================"
echo " All done in $(( ($(date +%s) - T0) / 60 )) min."
echo " Dashboard   : http://localhost:8000"
echo " HDFS UI     : http://localhost:9870    YARN UI: http://localhost:8088"
echo "================================================================"
