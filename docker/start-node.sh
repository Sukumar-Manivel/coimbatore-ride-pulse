#!/usr/bin/env bash
# Starts the Hadoop daemons for one container. Usage: start-node.sh master|worker
set -e
ROLE="${1:-worker}"
LOGDIR="$HADOOP_HOME/logs"
mkdir -p "$LOGDIR"

if [ "$ROLE" = "master" ]; then
  if [ ! -f /hadoop/dfs/name/current/VERSION ]; then
    echo "[master] formatting HDFS NameNode (first start only)"
    
    hdfs namenode -format -force -nonInteractive -clusterId ride-pulse
  fi
  hdfs --daemon start namenode
  yarn --daemon start resourcemanager
  mapred --daemon start historyserver
  # Serve the dashboard on :8000
  (cd /project/dashboard && python3 -m http.server 8000 >/tmp/dashboard.log 2>&1 &)
  echo "[master] NameNode :9870  YARN :8088  JobHistory :19888  Dashboard :8000"
else
  echo "[$(hostname)] waiting for NameNode on master:9000 ..."
  until nc -z master 9000; do sleep 2; done
  hdfs --daemon start datanode
  yarn --daemon start nodemanager
  echo "[$(hostname)] DataNode + NodeManager started"
fi

# Keep the container alive and show daemon logs.
sleep 3
exec tail -F "$LOGDIR"/*.log
