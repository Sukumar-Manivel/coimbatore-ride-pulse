# One image for every node of the Coimbatore Ride Pulse Hadoop cluster.
FROM eclipse-temurin:8-jdk-jammy

ARG HADOOP_VERSION=3.3.6
ARG HADOOP_URL=https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz

RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      python3 python3-numpy curl procps netcat-openbsd ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN curl -fSL --retry 5 "$HADOOP_URL" -o /tmp/hadoop.tgz \
 && tar -xzf /tmp/hadoop.tgz -C /opt \
 && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop \
 && rm -f /tmp/hadoop.tgz \
 && rm -rf /opt/hadoop/share/doc

ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    HADOOP_VERSION=${HADOOP_VERSION} \
    PATH=/opt/hadoop/bin:/opt/hadoop/sbin:$PATH \
    HDFS_NAMENODE_USER=root \
    HDFS_DATANODE_USER=root \
    HDFS_SECONDARYNAMENODE_USER=root \
    YARN_RESOURCEMANAGER_USER=root \
    YARN_NODEMANAGER_USER=root \
    MAPRED_HISTORYSERVER_USER=root \
    PYTHONUNBUFFERED=1

COPY config/*.xml /opt/hadoop/etc/hadoop/
COPY config/workers /opt/hadoop/etc/hadoop/workers
COPY start-node.sh /usr/local/bin/start-node.sh

# Tolerate Windows (CRLF) checkouts, and pin JAVA_HOME + daemon heap for a laptop.
RUN sed -i 's/\r$//' /usr/local/bin/start-node.sh /opt/hadoop/etc/hadoop/workers \
 && chmod +x /usr/local/bin/start-node.sh \
 && echo "export JAVA_HOME=${JAVA_HOME}" >> /opt/hadoop/etc/hadoop/hadoop-env.sh \
 && echo "export HADOOP_HEAPSIZE_MAX=512m" >> /opt/hadoop/etc/hadoop/hadoop-env.sh \
 && mkdir -p /hadoop/dfs/name /hadoop/dfs/data /staging

ENTRYPOINT ["/usr/local/bin/start-node.sh"]
