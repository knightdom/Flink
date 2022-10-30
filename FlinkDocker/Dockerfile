FROM docker.io/flink:1.16.0-scala_2.12-java8
LABEL maintainer="Edgar <kinghtdom@gmail.com>"
LABEL description="Docker image with Flink (1.15) and Hadoop (3.3), based on flink:1.16.0-scala_2.12-java8"

USER root

ENV HADOOP_HOME="/opt/hadoop"
ENV HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
ENV HADOOP_LOG_DIR="/var/log/hadoop"
ENV PATH="$HADOOP_HOME/hadoop/sbin:$HADOOP_HOME/bin:$PATH"

WORKDIR /opt

RUN sed -i s@/deb.debian.org@/mirrors.aliyun.com/@g /etc/apt/sources.list
RUN apt-get clean && apt-get update && apt-get install -y openssh-server vim net-tools inetutils-ping lsof netcat openjdk-8-jdk

RUN ssh-keygen -t rsa -f /root/.ssh/id_rsa -P '' && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys

COPY config/* /tmp/

RUN mv /tmp/hadoop-3.3.4.tar.gz /opt && \
    tar -xzvf hadoop-3.3.4.tar.gz && \
	mv hadoop-3.3.4 hadoop && \
	rm -rf hadoop-3.3.4.tar.gz && \
	mkdir /var/log/hadoop

RUN mkdir -p /root/hdfs/namenode && \
    mkdir -p /root/hdfs/datanode && \
    mkdir -p /root/script

RUN mv /tmp/ssh_config /root/.ssh/config && \
    mv /tmp/hadoop-env.sh $HADOOP_CONF_DIR/hadoop-env.sh && \
    mv /tmp/hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml && \
    mv /tmp/core-site.xml $HADOOP_CONF_DIR/core-site.xml && \
    mv /tmp/mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml && \
    mv /tmp/yarn-site.xml $HADOOP_CONF_DIR/yarn-site.xml && \
    mv /tmp/workers $HADOOP_CONF_DIR/workers

COPY start-hadoop.sh /opt/start-hadoop.sh

RUN chmod +x /opt/start-hadoop.sh && \
    chmod +x $HADOOP_HOME/sbin/start-dfs.sh && \
    chmod +x $HADOOP_HOME/sbin/start-yarn.sh

#RUN hdfs namenode -format

#RUN sed -i '$a\if [ -f /root/script/autoservice.sh ]; then ' /root/.bashrc && \
#    sed -i '$a\     . /root/script/autoservice.sh' /root/.bashrc && \
#    sed -i '$a\fi' /root/.bashrc

#ENTRYPOINT [ "/root/script/autoservice.sh" ]

#ENTRYPOINT ["service", "ssh", "start"]