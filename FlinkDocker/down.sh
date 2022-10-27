#!/bin/bash
#echo $1 | sed "s/\(\(\/\)\|\(\?\)\|\(\&\)\)/\\\\\1/g"

if [ ! -d /var/log/nginx ]
then
    mkdir /var/log/nginx
fi

docker stop FlinkTaskManager1 FlinkTaskManager2 FlinkJobManager
docker rm FlinkTaskManager1 FlinkTaskManager2 FlinkJobManager
docker build -t kinghtdom/hadoop3.3-flink1.15 .
docker-compose up -d