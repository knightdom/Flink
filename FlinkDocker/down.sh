#!/bin/bash
#echo $1 | sed "s/\(\(\/\)\|\(\?\)\|\(\&\)\)/\\\\\1/g"

if [ ! -d /var/log/nginx ]
then
    mkdir /var/log/nginx
fi

docker stop flinkdocker_FlinkTaskManager_1 flinkdocker_FlinkTaskManager_2 flinkdocker_FlinkJobManager_1
docker rm flinkdocker_FlinkTaskManager_1 flinkdocker_FlinkTaskManager_2 flinkdocker_FlinkJobManager_1
docker-compose up -d