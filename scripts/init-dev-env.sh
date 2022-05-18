#!/usr/bin/env bash
set -e
set -x
# 开发环境基于docker启动xxl-job-admin的脚本
PORT='3306'
PASSWD='xxlpasswd@123'
USERNAME="xxluser"

if [ ! "$(docker ps -q -f "name=xxl-db")" ];then
    docker run -d --name xxl-db -p $PORT:3306 -e MYSQL_PASSWORD=$PASSWD -e MYSQL_USER=$USERNAME -e MYSQL_DATABASE=xxl_job -e MYSQL_ALLOW_EMPTY_PASSWORD=true mysql:5.7
    echo 'create xxl-db container ok'

    if [ ! -f "tables_xxl_job.sql" ];then
        wget https://raw.githubusercontent.com/xuxueli/xxl-job/master/doc/db/tables_xxl_job.sql
    fi

    set +e
    while true; do
        docker exec -i xxl-db sh -c "exec mysql -u${USERNAME} -p${PASSWD}" < ./tables_xxl_job.sql
        if [ $? -eq 0 ]; then
            break;
        fi
        sleep 2
    done
    echo 'init mysql database ok'

else
    echo 'xxl-db container already existed.'
fi

set -e
if [ ! $(docker ps -q -f "name=xxl-job-admin") ];then
    docker run -d --link xxl-db:xxl-db --name xxl-job-admin \
    -e PARAMS="--spring.datasource.url=jdbc:mysql://xxl-db:3306/xxl_job?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true --spring.datasource.username=$USERNAME --spring.datasource.password=$PASSWD" \
    -p 8080:8080 \
    -v /tmp:/data/applogs  \
    xuxueli/xxl-job-admin:2.3.0

    echo 'create xxl-job-admin container ok'
else
    echo 'xxl-job-admin container already existed.'
fi
