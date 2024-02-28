#!/usr/bin/env bash
set -e
set -x
# 开发环境基于docker启动xxl-job-admin的脚本
PORT='3306'
PASSWD='xxlpasswd@123'
USERNAME="xxluser"
XXLIMAGE="xuxueli/xxl-job-admin:2.3.0"
XXLSQLFILE="tables_xxl_job23.sql"
TOKEN="1234567890"

# 如果Mac m2, 拉取镜像的时候需要加platform
# docker pull --platform=linux/amd64 mysql:5.7

if [ ! "$(docker ps -q -f "name=xxl-db")" ];then
    # docker pull --platform=linux/amd64 mysql:5.7
    docker run -d --name xxl-db -p $PORT:3306 -e MYSQL_PASSWORD=$PASSWD -e MYSQL_USER=$USERNAME -e MYSQL_DATABASE=xxl_job -e MYSQL_ALLOW_EMPTY_PASSWORD=true mysql:5.7
    echo 'create xxl-db container ok'

    if [ ! -f $XXLSQLFILE ];then
        # 需要下载对应xxl版本的SQL文件，不然会数据库对应不上部分功能用不了
        wget https://raw.githubusercontent.com/xuxueli/xxl-job/master/doc/db/tables_xxl_job.sql -O $XXLSQLFILE
    fi

    set +e
    while true; do
        docker exec -i xxl-db sh -c "exec mysql -u${USERNAME} -p${PASSWD}" < ./$XXLSQLFILE
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
    # docker pull --platform=linux/amd64 $XXLIMAGE
    docker run -d --link xxl-db:xxl-db --name xxl-job-admin \
    -e PARAMS="--spring.datasource.url=jdbc:mysql://xxl-db:3306/xxl_job?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true --spring.datasource.username=$USERNAME --spring.datasource.password=$PASSWD --xxl.job.accessToken=$TOKEN" \
    -p 8080:8080 \
    -v /tmp:/data/applogs  \
    $XXLIMAGE

    echo 'create xxl-job-admin container ok'
else
    echo 'xxl-job-admin container already existed.'
fi
