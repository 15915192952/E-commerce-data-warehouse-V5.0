#!/bin/bash

case $1 in
"start"){
        for i in linux1 linux2 linux3
        do
                echo " --------启动 $i 采集flume-------"
                ssh $i "nohup /export/server/flume-1.9.0/bin/flume-ng agent -n a1 -c /export/server/flume-1.9.0/conf/ -f /export/server/flume-1.9.0/job/file_to_kafka.conf >/dev/null 2>&1 & "
        done
};; 
"stop"){
        for i in linux1 linux2 linux3
        do
                echo " --------停止 $i 采集flume-------"
                ssh $i "ps -ef | grep file_to_kafka.conf | grep -v grep |awk  '{print \$2}' | xargs -n1 kill -9 "
        done

};;
esac

