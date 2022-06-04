#!/bin/bash

case $1 in
"start")
        echo " --------启动 linux2 日志数据flume-------"
        ssh linux2 "nohup /export/server/flume-1.9.0/bin/flume-ng agent -n a1 -c /export/server/flume-1.9.0/conf -f /export/server/flume-1.9.0/job/kafka_to_hdfs.conf >/dev/null 2>&1 &"
;;

"stop")

        echo " --------停止 linux2 日志数据flume-------"
        ssh linux2 "ps -ef | grep kafka_to_hdfs.conf | grep -v grep |awk '{print \$2}' | xargs -n1 kill"
;;
esac

