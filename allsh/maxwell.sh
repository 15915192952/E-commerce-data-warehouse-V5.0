#!/bin/bash
case $1 in
	"start")
	/export/server/maxwell/bin/maxwell --config /export/server/maxwell/config.properties --daemon
	;;
	"stop")
	ps -ef | grep maxwell | grep -v grep | awk '{print $2}' | xargs kill
	;;
esac
