#!/bin/bash

cd `dirname $0`

APPJSON='lobster.json'

ulimit -n 1000000

case "$1" in
  start)
    pm2 start $APPJSON
    ;;
  stop)
    pm2 stop $APPJSON
    ;;
  restart)
    pm2 restart $APPJSON
    ;;
  *)
  echo "Usage: sbin/lobster.sh {start|stop|restart}"
  exit 1
esac