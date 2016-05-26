#!/bin/bash

cd `dirname $0`

APPJSON=`cat lobster.json`

case "$1" in
  start)
    echo $APPJSON | pm2 start -
    ;;
  stop)
    pm2 stop all
    ;;
  restart)
    pm2 restart all
    ;;
  *)
  echo "Usage: sbin/lobster.sh {start|stop|restart}"
  exit 1
esac