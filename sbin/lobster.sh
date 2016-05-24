#!/bin/bash

cd `dirname $0`

APPJSON=`cat lobster.json`

case "$1" in
  start)
    echo $APPJSON | pm2 start -
    ;;
  stop)
    echo $APPJSON | pm2 stop -
    ;;
  restart)
    echo $APPJSON | pm2 restart - 
    ;;
  *)
  echo "Usage: sbin/lobster.sh {start|stop|restart}"
  exit 1
esac