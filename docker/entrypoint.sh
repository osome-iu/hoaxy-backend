#!/bin/sh

case "$1" in

  'server')
  	#ARGS="-dir /data"
  	#if [ -n "$MASTER_PORT_9333_TCP_ADDR" ] ; then
	#	ARGS="$ARGS "
	#fi
  	exec hoaxy $@ $ARGS
  	;;

  'bash')
        exec /bin/bash
        ;;

  'config')
  	exec hoaxy $@ $ARGS
	;;

  'version')
  	ARGS="-v"
  	exec hoaxy $ARGS
	;;

  *)
  	exec hoaxy $@
	;;
esac
