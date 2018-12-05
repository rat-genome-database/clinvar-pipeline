#!/usr/bin/env bash
#
# ClinVar loading pipeline - wrapper script
#
. /etc/profile
APPNAME=ClinVarPipeline

APPDIR=/home/rgddata/pipelines/$APPNAME
cd $APPDIR

java -Dspring.config=$APPDIR/../properties/default_db.xml \
    -Dlog4j.configuration=file://$APPDIR/properties/log4j.properties \
    -jar lib/${APPNAME}.jar "$@" 2>&1 | tee run.log