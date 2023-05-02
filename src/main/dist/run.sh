#!/usr/bin/env bash
#
# ClinVar loading pipeline - wrapper script
#
. /etc/profile
APPNAME="clinvar-pipeline"

APPDIR=/home/rgddata/pipelines/$APPNAME
cd $APPDIR

java -Dspring.config=$APPDIR/../properties/default_db2.xml \
    -Dlog4j.configurationFile=file://$APPDIR/properties/log4j2.xml \
    -Xmx20g -jar lib/${APPNAME}.jar "$@" 2>&1 | tee run.log