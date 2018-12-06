# script to run the annotations pipeline for variants
#
APPDIR=/home/rgddata/pipelines/ClinVarPipeline
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu

$APPDIR/run.sh --annotate

mailx -s "[$SERVER] ClinVar Annot Pipeline OK" $EMAIL_LIST < $APPDIR/logs/annotator.log

if [ "$SERVER" == "REED" ]; then
    mailx -s "ClinVar unmatchable conditions" slaulederkind@mcw.edu,mtutaj@mcw.edu < $APPDIR/data/unmatchable_conditions.txt
    mailx -s "ClinVar duplicates" mtutaj@mcw.edu < $APPDIR/logs/duplicates.log
fi
