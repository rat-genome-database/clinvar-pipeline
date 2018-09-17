# script to run the annotations pipeline for variants
#
APPDIR=/home/rgddata/pipelines/ClinVarPipeline
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu

$APPDIR/run.sh --skipLoading --annotate | tee $APPDIR/annot.log

mailx -s "[$SERVER] ClinVar Annot Pipeline OK" $EMAIL_LIST < $APPDIR/annot.log

if [ "$SERVER" == "REED" ]; then
    mailx -s "ClinVar unmatchable conditions" slaulederkind@mcw.edu,mtutaj@mcw.edu < $APPDIR/data/unmatchable_conditions.txt
fi
