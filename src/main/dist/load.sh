# script to load ClinVar variants
APPDIR=/home/rgddata/pipelines/ClinVarPipeline
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu

$APPDIR/run.sh --load | tee $APPDIR/load.log

mailx -s "[$SERVER] ClinVar Pipeline OK" $EMAIL_LIST < $APPDIR/load.log
