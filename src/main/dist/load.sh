# script to load ClinVar variants
APPDIR=/home/rgddata/pipelines/ClinVarPipeline
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu

$APPDIR/run.sh --load

mailx -s "[$SERVER] ClinVar Pipeline OK" $EMAIL_LIST < $APPDIR/logs/loader_daily.log
