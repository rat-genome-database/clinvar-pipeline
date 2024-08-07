# script to load ClinVar variants
APPDIR=/home/rgddata/pipelines/"clinvar-pipeline"
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu,llamers@mcw.edu

$APPDIR/run.sh --addRsIds

mailx -s "[$SERVER] ClinVar Pipeline OK" $EMAIL_LIST < $APPDIR/logs/rsSummary.log
