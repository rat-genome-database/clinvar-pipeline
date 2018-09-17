# script to qc duplicate terms
APPDIR=/home/rgddata/pipelines/ClinVarPipeline
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu
if [ "$SERVER" == "REED" ]; then
  EMAIL_LIST="mtutaj@mcw.edu,slaulederkind@mcw.edu"
fi

$APPDIR/run.sh --qcDuplicateTerms | tee $APPDIR/qcDuplicateTerms.log

NO_DUPLICATES=`grep "^0 DUPLICATE TERMS" $APPDIR/qcDuplicateTerms.log | wc -l`
if [ NO_DUPLICATES = 1 ]; then
  echo "NO DUPLICATE TERMS"
else
  mailx -s "[$SERVER] RDO DUPLICATE TERMS" $EMAIL_LIST < $APPDIR/qcDuplicateTerms.log
fi