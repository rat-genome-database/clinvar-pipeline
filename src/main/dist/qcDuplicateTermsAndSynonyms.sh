# script to qc duplicate terms
APPDIR=/home/rgddata/pipelines/"clinvar-pipeline"
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
EMAIL_LIST=mtutaj@mcw.edu
if [ "$SERVER" == "REED" ]; then
  EMAIL_LIST="mtutaj@mcw.edu,slaulederkind@mcw.edu"
fi

$APPDIR/run.sh --qcDuplicateTermsAndSynonyms | tee $APPDIR/qcDuplicateTermsAndSynonyms.log

NO_DUPLICATES=`grep "^0 DUPLICATE SYNONYMS" $APPDIR/qcDuplicateTermsAndSynonyms.log | wc -l`
if [ NO_DUPLICATES = 1 ]; then
  echo "NO DUPLICATE SYNONYMS"
else
  mailx -s "[$SERVER] RDO DUPLICATE SYNONYMS" $EMAIL_LIST < $APPDIR/qcDuplicateTermsAndSynonyms.log
fi