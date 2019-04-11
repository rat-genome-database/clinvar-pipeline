# clinvar-pipeline
Loads variants from ClinVar into RGD database. Creates disease annotations to variants and also for genes, based on orthology.

LOGIC NOTES

1. parser and obsolete SO acc ids

   parser determines a SO acc id for every incoming variant;
   occasionally this SO acc id points to an obsolete SO term (we cannot assign obsolete SO acc ids to variants);
   for those obsolete SO acc ids the code is looking for substitute SO terms:
     1) code is looking for 'replaced_by' field of the obsolete SO term
     2) code is looking for any term which lists obsolete SO acc id as 'alt_id'

   VARIANTS_WITH_SUBSTITUTE_SO_ACC_ID counter is incremented for every one variant
     having obsolete SO acc id successfully substituted with alternate SO acc id

   note: this feature was implemented on Aug 1, 2016, ClinVar pipeline ver 1.1.24, per RGDD-1248

2. QC clinical significance

   the code is detecting multiple clinical significance values:
     'benign/likely benign'
     'pathogenic, likely pathogenic'

   and is constructing the final clinical significance value by merging it with the value in the database, f.e.
     'pathogenic|likely pathogenic|likely benign|benign'

3. variant notes

    since variant notes are stored in VARCHAR2(4000) column in the database, they are truncated to 4000 characters

ANNOTATIONS

  primary annotations, to human variants and associated human genes, are made with 'IAGP' evidence code
  homologous annotations to rat and mouse genes are made with 'ISO' evidence code
