package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.ontology.Annotation;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class AnnotCache {

    Logger log = LogManager.getLogger("annotator");
    Logger logUpdated = LogManager.getLogger("annotationsUpdated");

    // compiled once: String.split() has no fast path for these, so calling it inline recompiled the
    // pattern on every merge -- tens of millions of times per run
    private static final Pattern FIELD_SEPARATOR = Pattern.compile("[\\|\\,\\;]");
    private static final Pattern NOTES_SEPARATOR = Pattern.compile(" \\| ");

    /** the pieces of a pipe/comma/semicolon separated field, sorted and deduplicated */
    private static Set<String> splitToSet( String value, Pattern separator ) {
        if( value==null ) {
            return new TreeSet<>();
        }
        return new TreeSet<>(Arrays.asList(separator.split(value)));
    }

    private AtomicInteger insertedAnnots = new AtomicInteger(0);
    // we store them in a map to avoid multiple updates
    private ConcurrentHashMap<Integer, Object> upToDateFullAnnotKeys = new ConcurrentHashMap<Integer, Object>();
    private ConcurrentHashMap<Integer, Object> updatedFullAnnotKeys = new ConcurrentHashMap<Integer, Object>();

    private List<Annotation> incomingAnnots = new ArrayList<>();

    synchronized public void addIncomingAnnot(Annotation a) {
        incomingAnnots.add(a);
    }

    void qcAndLoadAnnots(Dao dao) throws Exception {

        List<Annotation> mergedAnnots = mergeIncomingAnnots();

        // TODO
        //List<Annotation> uniqueAnnots = getAnnotsWithoutDuplicates(dao, mergedAnnots);
        //uniqueAnnots.parallelStream().forEach( a -> {

        mergedAnnots.parallelStream().forEach( a -> {

            try {
                int fullAnnotKey = dao.getAnnotationKey(a);
                if (fullAnnotKey == 0) {
                    dao.insertAnnotation(a);
                    insertedAnnots.incrementAndGet();
                } else {

                    // check if you need to update notes, annot ext
                    Annotation annotInRgd = dao.getAnnotation(fullAnnotKey);
                    boolean changed = !Utils.stringsAreEqual(annotInRgd.getNotes(), a.getNotes())
                            || !Utils.stringsAreEqual(annotInRgd.getAnnotationExtension(), a.getAnnotationExtension())
                            || !Utils.stringsAreEqual(annotInRgd.getGeneProductFormId(), a.getGeneProductFormId());

                    if( changed ) {
                        String msg = "KEY:" + fullAnnotKey + " " + a.getTermAcc() + " RGD:" + a.getAnnotatedObjectRgdId() + " RefRGD:" + a.getRefRgdId() + " " + a.getEvidence() + " W:" + a.getWithInfo();
                        if( !Utils.stringsAreEqual(annotInRgd.getAnnotationExtension(), a.getAnnotationExtension()) ) {
                            msg += "\n   ANNOT_EXT  OLD["+Utils.NVL(annotInRgd.getAnnotationExtension(),"")+"]  NEW["+a.getAnnotationExtension()+"]";
                        }
                        if( !Utils.stringsAreEqual(annotInRgd.getGeneProductFormId(), a.getGeneProductFormId()) ) {
                            msg += "\n   GENE_FORM  OLD["+Utils.NVL(annotInRgd.getGeneProductFormId(),"")+"]  NEW["+a.getGeneProductFormId()+"]";
                        }
                        if( !Utils.stringsAreEqual(annotInRgd.getNotes(), a.getNotes()) ) {
                            msg += "\n   NOTES  OLD["+Utils.NVL(annotInRgd.getNotes(),"")+"]  NEW["+a.getNotes()+"]";
                        }
                        logUpdated.debug(msg);

                        a.setKey(fullAnnotKey);
                        dao.updateAnnotation(a);
                        updatedFullAnnotKeys.put(fullAnnotKey, 0);
                    } else {
                        upToDateFullAnnotKeys.put(fullAnnotKey, 0);
                    }
                }
            } catch(Exception e) {
                log.warn("PROBLEMATIC ANNOT=  "+a.dump("|"));
                throw new RuntimeException(e);
            }
        });

    }

    List<Annotation> mergeIncomingAnnots() throws CloneNotSupportedException {

        // merge XREF_SOURCE
        List<Annotation> annots = mergeIncomingAnnots1();
        // merge WITH_INFO
        return mergeIncomingAnnots2(annots);
    }

    /**
     * Incoming annots built on the base of human ClinVar annots are often quite similar, differing only in XREF_SOURCE field.
     * Per RGD strategy, we can safely merge these annots into a single one, with its XREF_SOURCE field being an aggregate of XREF_SOURCE field
     * from source annotations. Also NOTES field is being merged as well.
     */
    List<Annotation> mergeIncomingAnnots1() throws CloneNotSupportedException {

        log.info("   incoming annot count1 = "+Utils.formatThousands(incomingAnnots.size()));

        Map<String, Annotation> mergedAnnots = new HashMap<>();
        // pieces gathered per merge key; an entry appears only once a key has been seen twice, so a
        // key seen once keeps its original XREF_SOURCE and NOTES untouched, exactly as before
        Map<String, Set<String>> xrefsByKey = new HashMap<>();
        Map<String, Set<String>> notesByKey = new HashMap<>();

        for( Annotation a: incomingAnnots ) {
            String key = getMergeKey(a);
            Annotation mergedA = mergedAnnots.get(key);
            if( mergedA==null ) {
                mergedAnnots.put(key, a);
            } else {
                // merge XREF_SOURCE field
                Set<String> xrefs = xrefsByKey.get(key);
                if( xrefs==null ) {
                    xrefs = splitToSet(mergedA.getXrefSource(), FIELD_SEPARATOR);
                    xrefsByKey.put(key, xrefs);
                }
                xrefs.addAll(splitToSet(a.getXrefSource(), FIELD_SEPARATOR));

                Set<String> notes = notesByKey.get(key);
                if( notes==null ) {
                    notes = splitToSet(mergedA.getNotes(), NOTES_SEPARATOR);
                    notesByKey.put(key, notes);
                }
                notes.addAll(splitToSet(a.getNotes(), NOTES_SEPARATOR));
            }
        }

        // the merged fields are built once per key rather than rebuilt on every merge
        for( Map.Entry<String, Set<String>> entry: xrefsByKey.entrySet() ) {
            mergedAnnots.get(entry.getKey()).setXrefSource(Utils.concatenate(entry.getValue(), "|"));
        }
        for( Map.Entry<String, Set<String>> entry: notesByKey.entrySet() ) {
            mergedAnnots.get(entry.getKey()).setNotes(Utils.concatenate(entry.getValue(), " | "));
        }

        List<Annotation> mergedAnnotList = new ArrayList<>(mergedAnnots.values());

        // the annotations that lost the merge are dead now; releasing them here keeps them from
        // being held for the whole of merge-2 and the database sync
        incomingAnnots.clear();

        splitAnnots(mergedAnnotList);
        log.info("   merged annot count (XREF_SOURCE) = "+Utils.formatThousands(mergedAnnotList.size()));
        return mergedAnnotList;
    }

    void splitAnnots(List<Annotation> annots) throws CloneNotSupportedException {

        // XREF_SOURCE field cannot be longer than 4000 chars; if it is longer, it must be split into multiple annotations
        List<Annotation> annotSplits = new ArrayList<>();

        for( Annotation a: annots ) {
            if( a.getXrefSource()==null ) {
                continue;
            }

            while( a.getXrefSource().length()>4000 ) {
                int splitPos = a.getXrefSource().lastIndexOf("|", 4000);
                String goodXrefSrc = a.getXrefSource().substring(0, splitPos);
                Annotation a2 = (Annotation) a.clone();
                a2.setXrefSource(goodXrefSrc);
                annotSplits.add(a2);
                a.setXrefSource(a.getXrefSource().substring(splitPos+1));

                if(false) { // dbg
                    log.warn("===");
                    log.warn("SPLIT1 " + a2.dump("|"));
                    log.warn("SPLIT2 " + a.dump("|"));
                    log.warn("===");
                }
            }
        }

        if( !annotSplits.isEmpty() ) {
            log.info("   merged annot splits by XREF_SOURCE = "+Utils.formatThousands(annotSplits.size()));
            annots.addAll(annotSplits);
        }
    }

    List<Annotation> mergeIncomingAnnots2( List<Annotation> annots ) throws CloneNotSupportedException {

        log.info("   incoming annot count2 = "+Utils.formatThousands(annots.size()));

        Map<String, Annotation> mergedAnnots = new HashMap<>();
        Map<String, Set<String>> withInfosByKey = new HashMap<>();
        Map<String, Set<String>> notesByKey = new HashMap<>();

        for( Annotation a: annots ) {
            String key = getMergeKey2(a);
            Annotation mergedA = mergedAnnots.get(key);
            if( mergedA==null ) {
                mergedAnnots.put(key, a);
            } else {
                // merge WITH_INFO field
                Set<String> withInfos = withInfosByKey.get(key);
                if( withInfos==null ) {
                    withInfos = splitToSet(mergedA.getWithInfo(), FIELD_SEPARATOR);
                    withInfosByKey.put(key, withInfos);
                }
                withInfos.addAll(splitToSet(a.getWithInfo(), FIELD_SEPARATOR));

                Set<String> notes = notesByKey.get(key);
                if( notes==null ) {
                    notes = splitToSet(mergedA.getNotes(), NOTES_SEPARATOR);
                    notesByKey.put(key, notes);
                }
                notes.addAll(splitToSet(a.getNotes(), NOTES_SEPARATOR));
            }
        }

        for( Map.Entry<String, Set<String>> entry: withInfosByKey.entrySet() ) {
            mergedAnnots.get(entry.getKey()).setWithInfo(Utils.concatenate(entry.getValue(), "|"));
        }
        for( Map.Entry<String, Set<String>> entry: notesByKey.entrySet() ) {
            mergedAnnots.get(entry.getKey()).setNotes(Utils.concatenate(entry.getValue(), " | "));
        }

        List<Annotation> mergedAnnotList = new ArrayList<>(mergedAnnots.values());

        splitAnnots2(mergedAnnotList);
        log.info("   merged annot count (WITH_INFO) = "+Utils.formatThousands(mergedAnnotList.size()));
        return mergedAnnotList;
    }

    void splitAnnots2(List<Annotation> annots) throws CloneNotSupportedException {

        // WITH_INFO field cannot be longer than 1700 chars; if it is longer, it must be split into multiple annotations
        List<Annotation> annotSplits = new ArrayList<>();

        for( Annotation a: annots ) {
            if( a.getWithInfo()==null ) {
                continue;
            }

            while( a.getWithInfo().length()>1700 ) {
                int splitPos = a.getWithInfo().lastIndexOf("|", 1700);
                String goodWithInfo = a.getWithInfo().substring(0, splitPos);
                Annotation a2 = (Annotation) a.clone();
                a2.setWithInfo(goodWithInfo);
                annotSplits.add(a2);
                a.setWithInfo(a.getWithInfo().substring(splitPos+1));

                if(false) { // dbg
                    log.warn("===");
                    log.warn("SPLIT1 " + a2.dump("|"));
                    log.warn("SPLIT2 " + a.dump("|"));
                    log.warn("===");
                }
            }
        }

        if( !annotSplits.isEmpty() ) {
            log.info("   merged annot splits by WITH_INFO = "+Utils.formatThousands(annotSplits.size()));
            annots.addAll(annotSplits);
        }
    }

    String getMergeKey(Annotation a) {
        return a.getAnnotatedObjectRgdId()+"|"+a.getTermAcc()+"|"+a.getDataSrc()+"|"+a.getEvidence()
                +"|"+a.getRefRgdId()+"|"+a.getCreatedBy()+"|"+Utils.defaultString(a.getQualifier())
                +"|"+a.getWithInfo()
                +"|"+Utils.defaultString(a.getAnnotationExtension())+"|"+Utils.defaultString(a.getQualifier());
    }

    String getMergeKey2(Annotation a) {
        return a.getAnnotatedObjectRgdId()+"|"+a.getTermAcc()+"|"+a.getDataSrc()+"|"+a.getEvidence()
                +"|"+a.getRefRgdId()+"|"+a.getCreatedBy()+"|"+Utils.defaultString(a.getQualifier())
                +"|"+a.getXrefSource()
                +"|"+Utils.defaultString(a.getAnnotationExtension())+"|"+Utils.defaultString(a.getQualifier());
    }

    public void clear() {
        insertedAnnots.set(0);
        upToDateFullAnnotKeys.clear();
        updatedFullAnnotKeys.clear();
        incomingAnnots.clear();
    }

    public void syncWithDb( Dao dao, String category ) throws Exception {
        // qc incoming annots to determine annots for insertion / deletion
        qcAndLoadAnnots(dao);

        int count = insertedAnnots.get();
        if (count != 0) {
            log.info(category + " annotations inserted: " + Utils.formatThousands(count));
        }

        count = updatedFullAnnotKeys.size();
        if (count != 0) {
            log.info(category + " annotations updated: " + Utils.formatThousands(count));
        }

        // update last modified date for matching annots in batches
        updateLastModified(dao);
    }

    int updateLastModified( Dao dao ) throws Exception {

        int rowsUpdated = 0;

        // do the updates in batches of 999, because Oracle has an internal limit of 1000
        List<Integer> fullAnnotKeys = new ArrayList<>(upToDateFullAnnotKeys.keySet());
        for( int i=0; i<fullAnnotKeys.size(); i+= 999 ) {
            int j = i + 999;
            if( j > fullAnnotKeys.size() ) {
                j = fullAnnotKeys.size();
            }
            List<Integer> fullAnnotKeysSubset = fullAnnotKeys.subList(i, j);
            rowsUpdated += dao.updateLastModified(fullAnnotKeysSubset);
        }

        return rowsUpdated;
    }
}
