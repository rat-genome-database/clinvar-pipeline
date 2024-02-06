package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.ontology.Annotation;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class AnnotCache {

    Logger log = LogManager.getLogger("annotator");
    Logger logUpdated = LogManager.getLogger("annotationsUpdated");

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
        for( Annotation a: incomingAnnots ) {
            String key = getMergeKey(a);
            Annotation mergedA = mergedAnnots.get(key);
            if( mergedA==null ) {
                mergedAnnots.put(key, a);
            } else {
                // merge XREF_SOURCE field
                Set<String> xrefs;
                if( a.getXrefSource()!=null ) {
                    xrefs = new TreeSet<>(Arrays.asList(a.getXrefSource().split("[\\|\\,\\;]")));
                } else {
                    xrefs = new TreeSet<>();
                }
                if( mergedA.getXrefSource()!=null ) {
                    xrefs.addAll(Arrays.asList(mergedA.getXrefSource().split("[\\|\\,\\;]")));
                }
                mergedA.setXrefSource(Utils.concatenate(xrefs,"|"));


                Set<String> notes;
                if( a.getNotes()!=null ) {
                    notes = new TreeSet<>(Arrays.asList(a.getNotes().split(" \\| ")));
                } else {
                    notes = new TreeSet<>();
                }
                if( mergedA.getNotes()!=null ) {
                    notes.addAll(Arrays.asList(mergedA.getNotes().split(" \\| ")));
                }
                mergedA.setNotes(Utils.concatenate(notes," | "));
            }
        }

        List<Annotation> mergedAnnotList = new ArrayList<>(mergedAnnots.values());

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
        for( Annotation a: annots ) {
            String key = getMergeKey2(a);
            Annotation mergedA = mergedAnnots.get(key);
            if( mergedA==null ) {
                mergedAnnots.put(key, a);
            } else {
                // merge WITH_INFO field
                Set<String> withInfos;
                if( a.getWithInfo()!=null ) {
                    withInfos = new TreeSet<>(Arrays.asList(a.getWithInfo().split("[\\|\\,\\;]")));
                } else {
                    withInfos = new TreeSet<>();
                }
                if( mergedA.getWithInfo()!=null ) {
                    withInfos.addAll(Arrays.asList(mergedA.getWithInfo().split("[\\|\\,\\;]")));
                }
                mergedA.setWithInfo(Utils.concatenate(withInfos,"|"));


                Set<String> notes;
                if( a.getNotes()!=null ) {
                    notes = new TreeSet<>(Arrays.asList(a.getNotes().split(" \\| ")));
                } else {
                    notes = new TreeSet<>();
                }
                if( mergedA.getNotes()!=null ) {
                    notes.addAll(Arrays.asList(mergedA.getNotes().split(" \\| ")));
                }
                mergedA.setNotes(Utils.concatenate(notes," | "));
            }
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
