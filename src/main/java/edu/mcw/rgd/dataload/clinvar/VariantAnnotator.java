package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.ontology.Annotation;
import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.process.CounterPool;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.Map;

/**
 * @author mtutaj
 * @since 3/20/14
 * utility class to generate annotations in full annot table for variants
 * <p>
 * only carpe-compliant variants are processed:
 *   (requirement removed in Oct 2014: carpe-compliant variant must have a position on GRCH37 assembly map)
 *   and must be one of following types: deletion, duplication, insertion, single nucleotide variant
 * <p>
 * per discussion with Stan:
 * - variant annotations have IAGP code
 * - IAGP human gene annotations, have WITH_INFO set to variant rgd id
 * - ISO rat/mouse annotations, have WITH_INFO set to orthologous human gene rgd id
 */
public class VariantAnnotator {

    Dao dao;
    Logger logDebug = LogManager.getLogger("dbg");
    Logger log = LogManager.getLogger("annotator");

    private String version;
    private int createdBy;
    private String evidence;
    private String dataSrc;
    private int refRgdId;

    private CounterPool counters;

    private int omimToRdoCount = 0;
    private int meshToRdoCount = 0;
    private int termNameToRdoCount = 0;
    private int termNameToHpoCount = 0;

    private Map<String,List<String>> mapConceptToOmim;
    private String conceptToOmimFile;
    private Map<String,Integer> unmatchableConditions = new HashMap<>();
    private Set<String> unmatchableDrugResponseTerms = new TreeSet<>();
    private TermNameMatcher rdoTermNameMatcher = null;
    private TermNameMatcher hpoTermNameMatcher = null;
    private Set<String> excludedConditionNames;
    private Set<String> excludedClinicalSignificance;
    private Set<String> processedVariantTypes;
    private boolean skipDrugResponseUnmatchableConditions;
    private String staleAnnotDeleteThreshold;

    public void run(Dao dao) throws Exception {

        long time0 = System.currentTimeMillis();

        this.dao = dao;
        counters = new CounterPool();

        log.info(getVersion());
        log.info(dao.getConnectionInfo());

        Date pipelineStartTime = Utils.addHoursToDate(new Date(), -1);
        logDebug.info("Starting...");

        loadConceptToOmimMap();
        logDebug.info("concept-to-omim map loaded");

        int origAnnotCount = dao.getCountOfAnnotationsByReference(getRefRgdId(), getDataSrc());
        log.info("initial annotation count: "+Utils.formatThousands(origAnnotCount));

        List<VariantInfo> variants = dao.getActiveVariants();
        logDebug.info("active variants loaded: "+Utils.formatThousands(variants.size()));

        variants.parallelStream().forEach( ge -> {
            counters.increment("INCOMING VARIANTS");

            try {
                logDebug.info("VAR_RGD_ID:" + ge.getRgdId() + " " + ge.getName());

                if (!variantIsCarpeCompliant(ge)) {
                    counters.increment("NOT CARPE-COMPLIANT VARIANTS");
                    return;
                }
                String pubMedIds = getPubMedIds(ge.getRgdId());

                // for all the associated genes, collect terms
                List<Integer> associatedGenes = dao.getAssociatedGenes(ge.getRgdId());

                generateDiseaseAnnotations(ge, associatedGenes, pubMedIds, dao);

                generatePhenotypeAnnotations(ge, associatedGenes, pubMedIds, dao);

            } catch( Exception e ) {
                throw new RuntimeException(e);
            }
        });

        dumpUnmatchableConditions();
        log.info("  drugResponseTermCount = "+Utils.formatThousands(unmatchableDrugResponseTerms.size()));
        log.info("  omimToRdoCount = "+Utils.formatThousands(omimToRdoCount)+",   meshToRdoCount = "+Utils.formatThousands(meshToRdoCount));
        log.info("  termNameToRdoCount = "+Utils.formatThousands(termNameToRdoCount));
        log.info("  termNameToHpoCount = "+Utils.formatThousands(termNameToHpoCount));

        // delete stale annotations
        int annotsDeleted = dao.deleteObsoleteAnnotations(getCreatedBy(), pipelineStartTime, getStaleAnnotDeleteThreshold(),
                getRefRgdId(), getDataSrc(), origAnnotCount, counters);
        counters.add("annotations - deleted", annotsDeleted);

        log.info(counters.dumpAlphabetically());

        log.info("STOP annot pipeline;   elapsed "+Utils.formatElapsedTime(System.currentTimeMillis(), time0));
    }

    void generateDiseaseAnnotations(VariantInfo ge, List<Integer> associatedGenes, String pubMedIds, Dao dao) throws Exception {

        Set<Term> diseaseTerms = new HashSet<>();
        for (Integer geneRgdId : associatedGenes) {
            diseaseTerms.addAll(getDiseaseTerms(ge.getRgdId(), geneRgdId, ge.getTraitName()));
        }

        // find matching RDO terms to make annotations
        for (Term term : diseaseTerms) {

            // create incoming annotation for variant
            Annotation annot = new Annotation();
            annot.setAnnotatedObjectRgdId(ge.getRgdId());
            annot.setTermAcc(term.getAccId());
            annot.setTerm(term.getTerm());
            annot.setObjectName(ge.getName());
            annot.setObjectSymbol(ge.getSymbol());
            annot.setRgdObjectKey(ge.getObjectKey());
            annot.setAspect("D"); // for RDO
            annot.setCreatedBy(getCreatedBy()); // ClinVar Annotation pipeline
            annot.setEvidence(getEvidence());
            annot.setSpeciesTypeKey(SpeciesType.HUMAN);
            annot.setDataSrc(getDataSrc());
            annot.setRefRgdId(getRefRgdId());
            annot.setXrefSource(pubMedIds);
            annot.setLastModifiedBy(annot.getCreatedBy());
            // term comment field contains the original OMIM acc id, or MESH acc id
            String matchByAccId = term.getComment() == null ? term.getAccId() : term.getComment();
            annot.setNotes("ClinVar Annotator: match by " + matchByAccId);

            // does incoming annotation match rgd?
            int inRgdAnnotKey = dao.getAnnotationKey(annot);
            if (inRgdAnnotKey != 0) {
                dao.updateLastModifiedDateForAnnotation(inRgdAnnotKey, getCreatedBy());
                counters.increment("RDO annotations - variant - matching");
            } else {
                dao.insertAnnotation(annot);
                counters.increment("RDO annotations - variant - inserted");
            }
        }

        generateGeneDiseaseAnnotations(ge.getRgdId(), associatedGenes, pubMedIds, ge.getTraitName());
    }

    void generatePhenotypeAnnotations(VariantInfo ge, List<Integer> associatedGenes, String pubMedIds, Dao dao) throws Exception {

        Set<Term> phenotypeTerms = new HashSet<>();
        for (Integer geneRgdId : associatedGenes) {
            phenotypeTerms.addAll(getPhenotypeTerms(ge.getRgdId(), geneRgdId, ge.getTraitName()));
        }

        // find matching HPO terms to make annotations
        for (Term term : phenotypeTerms) {

            // create incoming annotation for variant
            Annotation annot = new Annotation();
            annot.setAnnotatedObjectRgdId(ge.getRgdId());
            annot.setTermAcc(term.getAccId());
            annot.setTerm(term.getTerm());
            annot.setObjectName(ge.getName());
            annot.setObjectSymbol(ge.getSymbol());
            annot.setRgdObjectKey(ge.getObjectKey());
            annot.setAspect("H"); // for HPO
            annot.setCreatedBy(getCreatedBy()); // ClinVar Annotation pipeline
            annot.setEvidence(getEvidence());
            annot.setSpeciesTypeKey(SpeciesType.HUMAN);
            annot.setDataSrc(getDataSrc());
            annot.setRefRgdId(getRefRgdId());
            annot.setXrefSource(pubMedIds);
            annot.setLastModifiedBy(annot.getCreatedBy());
            // term comment field contains the original OMIM acc id, or MESH acc id
            String matchByAccId = term.getComment() == null ? term.getAccId() : term.getComment();
            annot.setNotes("ClinVar Annotator: match by " + matchByAccId);

            // does incoming annotation match rgd?
            int inRgdAnnotKey = dao.getAnnotationKey(annot);
            if (inRgdAnnotKey != 0) {
                dao.updateLastModifiedDateForAnnotation(inRgdAnnotKey, getCreatedBy());
                counters.increment("HPO annotations - variant - matching");
            } else {
                dao.insertAnnotation(annot);
                counters.increment("HPO annotations - variant - inserted");
            }
        }

        generateGenePhenotypeAnnotations(ge.getRgdId(), associatedGenes, pubMedIds, ge.getTraitName());
    }

    boolean variantIsCarpeCompliant(VariantInfo vi) throws Exception {

        // variant must be one of the following types:
        //deletion, duplication, insertion, single nucleotide variant
        if( !getProcessedVariantTypes().contains(vi.getObjectType()) ) {
            return false;
        }

        // clinical significance must be other than 'not provided'
        if( getExcludedClinicalSignificance().contains(vi.getClinicalSignificance()) ) {
            return false;
        }

        // exclude variants with clinical significance of 'uncertain significance'
        // coming from 'Leeds Institute of Molecular Medicine (LIMM)'
        if( Utils.stringsAreEqual(vi.getClinicalSignificance(), "uncertain significance")
            && Utils.stringsAreEqual(vi.getSubmitter(), "Leeds Institute of Molecular Medicine (LIMM)") ) {
            return false;
        }

        return true;
    }

    void generateGeneDiseaseAnnotations(int varRgdId, List<Integer> associatedGenes, String pubMedIds, String conditions) throws Exception {

        Annotation annot = new Annotation();
        annot.setAspect("D"); // for RDO
        annot.setCreatedBy(getCreatedBy()); // ClinVar Annotation pipeline
        annot.setEvidence("IAGP");
        annot.setSpeciesTypeKey(SpeciesType.HUMAN);
        annot.setDataSrc(getDataSrc());
        annot.setRefRgdId(getRefRgdId());
        annot.setXrefSource(pubMedIds);
        annot.setLastModifiedBy(annot.getCreatedBy());

        for( Integer geneRgdId: associatedGenes ) {

            for( Term term: getDiseaseTerms(varRgdId, geneRgdId, conditions) ) {
                Gene gene = dao.getGene(geneRgdId);
                String species = getSpeciesName(gene.getSpeciesTypeKey());

                Annotation humanGeneAnnot = (Annotation) annot.clone();
                humanGeneAnnot.setAnnotatedObjectRgdId(geneRgdId);
                humanGeneAnnot.setRgdObjectKey(RgdId.OBJECT_KEY_GENES);
                humanGeneAnnot.setObjectName(gene.getName());
                humanGeneAnnot.setObjectSymbol(gene.getSymbol());
                humanGeneAnnot.setWithInfo("RGD:"+varRgdId);
                humanGeneAnnot.setTermAcc(term.getAccId());
                humanGeneAnnot.setTerm(term.getTerm());
                humanGeneAnnot.setNotes("ClinVar Annotator: match by "+term.getComment());

                // does incoming annotation match rgd?
                int inRgdAnnotKey = dao.getAnnotationKey(humanGeneAnnot);
                if( inRgdAnnotKey!=0 ) {
                    dao.updateLastModifiedDateForAnnotation(inRgdAnnotKey, getCreatedBy());
                    counters.increment("RDO annotations - gene - "+species+" - matching");
                    counters.increment("RDO annotations - gene - ALL SPECIES - matching");
                } else {
                    dao.insertAnnotation(humanGeneAnnot);
                    counters.increment("RDO annotations - gene - "+species+" - inserted");
                    counters.increment("RDO annotations - gene - ALL SPECIES - inserted");
                }

                // create homologous rat/mouse annotations -- discontinued
                // we would rely on transitive-annotations-pipeline to create these
                if( false )
                for( Gene homolog: dao.getHomologs(gene.getRgdId()) ) {
                    if( homolog.getSpeciesTypeKey()!=SpeciesType.RAT && homolog.getSpeciesTypeKey()!=SpeciesType.MOUSE )
                        continue;
                    species = getSpeciesName(homolog.getSpeciesTypeKey());

                    Annotation homologAnnot = (Annotation) humanGeneAnnot.clone();
                    homologAnnot.setSpeciesTypeKey(homolog.getSpeciesTypeKey());
                    homologAnnot.setAnnotatedObjectRgdId(homolog.getRgdId());
                    homologAnnot.setObjectName(homolog.getName());
                    homologAnnot.setObjectSymbol(homolog.getSymbol());
                    homologAnnot.setWithInfo("RGD:"+gene.getRgdId());
                    homologAnnot.setEvidence("ISO");

                    int inRgdAnnotKey2 = dao.getAnnotationKey(homologAnnot);
                    if( inRgdAnnotKey2!=0 ) {
                        dao.updateLastModifiedDateForAnnotation(inRgdAnnotKey2, getCreatedBy());
                        counters.increment("RDO annotations - gene - "+species+" - matching");
                        counters.increment("RDO annotations - gene - ALL SPECIES - matching");
                    } else {
                        dao.insertAnnotation(homologAnnot);
                        counters.increment("RDO annotations - gene - "+species+" - inserted");
                        counters.increment("RDO annotations - gene - ALL SPECIES - inserted");
                    }
                }
            }
        }
    }

    void generateGenePhenotypeAnnotations(int varRgdId, List<Integer> associatedGenes, String pubMedIds, String conditions) throws Exception {

        Annotation annot = new Annotation();
        annot.setAspect("H"); // for HPO
        annot.setCreatedBy(getCreatedBy()); // ClinVar Annotation pipeline
        annot.setEvidence("IAGP");
        annot.setSpeciesTypeKey(SpeciesType.HUMAN);
        annot.setDataSrc(getDataSrc());
        annot.setRefRgdId(getRefRgdId());
        annot.setXrefSource(pubMedIds);
        annot.setLastModifiedBy(annot.getCreatedBy());

        for( Integer geneRgdId: associatedGenes ) {

            for( Term term: getPhenotypeTerms(varRgdId, geneRgdId, conditions) ) {
                Gene gene = dao.getGene(geneRgdId);
                String species = getSpeciesName(gene.getSpeciesTypeKey());

                Annotation humanGeneAnnot = (Annotation) annot.clone();
                humanGeneAnnot.setAnnotatedObjectRgdId(geneRgdId);
                humanGeneAnnot.setRgdObjectKey(RgdId.OBJECT_KEY_GENES);
                humanGeneAnnot.setObjectName(gene.getName());
                humanGeneAnnot.setObjectSymbol(gene.getSymbol());
                humanGeneAnnot.setWithInfo("RGD:"+varRgdId);
                humanGeneAnnot.setTermAcc(term.getAccId());
                humanGeneAnnot.setTerm(term.getTerm());
                humanGeneAnnot.setNotes("ClinVar Annotator: match by "+term.getComment());

                // does incoming annotation match rgd?
                int inRgdAnnotKey = dao.getAnnotationKey(humanGeneAnnot);
                if( inRgdAnnotKey!=0 ) {
                    dao.updateLastModifiedDateForAnnotation(inRgdAnnotKey, getCreatedBy());
                    counters.increment("HPO annotations - gene - "+species+" - matching");
                    counters.increment("HPO annotations - gene - ALL SPECIES - matching");
                } else {
                    dao.insertAnnotation(humanGeneAnnot);
                    counters.increment("HPO annotations - gene - "+species+" - inserted");
                    counters.increment("HPO annotations - gene - ALL SPECIES - inserted");
                }
            }
        }
    }

    Collection<Term> getDiseaseTerms(int varRgdId, int geneRgdId, String conditions) throws Exception {

        Set<Term> diseaseTerms = new HashSet<>();

        List<Integer> xdbKeys = new ArrayList<>(1);
        xdbKeys.add(54); // MedGen concept id

        for(XdbId xdbId: dao.getXdbIds(varRgdId, xdbKeys) ) {

            // for every MedGen id, find the matching RDO term
            String key = xdbId.getAccId()+"-"+geneRgdId;
            List<String> omimIds = mapConceptToOmim.get(key);
            if( omimIds!=null ) {
                for( String omimId: omimIds ) {

                    int oldCount = diseaseTerms.size();
                    diseaseTerms.addAll(dao.getRdoTermsBySynonym("OMIM:"+omimId));
                    checkIfRdoTerms(diseaseTerms);
                    int newCount = diseaseTerms.size();
                    omimToRdoCount += newCount - oldCount;
                }
                continue;
            }

            // try also exact match by condition name
            int oldCount = diseaseTerms.size();
            diseaseTerms.addAll(getDiseaseTermsByConditionName(conditions, varRgdId));
            int newCount = diseaseTerms.size();
            termNameToRdoCount += newCount - oldCount;
        }

        return diseaseTerms;
    }

    List<Term> getPhenotypeTermsByConditionName(String conditionString, int varRgdId) throws Exception {

        List<Term> results = new ArrayList<>();
        List<Alias> synonyms = null;
        List<String> aliases = new ArrayList<>();

        // there could be multiple conditions, like this:
        // Global developmental delay [RCV000052977]|See cases [RCV000052977]
        for( String condition: conditionString.split("\\|", -1) ) {
            // get rid of terminating RCV id: " [RCVxxxx"
            int cutOffPos = condition.lastIndexOf(" [RCV");
            if( cutOffPos>0 ) {
                condition = condition.substring(0, cutOffPos);
            }

            // filter out junk words, like 'not specified'
            if( getExcludedConditionNames().contains(condition) ) {
                logDebug.info("  excluded condition name: "+condition);
                continue;
            }

            List<Term> phenotypes = getPhenotypeTermsBySemiExactTermNameMatch(condition);
            if( !phenotypes.isEmpty() ) {
                checkIfHpoTerms(phenotypes);
                for( Term phenotype: phenotypes ) {
                    phenotype.setComment("term: "+condition);
                }
                results.addAll(phenotypes);
                continue;
            }
            aliases.add(condition);

            // exact match by disease name did not work
            // try variant synonyms
            if( synonyms==null )
                synonyms = dao.getAliases(varRgdId);
            int hitCount = 0;
            for( Alias syn: synonyms ) {
                phenotypes = getPhenotypeTermsBySemiExactTermNameMatch(syn.getValue());
                checkIfHpoTerms(phenotypes);
                for( Term phenotype: phenotypes ) {
                    phenotype.setComment("term: "+syn.getValue());
                }
                results.addAll(phenotypes);
                hitCount += phenotypes.size();
                aliases.add(syn.getValue());
            }
            if( hitCount>0 ) {
                continue;
            }

            // no match by exact match of condition synonyms to term name -- try synonyms
            hitCount = 0;
            for( String syn: aliases ) {
                phenotypes = getPhenotypeTermsBySemiExactTermNameMatch(syn);
                checkIfHpoTerms(phenotypes);
                for( Term phenotype: phenotypes ) {
                    phenotype.setComment("synonym: "+syn);
                }
                results.addAll(phenotypes);
                hitCount += phenotypes.size();
            }
            if( hitCount>0 ) {
                continue;
            }

            // no match by
            // addToUnmatchableConditions(condition);
        }

        return results;
    }

    Collection<Term> getPhenotypeTerms(int varRgdId, int geneRgdId, String conditions) throws Exception {

        Set<Term> phenotypeTerms = new HashSet<>();

        // try also exact match by condition name
        int oldCount = phenotypeTerms.size();
        phenotypeTerms.addAll(getPhenotypeTermsByConditionName(conditions, varRgdId));
        int newCount = phenotypeTerms.size();
        termNameToHpoCount += newCount - oldCount;

        return phenotypeTerms;
    }


    List<Term> getDiseaseTermsByConditionName(String conditionString, int varRgdId) throws Exception {

        List<Term> results = new ArrayList<>();
        List<Alias> synonyms = null;
        List<String> aliases = new ArrayList<>();

        // there could be multiple conditions, like this:
        // Global developmental delay [RCV000052977]|See cases [RCV000052977]
        for( String condition: conditionString.split("\\|", -1) ) {
            // get rid of terminating RCV id: " [RCVxxxx"
            int cutOffPos = condition.lastIndexOf(" [RCV");
            if( cutOffPos>0 ) {
                condition = condition.substring(0, cutOffPos);
            }

            // filter out junk words, like 'not specified'
            if( getExcludedConditionNames().contains(condition) ) {
                logDebug.info("  excluded condition name: "+condition);
                continue;
            }

            List<Term> diseases = getDiseaseTermsBySemiExactTermNameMatch(condition);
            if( !diseases.isEmpty() ) {
                checkIfRdoTerms(diseases);
                for( Term disease: diseases ) {
                    disease.setComment("term: "+condition);
                }
                results.addAll(diseases);
                continue;
            }
            aliases.add(condition);

            // exact match by disease name did not work
            // try variant synonyms
            if( synonyms==null )
                synonyms = dao.getAliases(varRgdId);
            int hitCount = 0;
            for( Alias syn: synonyms ) {
                diseases = getDiseaseTermsBySemiExactTermNameMatch(syn.getValue());
                checkIfRdoTerms(diseases);
                for( Term disease: diseases ) {
                    disease.setComment("term: "+syn.getValue());
                }
                results.addAll(diseases);
                hitCount += diseases.size();
                aliases.add(syn.getValue());
            }
            if( hitCount>0 )
                continue;

            // no match by exact match of condition synonyms to term name -- try synonyms
            hitCount = 0;
            for( String syn: aliases ) {
                diseases = getDiseaseTermsBySemiExactTermNameMatch(syn);
                checkIfRdoTerms(diseases);
                for( Term disease: diseases ) {
                    disease.setComment("synonym: "+syn);
                }
                results.addAll(diseases);
                hitCount += diseases.size();
            }
            if( hitCount>0 )
                continue;

            // no match by
            addToUnmatchableConditions(condition);
        }

        return results;
    }

    synchronized void addToUnmatchableConditions(String condition) {

        // to reduce nr of duplicate conditions (differing only by case), we convert everything to upper case
        condition = condition.toUpperCase();

        // keep track of drug response terms
        boolean isDrugResponseTerm = false;
        if( condition.contains("RESPONSE") ) {
            unmatchableDrugResponseTerms.add(condition);
            isDrugResponseTerm = true;
        }

        // screen out "drug response" unmatchable conditions, if applicable
        if( getSkipDrugResponseUnmatchableConditions() ) {
            if(isDrugResponseTerm) {
                return;
            }
        }

        // add incoming condition to unmatchable conditions collection
        Integer hits = unmatchableConditions.get(condition);
        if( hits==null )
            hits = 0;
        unmatchableConditions.put(condition, 1+hits);
    }

    void checkIfRdoTerms(Collection<Term> terms) throws Exception {
        for( Term term: terms ) {
            if( !term.getOntologyId().equals("RDO") ) {
                throw new Exception(term.getAccId()+" is NOT an RDO term!");
            }
        }
    }

    void checkIfHpoTerms(Collection<Term> terms) throws Exception {
        for( Term term: terms ) {
            if( !term.getOntologyId().equals("HP") ) {
                throw new Exception(term.getAccId()+" is NOT an HP term!");
            }
        }
    }

    String getPubMedIds(int rgdId) throws Exception {
        List<String> pubMedIds = new ArrayList<>();
        for( XdbId id: dao.getXdbIds(rgdId, XdbId.XDB_KEY_PUBMED) ) {
            // additional QC for PMID accessions: remove all non digit characters
            // (in the past, due to some malformed PMID ids, some of our web pages were broken)
            pubMedIds.add("PMID:"+id.getAccId().replaceAll("\\D", ""));
        }
        Collections.sort(pubMedIds);
        return Utils.concatenate(pubMedIds, "|");
    }

    void loadConceptToOmimMap() throws Exception {

        FileDownloader downloader = new FileDownloader();
        downloader.setExternalFile(getConceptToOmimFile());
        downloader.setLocalFile("data/concept_to_omim.txt.gz");
        downloader.setPrependDateStamp(true);
        downloader.setUseCompression(true);
        String localFileName = downloader.downloadNew();

        mapConceptToOmim = new HashMap<>();

        BufferedReader reader = Utils.openReader(localFileName);
        String line;
        while( (line=reader.readLine())!=null ) {
            if( line.startsWith("#") ) // skip comment lines
                continue;

            String[] cols = line.split("[\\t]", -1);
            //   0          1           2             3         4          5            6           7
            //#GeneID	GeneSymbol	ConceptID	DiseaseName	SourceName	SourceID	DiseaseMIM	LastUpdated
            //2	A2M	C0002395	Alzheimer's disease	SNOMED CT	26929004	104300	16 May 2011
            String conceptId = cols[2];
            String omimId = cols[6];
            String geneId = cols[0];
            List<Gene> genes = dao.getHumanGenesByGeneId(geneId);
            if( genes==null || genes.isEmpty() ) {
                log.warn("No gene in RGD [" + cols[1] + "] [" + cols[3]+"]");
                continue;
            }
            if( genes.size()>1 ) {
                log.warn("Multiple genes in RGD ["+cols[1]+"] ["+cols[3]+"]");
                continue;
            }
            int geneRgdId = genes.get(0).getRgdId();

            String key = conceptId+"-"+geneRgdId;
            List<String> omimIds = mapConceptToOmim.get(key);
            if( omimIds==null ) {
                omimIds = new ArrayList<>();
                mapConceptToOmim.put(key, omimIds);
            }
            if( !omimIds.contains(omimId) )
                omimIds.add(omimId);
        }
        reader.close();

        if( mapConceptToOmim.isEmpty() ) {
            String msg = "ERROR! concept-to-omim map is empty! aborting...";
            log.error(msg);
            throw new Exception(msg);
        }
        log.info("  concept-to-omim map loaded: "+mapConceptToOmim.size());
    }

    void dumpUnmatchableConditions() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter("data/unmatchable_conditions.txt"));

        String msg = "Unmatchable conditions: "+unmatchableConditions.size()+"\n";
        writer.write(msg);

        // build inverse map
        Map<Integer, Set<String>> imap = new TreeMap<>();
        for( Map.Entry<String,Integer> entry: unmatchableConditions.entrySet() ) {
            Set<String> names = imap.get(entry.getValue());
            if( names==null ) {
                names = new TreeSet<>();
                imap.put(entry.getValue(), names);
            }
            names.add(entry.getKey());
        }
        for( Map.Entry<Integer, Set<String>> entry: imap.entrySet() ) {
            msg = "  ["+entry.getKey()+"] \n";
            writer.write(msg);

            for( String name: entry.getValue() ) {
                msg = "    "+name+"\n";
                writer.write(msg);
            }
        }
        writer.close();
    }

    List<Term> getDiseaseTermsBySemiExactTermNameMatch(String termName) throws Exception {
        Set<String> termAccs = getTermAccIds("RDO", termName);
        if( termAccs==null )
            return Collections.emptyList();

        List<Term> terms = new ArrayList<>();
        for( String accId: termAccs ) {
            terms.add(dao.getTermByAccId(accId));
        }
        return terms;
    }

    List<Term> getPhenotypeTermsBySemiExactTermNameMatch(String termName) throws Exception {
        Set<String> termAccs = getTermAccIds("HP", termName);
        if( termAccs==null )
            return Collections.emptyList();

        List<Term> terms = new ArrayList<>();
        for( String accId: termAccs ) {
            terms.add(dao.getTermByAccId(accId));
        }
        return terms;
    }

    synchronized Set<String> getTermAccIds(String ontId, String termName) throws Exception {

        TermNameMatcher termNameMatcher = ontId.equals("RDO") ? rdoTermNameMatcher : hpoTermNameMatcher;
        if( termNameMatcher==null ) {
            termNameMatcher = new TermNameMatcher(ontId);
            termNameMatcher.indexTermsAndSynonyms(dao);

            if( ontId.equals("RDO") ) {
                rdoTermNameMatcher = termNameMatcher;
            } else {
                hpoTermNameMatcher = termNameMatcher;
            }
        }
        return termNameMatcher.getTermAccIds(termName);
    }

    synchronized String getSpeciesName(int speciesTypeKey) {
        return SpeciesType.getCommonName(speciesTypeKey).toLowerCase();
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setCreatedBy(int createdBy) {
        this.createdBy = createdBy;
    }

    public int getCreatedBy() {
        return createdBy;
    }

    public void setEvidence(String evidence) {
        this.evidence = evidence;
    }

    public String getEvidence() {
        return evidence;
    }

    public void setDataSrc(String dataSrc) {
        this.dataSrc = dataSrc;
    }

    public String getDataSrc() {
        return dataSrc;
    }

    public void setRefRgdId(int refRgdId) {
        this.refRgdId = refRgdId;
    }

    public int getRefRgdId() {
        return refRgdId;
    }

    public void setConceptToOmimFile(String conceptToOmimFile) {
        this.conceptToOmimFile = conceptToOmimFile;
    }

    public String getConceptToOmimFile() {
        return conceptToOmimFile;
    }

    public void setExcludedConditionNames(Set<String> excludedConditionNames) {
        this.excludedConditionNames = excludedConditionNames;
    }

    public Set<String> getExcludedConditionNames() {
        return excludedConditionNames;
    }

    public void setExcludedClinicalSignificance(Set<String> excludedClinicalSignificance) {
        this.excludedClinicalSignificance = excludedClinicalSignificance;
    }

    public Set<String> getExcludedClinicalSignificance() {
        return excludedClinicalSignificance;
    }

    public void setProcessedVariantTypes(Set<String> processedVariantTypes) {
        this.processedVariantTypes = processedVariantTypes;
    }

    public Set<String> getProcessedVariantTypes() {
        return processedVariantTypes;
    }

    public void setSkipDrugResponseUnmatchableConditions(boolean skipDrugResponseUnmatchableConditions) {
        this.skipDrugResponseUnmatchableConditions = skipDrugResponseUnmatchableConditions;
    }

    public boolean getSkipDrugResponseUnmatchableConditions() {
        return skipDrugResponseUnmatchableConditions;
    }

    public void setStaleAnnotDeleteThreshold(String staleAnnotDeleteThreshold) {
        this.staleAnnotDeleteThreshold = staleAnnotDeleteThreshold;
    }

    public String getStaleAnnotDeleteThreshold() {
        return staleAnnotDeleteThreshold;
    }
}
