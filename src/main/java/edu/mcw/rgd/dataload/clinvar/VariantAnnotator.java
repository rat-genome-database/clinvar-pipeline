package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.ontology.Annotation;
import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.*;
import java.util.Map;
import java.util.zip.GZIPInputStream;

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

    Dao dao = new Dao();
    Log logDebug = LogFactory.getLog("dbg");

    private String version;
    private int createdBy;
    private String evidence;
    private String dataSrc;
    private int refRgdId;

    private int[] geneAnnotsMatching = new int[4];
    private int[] geneAnnotsInserted = new int[4];

    private int omimToRdoCount = 0;
    private int meshToRdoCount = 0;
    private int termNameToRdoCount = 0;

    private Map<String,List<String>> mapConceptToOmim;
    private String conceptToOmimFile;
    private Map<String,Integer> unmatchableConditions = new HashMap<>();
    private Set<String> unmatchableDrugResponseTerms = new TreeSet<>();
    private TermNameMatcher termNameMatcher = null;
    private Set<String> excludedConditionNames;
    private Set<String> excludedClinicalSignificance;
    private Set<String> processedVariantTypes;
    private boolean skipDrugResponseUnmatchableConditions;
    private String staleAnnotDeleteThreshold;

    public void run() throws Exception {

        Date pipelineStartTime = new Date();
        logDebug.info("Starting...");

        loadConceptToOmimMap();
        logDebug.info("concept-to-omim map loaded");

        int variantsProcessed = 0;
        int variantsNotCarpeCompliant = 0;
        int annotsMatching = 0;
        int annotsInserted = 0;

        List<VariantInfo> variants = dao.getActiveVariants();
        Collections.shuffle(variants);
        logDebug.info("active variants loaded: "+variants.size());

        for(VariantInfo ge: variants ) {
            variantsProcessed++;
            logDebug.info("VAR_RGD_ID:"+ge.getRgdId()+" "+ge.getName());

            if( !variantIsCarpeCompliant(ge) ) {
                variantsNotCarpeCompliant++;
                logDebug.info("  variant not carpe compliant");
                continue;
            }
            String pubMedIds = getPubMedIds(ge.getRgdId());

            // for all the associated genes, collect terms
            List<Integer> associatedGenes = dao.getAssociatedGenes(ge.getRgdId());
            Set<Term> diseaseTerms = new HashSet<>();
            for( Integer geneRgdId: associatedGenes ) {
                diseaseTerms.addAll(getDiseaseTerms(ge.getRgdId(), geneRgdId, ge.getTraitName()));
            }

            // find matching RDO terms to make annotations
            for( Term term: diseaseTerms ) {

                // create incoming annotation
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
                String matchByAccId = term.getComment()==null ? term.getAccId() : term.getComment();
                annot.setNotes("ClinVar Annotator: match by "+matchByAccId);

                // does incoming annotation match rgd?
                int inRgdAnnotKey = dao.getAnnotationKey(annot);
                if( inRgdAnnotKey!=0 ) {
                    dao.updateLastModifiedDateForAnnotation(inRgdAnnotKey, getCreatedBy());
                    annotsMatching++;
                } else {
                    dao.insertAnnotation(annot);
                    annotsInserted++;
                }
            }

            generateGeneAnnotations(ge.getRgdId(), associatedGenes, pubMedIds, ge.getTraitName());
        }

        dumpUnmatchableConditions();
        System.out.println("  drugResponseTermCount="+unmatchableDrugResponseTerms.size());

        System.out.println(variantsProcessed+"  omimToRdoCount="+omimToRdoCount+", meshToRdoCount="+meshToRdoCount);
        System.out.println("  termNameToRdoCount="+termNameToRdoCount);

        // delete stale annotations
        int annotsDeleted = dao.deleteObsoleteAnnotations(getCreatedBy(), pipelineStartTime, getStaleAnnotDeleteThreshold(),
                getRefRgdId(), getDataSrc());

        System.out.println();
        System.out.println("variants processed: "+variantsProcessed);
        System.out.println(" -out of which variants non carpe compliant: "+variantsNotCarpeCompliant);
        System.out.println();

        if( annotsMatching>0 )
            System.out.println("  matching variant annotations: "+annotsMatching);
        if( annotsInserted>0 )
            System.out.println("  inserted variant annotations: "+annotsInserted);
        if( annotsDeleted>0 )
            System.out.println("  deleted variant annotations : "+annotsDeleted);

        for( int i=1; i<=3; i++ ) {
            String species = SpeciesType.getCommonName(i).toLowerCase();
            if( geneAnnotsMatching[i]>0 )
                System.out.println("  matching "+species+" gene annotations: "+geneAnnotsMatching[i]);
            if( geneAnnotsInserted[i]>0 )
                System.out.println("  inserted "+species+" gene annotations: "+geneAnnotsInserted[i]);
        }

        System.out.println("STOP annot pipeline;   elapsed "+Utils.formatElapsedTime(System.currentTimeMillis(), pipelineStartTime.getTime()));
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

    void generateGeneAnnotations(int varRgdId, List<Integer> associatedGenes, String pubMedIds, String conditions) throws Exception {

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
                    geneAnnotsMatching[gene.getSpeciesTypeKey()]++;
                } else {
                    dao.insertAnnotation(humanGeneAnnot);
                    geneAnnotsInserted[gene.getSpeciesTypeKey()]++;
                }

                // create homologous rat/mouse annotations
                for( Gene homolog: dao.getHomologs(gene.getRgdId()) ) {
                    if( homolog.getSpeciesTypeKey()!=SpeciesType.RAT && homolog.getSpeciesTypeKey()!=SpeciesType.MOUSE )
                        continue;

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
                        geneAnnotsMatching[homolog.getSpeciesTypeKey()]++;
                    } else {
                        dao.insertAnnotation(homologAnnot);
                        geneAnnotsInserted[homolog.getSpeciesTypeKey()]++;
                    }
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

    void addToUnmatchableConditions(String condition) {

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
        String localFileName = downloader.download();

        mapConceptToOmim = new HashMap<>();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(localFileName))));
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
                System.out.println("No gene in RGD [" + cols[1] + "] [" + cols[3]+"]");
                continue;
            }
            if( genes.size()>1 ) {
                System.out.println("Multiple genes in RGD ["+cols[1]+"] ["+cols[3]+"]");
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

        System.out.println("  concept-to-omim map loaded: "+mapConceptToOmim.size());
    }

    void dumpUnmatchableConditions() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter("data/unmatchable_conditions.txt"));

        String msg = "Unmatchable conditions: "+unmatchableConditions.size()+"\n";
        System.out.print(msg);
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
            System.out.print(msg);
            writer.write(msg);

            for( String name: entry.getValue() ) {
                msg = "    "+name+"\n";
                System.out.print(msg);
                writer.write(msg);
            }
        }
        writer.close();
    }

    List<Term> getDiseaseTermsBySemiExactTermNameMatch(String termName) throws Exception {
        if( termNameMatcher==null ) {
            termNameMatcher = new TermNameMatcher();
            termNameMatcher.indexTermsAndSynonyms(dao);
        }
        Set<String> termAccs = termNameMatcher.getTermAccIds(termName);
        if( termAccs==null )
            return Collections.emptyList();

        List<Term> terms = new ArrayList<>();
        for( String accId: termAccs ) {
            terms.add(dao.getTermByAccId(accId));
        }
        return terms;
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
