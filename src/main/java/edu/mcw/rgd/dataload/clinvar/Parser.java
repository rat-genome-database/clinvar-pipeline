package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.xml.XomAnalyzer;
import nu.xom.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author mtutaj
 * @since 2/14/14
 * xml parser
 */
public class Parser extends XomAnalyzer {

    static final SimpleDateFormat SDT_YYYYMMDD = new SimpleDateFormat("yyyy-MM-dd");

    Logger log = LogManager.getLogger("loader");
    Logger logDebug = LogManager.getLogger("dbg");

    public QC qc;
    public Loader loader;
    private Record rec;


    private static boolean terminationRequested = false;

    synchronized public void requestTermination() {
        terminationRequested = true;
    }

    synchronized public boolean isTerminationRequested() {
        return terminationRequested;
    }

    public void initRecord(String name) {
        if( isTerminationRequested() ) {
            throw new RuntimeException(new InterruptedException("xml parsing termination requested"));
        }
    }

    private void createRecord(String varID) {

        rec = new Record();

        VariantInfo var = new VariantInfo();
        var.setSource(Manager.SOURCE);
        var.setObjectKey(RgdId.OBJECT_KEY_VARIANTS);
        var.setObjectStatus("ACTIVE");
        var.setSpeciesTypeKey(SpeciesType.HUMAN);
        var.setSymbol("CV"+varID);
        rec.setVarIncoming(var);
    }

    public Element parseSubrecord(Element element) {
        try {
            String elName = element.getLocalName();
            if( elName.equals("ClassifiedRecord") ) {

                Elements simpleAlleles = element.getChildElements("SimpleAllele");
                if( simpleAlleles.size()>1 ) {
                    GlobalCounters.getInstance().incrementCounter("MULTI_ALLELE_VARIANTS_SKIPPED", 1);
                    String variationId = simpleAlleles.get(0).getAttributeValue("VariationID");
                    logDebug.info("MULTI_ALLELE_VARIANTS_SKIPPED for "+variationId);
                    this.rec = null;
                    return null;
                }
                if( simpleAlleles.size()==0 ) {
                    int genotypeBlock = element.getChildElements("Genotype").size();
                    if( genotypeBlock!=0 ) {
                        Element genotype = element.getFirstChildElement("Genotype");
                        int simpleAlleleCount = genotype.getChildElements("SimpleAllele").size();
                        String variationId = genotype.getAttributeValue("VariationID");
                        log.info("GENOTYPE_VARIANTS_SKIPPED for VariationID="+variationId+", allele_count="+simpleAlleleCount);
                        GlobalCounters.getInstance().incrementCounter("GENOTYPE_VARIANTS_SKIPPED", 1);
                        this.rec = null;
                        return null;
                    }
                    int haplotypeBlock = element.getChildElements("Haplotype").size();
                    if( haplotypeBlock!=0 ) {
                        Element haplotype = element.getFirstChildElement("Haplotype");
                        int simpleAlleleCount = haplotype.getChildElements("SimpleAllele").size();
                        String variationId = haplotype.getAttributeValue("VariationID");
                        log.info("HAPLOTYPE_VARIANTS_SKIPPED for VariationID="+variationId+", allele_count="+simpleAlleleCount);
                        GlobalCounters.getInstance().incrementCounter("HAPLOTYPE_VARIANTS_SKIPPED", 1);
                        this.rec = null;
                        return null;
                    }
                    log.warn("ERROR! NO SimpleAllele element under ClassifiedRecord");
                    this.rec = null;
                    return null;
                }
                Element simpleAllele = simpleAlleles.get(0);
                // legacy DB symbol is "CV"+AlleleID (= old <Measure>/@ID), not VariationID
                String alleleId = simpleAllele.getAttributeValue("AlleleID");
                String variationId = simpleAllele.getAttributeValue("VariationID");
                createRecord(alleleId);

                // legacy clinVarId (for xdb_ids.notes, aliases.notes, traitName suffix) was a single
                // RCV accession; in VCV pick the first RCVAccession to preserve that semantic
                String primaryRcv = "";
                {
                    Element rcvList0 = element.getFirstChildElement("RCVList");
                    if (rcvList0 != null) {
                        Element firstRcvAcc = rcvList0.getFirstChildElement("RCVAccession");
                        if (firstRcvAcc != null) {
                            primaryRcv = Utils.defaultString(firstRcvAcc.getAttributeValue("Accession"));
                        }
                    }
                }
                final String clinVarId = primaryRcv;
                if( !clinVarId.isEmpty() ) {
                    rec.getXdbIds().addIncomingXdbId(52, clinVarId, clinVarId);
                }

                Element nameEl = simpleAllele.getFirstChildElement("Name");
                if( nameEl != null ) {
                    rec.getVarIncoming().setName( nameEl.getValue() );
                } else {
                    log.warn("missing SimpleAllele/Name for VariationID="+variationId);
                }

                Element variantTypeEl = simpleAllele.getFirstChildElement("VariantType");
                if( variantTypeEl != null ) {
                    rec.getVarIncoming().setObjectType( variantTypeEl.getValue().toLowerCase() );
                }

                Element otherNameList = simpleAllele.getFirstChildElement("OtherNameList");
                if( otherNameList != null ) {
                    Element altName = otherNameList.getFirstChildElement("Name");
                    if( altName != null ) {
                        rec.setVariantAltName( altName.getValue() );
                    }
                }

                Element geneList = simpleAllele.getFirstChildElement("GeneList");
                if( geneList!=null ) {
                    Elements genes = geneList.getChildElements();
                    for (int i = 0; i < genes.size(); i++) {
                        Element gene = genes.get(i);
                        String geneSymbol = gene.getAttributeValue("Symbol");
                        String geneId = gene.getAttributeValue("GeneID");
                        String hgncId = gene.getAttributeValue("HGNC_ID");
                        rec.getGeneAssociations().add(geneId, geneSymbol);
                        rec.getXdbIds().addIncomingXdbId(XdbId.XDB_KEY_NCBI_GENE, geneId, clinVarId);
                        rec.getXdbIds().addIncomingXdbId(XdbId.XDB_KEY_HGNC, hgncId, clinVarId);
                    }
                }

                Element location = simpleAllele.getFirstChildElement("Location");
                if( location!=null ) {
                    Elements elements = location.getChildElements("CytogeneticLocation");
                    for (int i = 0; i < elements.size(); i++) {
                        Element cyto = elements.get(i);
                        rec.getMapPositions().addCytoPos(cyto.getValue());
                    }
                    elements = location.getChildElements("SequenceLocation");
                    for (int i = 0; i < elements.size(); i++) {
                        Element el = elements.get(i);
                        String assembly = el.getAttributeValue("Assembly");
                        String accession = el.getAttributeValue("Accession");
                        String chr = el.getAttributeValue("Chr");
                        String startPos = el.getAttributeValue("start");
                        String stopPos = el.getAttributeValue("stop");
                        String strand = null;
                        rec.getMapPositions().addPos(assembly, chr, accession, startPos, stopPos, strand);

                        String refNuc = el.getAttributeValue("referenceAlleleVCF");
                        if (refNuc != null && refNuc.length() > 0) {
                            rec.getVarIncoming().setRefNuc(refNuc);
                        }

                        String varNuc = el.getAttributeValue("alternateAlleleVCF");
                        if (varNuc != null && varNuc.length() > 0) {
                            rec.getVarIncoming().setVarNuc(varNuc);
                        }
                    }
                }

                String soAccIdFromXml = "";
                Element hgvsList = simpleAllele.getFirstChildElement("HGVSlist");
                if( hgvsList!=null ) {
                    Elements hgvss = hgvsList.getChildElements();
                    for( int i=0; i< hgvss.size(); i++ ) {
                        Element hgvs = hgvss.get(i);
                        String hgvsTypeAttr = hgvs.getAttributeValue("Type");
                        if( hgvsTypeAttr == null ) continue;
                        String hgvsType = hgvsTypeAttr.replace(", ", "_").replace(" ","").toLowerCase().replace("hgvs_","");

                        Element e1 = hgvs.getFirstChildElement("NucleotideExpression");
                        if( e1!=null ) {
                            Element exp = e1.getFirstChildElement("Expression");
                            if( exp != null ) {
                                String val = exp.getValue();
                                if( !Utils.isStringEmpty(val) ) {
                                    rec.getHgvsNames().addIncomingHgvsName(hgvsType, val);
                                    addRefSeqXref(hgvsType, val, clinVarId);
                                }
                            }
                        }
                        Element e2 = hgvs.getFirstChildElement("ProteinExpression");
                        if( e2!=null ) {
                            Element exp = e2.getFirstChildElement("Expression");
                            if( exp != null ) {
                                String val = exp.getValue();
                                if( !Utils.isStringEmpty(val) ) {
                                    rec.getHgvsNames().addIncomingHgvsName(hgvsType, val);
                                    addRefSeqXref(hgvsType, val, clinVarId);
                                }
                            }
                        }
                        Element e3 = hgvs.getFirstChildElement("MolecularConsequence");
                        if( e3!=null ) {
                            String consequence = e3.getAttributeValue("Type");
                            if( !Utils.isStringEmpty(consequence) ) {
                                rec.getVarIncoming().setMolecularConsequence(consequence);
                            }
                            // VCV exposes the SO acc id directly (e.g. "SO:0001574"); first one wins
                            String soAcc = e3.getAttributeValue("ID");
                            if( soAccIdFromXml.isEmpty() && !Utils.isStringEmpty(soAcc) && soAcc.startsWith("SO:") ) {
                                soAccIdFromXml = soAcc;
                            }
                        }
                    }
                }
                handleSoAccId(soAccIdFromXml);

                Element xrefList = simpleAllele.getFirstChildElement("XRefList");
                if( xrefList!=null ) {
                    parseXRefs(xrefList, clinVarId);
                }

                String classifiedCondition = null;
                Elements rcvLists = element.getChildElements( "RCVList" );
                for( int z=0; z<rcvLists.size(); z++ ) {
                    Element rcvList = rcvLists.get(z);
                    Elements rcvAcc = rcvList.getChildElements("RCVAccession");
                    for( int i=0; i<rcvAcc.size(); i++ ) {
                        Element el = rcvAcc.get(i);
                        String rcv = el.getAttributeValue("Accession");
                        rec.getXdbIds().addIncomingXdbId(52, rcv, rcv);

                        // RCV's condition list can be ClassifiedConditionList, OncogenicityConditionList,
                        // or SomaticClinicalImpactConditionList -- accept any *ConditionList child
                        Elements rcvKids = el.getChildElements();
                        for( int k=0; k<rcvKids.size(); k++ ) {
                            Element kid = rcvKids.get(k);
                            if( !kid.getLocalName().endsWith("ConditionList") ) continue;
                            Elements conds = kid.getChildElements();
                            for( int f=0; f<conds.size(); f++ ) {
                                if( classifiedCondition==null ) {
                                    classifiedCondition = conds.get(f).getValue();
                                }
                            }
                        }
                    }
                }

                // count every Classification subtype seen at the aggregate level (so unhandled
                // types surface in the run summary), and harvest condition-trait xrefs from each
                Element classificationsEl = element.getFirstChildElement("Classifications");
                if( classificationsEl != null ) {
                    Elements classifications = classificationsEl.getChildElements();
                    for( int i=0; i<classifications.size(); i++ ) {
                        Element classification = classifications.get(i);
                        GlobalCounters.getInstance().incrementCounter("CLASSIFICATION_AGGREGATE_"+classification.getLocalName(), 1);

                        Element conditionList = classification.getFirstChildElement("ConditionList");
                        if( conditionList == null ) continue;
                        Elements traitSets = conditionList.getChildElements("TraitSet");
                        for( int ts=0; ts<traitSets.size(); ts++ ) {
                            Elements traits = traitSets.get(ts).getChildElements("Trait");
                            for( int tr=0; tr<traits.size(); tr++ ) {
                                parseXRefs(traits.get(tr), clinVarId);
                            }
                        }
                    }
                }

                Elements clinicalAssertionLists = element.getChildElements("ClinicalAssertionList");
                for( int i=0; i<clinicalAssertionLists.size(); i++ ) {
                    Element clinicalAssertionList = clinicalAssertionLists.get(i);
                    Elements clinicalAssertions = clinicalAssertionList.getChildElements();
                    for( int j=0; j<clinicalAssertions.size(); j++ ) {
                        Element clinicalAssertion = clinicalAssertions.get(j);
                        if( !clinicalAssertion.getLocalName().equals("ClinicalAssertion")) {
                            log.warn("unexpected ClinicalAssertionList child: "+clinicalAssertion.getLocalName());
                        }
                        Elements elements = clinicalAssertion.getChildElements();
                        for( int y=0; y<elements.size(); y++ ) {
                            Element el = elements.get(y);
                            switch( el.getLocalName() ) {
                                case "ClinVarSubmissionID" -> {
                                    // nothing to do
                                }
                                case "ClinVarAccession" -> {
                                    String submitter = el.getAttributeValue("SubmitterName");
                                    String orgAbbreviation = el.getAttributeValue("OrgAbbreviation");
                                    rec.mergeSubmitterForVarIncoming(orgAbbreviation);
                                    rec.mergeSubmitterForVarIncoming(submitter);
                                }
                                case "AdditionalSubmitters" -> {
                                    Elements submitters = el.getChildElements("SubmitterDescription");
                                    if( submitters!=null) {
                                        for( int s=0; s<submitters.size(); s++ ) {
                                            Element submitterEl = submitters.get(s);
                                            String submitterName = submitterEl.getAttributeValue("SubmitterName");
                                            rec.mergeSubmitterForVarIncoming(submitterName);
                                        }
                                    }
                                }
                                case "RecordStatus" -> {
                                    // nothing to do
                                }
                                case "Classification" -> {
                                    String dateLastEvaluated = el.getAttributeValue("DateLastEvaluated");
                                    if( dateLastEvaluated!=null && !dateLastEvaluated.isEmpty()) {
                                        Date dt;
                                        synchronized (SDT_YYYYMMDD) { // Date.parse() must be synchronized among all threads, to avoid data corruption
                                            dt = SDT_YYYYMMDD.parse(dateLastEvaluated);
                                        }
                                        rec.getVarIncoming().setDateLastEvaluated(dt);
                                    }

                                    Elements elements2 = el.getChildElements();
                                    for (int u = 0; u < elements2.size(); u++) {
                                        Element el2 = elements2.get(u);
                                        String subName = el2.getLocalName();
                                        switch (subName) {
                                            case "ReviewStatus" -> {
                                                String reviewStatus = el2.getValue().toLowerCase();
                                                rec.mergeReviewStatusForVarIncoming(reviewStatus);
                                            }
                                            case "GermlineClassification", "NoClassification", "OncogenicityClassification" -> {
                                                GlobalCounters.getInstance().incrementCounter("CLASSIFICATION_PER_SUBMISSION_"+subName, 1);
                                                String value = el2.getValue().toLowerCase();
                                                rec.mergeClinicalSignificanceForVarIncoming(value);
                                            }
                                            case "SomaticClinicalImpact" -> {
                                                GlobalCounters.getInstance().incrementCounter("CLASSIFICATION_PER_SUBMISSION_"+subName, 1);
                                                String value = "somatic clinical impact: "+el2.getValue().toLowerCase();
                                                rec.mergeClinicalSignificanceForVarIncoming(value);
                                            }
                                            case "Citation" -> {
                                                parseCitation(el2, clinVarId);
                                            }
                                            case "Comment", "ExplanationOfClassification", "ClassificationScore" -> {
                                                // skip: these are explanatory text inside Classification, not the verdict
                                            }
                                            default -> {
                                                GlobalCounters.getInstance().incrementCounter("CLASSIFICATION_PER_SUBMISSION_UNHANDLED_"+subName, 1);
                                            }
                                        }
                                    }
                                }
                                case "Assertion", "StudyName", "StudyDescription" -> {
                                    // metadata only; nothing to extract
                                }
                                case "AttributeSet" -> {
                                    Elements citations = el.getChildElements("Citation");
                                    for( int c=0; c< citations.size(); c++ ) {
                                        Element citation = citations.get(c);
                                        parseCitation(citation, clinVarId);
                                    }
                                }
                                case "ObservedInList" -> {
                                    Elements observedInList = el.getChildElements();
                                    for( int h=0; h<observedInList.size(); h++ ) {
                                        Element observedIn = observedInList.get(h);
                                        Element method = observedIn.getFirstChildElement("Method");
                                        if( method == null ) continue;
                                        Element methodTypeEl = method.getFirstChildElement("MethodType");
                                        if( methodTypeEl == null ) continue;
                                        rec.mergeMethodTypeForVarIncoming( methodTypeEl.getValue().toLowerCase() );
                                    }
                                }
                                case "Comment" -> {
                                    // legacy DB notes column came from ClinVarAssertion/Comment (RCV)
                                    rec.mergeNotesForVarIncoming(el.getValue());
                                }
                                case "SimpleAllele", "Citation", "SubmissionNameList", "ReplacedList", "Haplotype" -> {
                                    // intentionally ignored
                                }
                                case "TraitSet" -> {
                                    Elements traitList = el.getChildElements("Trait");
                                    for( int h=0; h<traitList.size(); h++ ) {
                                        Element trait = traitList.get(h);
                                        parseXRefs(trait, clinVarId);
                                    }
                                }
                                default -> {
                                    GlobalCounters.getInstance().incrementCounter("CLINICAL_ASSERTION_UNHANDLED_"+el.getLocalName(), 1);
                                }
                            }
                        }
                    }
                }

                String preferredTrait = null;
                List<String> traits = new ArrayList<>(); // traits other than preferred
                Element traitMappingList = element.getFirstChildElement("TraitMappingList");
                if( traitMappingList!=null ){
                    Elements traitMappings = traitMappingList.getChildElements();
                    for( int t=0; t<traitMappings.size(); t++ ) {
                        Element traitMapping = traitMappings.get(t);
                        String mappingRef = traitMapping.getAttributeValue("MappingRef");
                        String mappingValue = traitMapping.getAttributeValue("MappingValue");
                        if( "Preferred".equals(mappingRef) ) {
                            preferredTrait = mappingValue;
                        }

                        Elements medGens = traitMapping.getChildElements("MedGen");
                        for( int m=0; m< medGens.size(); m++ ) {
                            Element medGen = medGens.get(m);
                            String medGenCUI = medGen.getAttributeValue("CUI");
                            String medGenName = medGen.getAttributeValue("Name");
                            if( medGenCUI != null && !medGenCUI.equals("None") ) {
                                rec.getXdbIds().addIncomingXdbId(54, medGenCUI, clinVarId); // MedGen xref
                            }
                            rec.getAliases().addIncomingAlias(medGenName, clinVarId, classifiedCondition);
                        }
                    }
                }

                // preferred trait or first trait parsed, becomes the trait name
                if( preferredTrait==null ) {
                    preferredTrait = classifiedCondition;
                }
                if( preferredTrait==null && traits.size()>0 ) {
                    preferredTrait = traits.remove(0);
                }
                if( preferredTrait!=null ) {
                    // legacy DB stored "<name> [<clinVarId>]"; preserve the suffix
                    String suffix = clinVarId.isEmpty() ? "" : " [" + clinVarId + "]";
                    rec.getVarIncoming().setTraitName(preferredTrait + suffix);
                }

                // the remaining traits are made aliases
                for( String trait: traits ) {
                    rec.getAliases().addIncomingAlias(trait, clinVarId, rec.getVarIncoming().getTraitName());
                }
            }
            else if( elName.equals("RecordStatus") ) {
                if( !element.getValue().equals("current") ) {
                    log.warn("NOT CURRENT RECORD!");
                }
            }
            else if( elName.equals("Species") ) {
                if( !element.getValue().equals("Homo sapiens") ) {
                    log.warn("INVALID SPECIES! 'Homo sapiens' was expected!");
                }
            }
            else if( elName.equals("ReplacedList") ) {
                // ignored: info about merged ClinVar variants
            }
            else if( elName.equals("IncludedRecord") ) {
                // ignored: info about merged ClinVar variants
            }
            else {
                GlobalCounters.getInstance().incrementCounter("UNKNOWN_TOP_LEVEL_"+elName, 1);
            }
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    void parseCitation( Element citation, String clinVarId ) {
        Elements ids = citation.getChildElements("ID");
        for( int x=0; x<ids.size(); x++ ) {
            Element id = ids.get(x);
            if( "PubMed".equals(id.getAttributeValue("Source")) ) {
                rec.getXdbIds().addIncomingXdbId(XdbId.XDB_KEY_PUBMED, id.getValue(), clinVarId);
            }
        }
    }

    void parseXRefs( Element el, String clinVarId ) {
        // also descend one level into <Name> children (aggregate Trait/Name/XRef hosts SNOMED CT etc.)
        Elements names = el.getChildElements("Name");
        for( int n=0; n<names.size(); n++ ) {
            parseXRefs(names.get(n), clinVarId);
        }

        Elements xrefs = el.getChildElements("XRef");
        for( int i=0; i<xrefs.size(); i++ ) {
            Element xref = xrefs.get(i);
            String type = xref.getAttributeValue("Type");
            String id = xref.getAttributeValue("ID");
            String db = xref.getAttributeValue("DB");

            if( db == null || id == null ) continue;

            // submitter-lab / per-gene LOVD / GenomeConnect XRefs are non-standard internal IDs;
            // ignore them silently rather than enumerating every institution by name
            if( db.contains(", ")
                    || db.startsWith("Leiden Muscular Dystrophy (")
                    || db.contains(" @ LOVD")
                    || db.startsWith("GenomeConnect") ) {
                continue;
            }

            switch (db) {
                case "OMIM" -> {
                    if (id.contains(".")) {
                        rec.getXdbIds().addIncomingXdbId(53, id, clinVarId); // OMIM Allele, f.e. "613665.004"
                        int dotPos = id.indexOf('.');
                        rec.getXdbIds().addIncomingXdbId(6, id.substring(0, dotPos), clinVarId); // OMIM, f.e. "613665"
                    } else {
                        rec.getXdbIds().addIncomingXdbId(6, id, clinVarId); // OMIM, f.e. "613665"
                    }
                }
                case "OMIM phenotypic series" -> {
                    rec.getXdbIds().addIncomingXdbId(66, "MIM:"+id, clinVarId);
                }
                case "dbSNP" -> {
                    if ("rs".equals(type)) {
                        // store the bare numeric in acc_id; XdbIds.addIncomingXdbId prefixes "rs" in link_text for xdb_key=48
                        rec.getXdbIds().addIncomingXdbId(48, id, clinVarId);
                    } else {
                        GlobalCounters.getInstance().incrementCounter("UNKNOWN_DBSNP_TYPE_"+type, 1);
                    }
                }
                case "MedGen" -> {
                    rec.getXdbIds().addIncomingXdbId(54, id, clinVarId);
                }
                case "MONDO" -> {
                    rec.getXdbIds().addIncomingXdbId(145, id, clinVarId);
                }
                case "MeSH", "MSH" -> {
                    rec.getXdbIds().addIncomingXdbId(47, id, clinVarId);
                }
                case "MESH" -> {
                    // ignored: entries like 'C04.557.450.795.870' are MeSH tree numbers, not accessions
                }
                case "HP", "HPO", "Human Phenotype Ontology" -> {
                    rec.getXdbIds().addIncomingXdbId(166, id, clinVarId);
                }
                case "EFO", "EFO: The Experimental Factor Ontology" -> {
                    rec.getXdbIds().addIncomingXdbId(93, id, clinVarId);
                }
                case "NCI" -> {
                    rec.getXdbIds().addIncomingXdbId(74, id, clinVarId); // NCI Thesaurus
                }
                case "Gene" -> {
                    rec.getXdbIds().addIncomingXdbId(XdbId.XDB_KEY_NCBI_GENE, id, clinVarId);
                }
                case "COSMIC" -> {
                    rec.getXdbIds().addIncomingXdbId(45, id, clinVarId);
                }
                case "Orphanet" -> {
                    String orphaId = id.startsWith("ORPHA") ? id.substring(5) : id;
                    rec.getXdbIds().addIncomingXdbId(62, orphaId, clinVarId);
                }
                case "SNOMED CT" -> {
                    rec.getXdbIds().addIncomingXdbId(55, id, clinVarId);
                }
                case "ADAM", "BRCA1-HCI",
                     "Breast Cancer Information Core (BIC) (BRCA1)",
                     "Breast Cancer Information Core (BIC) (BRCA2)",
                     "ClinGen", "ClinPGx Clinical Annotation", "ClinVar",
                     "dbRBC", "dbVar", "Decipher",
                     "GeneReviews", "Genetic Alliance", "GeneTests",
                     "Genetic Testing Registry (GTR)",
                     "HBVAR", "LOVD 3",
                     "MYBPC3 homepage - Leiden Muscular Dystrophy pages",
                     "NCBI for submitter", "New Leaf Center",
                     "PharmGKB Clinical Annotation", "RettBASE (CDKL5)",
                     "Tuberous sclerosis database (TSC1)", "Tuberous sclerosis database (TSC2)",
                     "UniProtKB", "UniProtKB/Swiss-Prot" -> {
                    // intentionally ignored
                }
                default -> {
                    GlobalCounters.getInstance().incrementCounter("UNKNOWN_XREF_DB_"+db, 1);
                }
            }
        }
    }

    void addRefSeqXref( String hgvsType, String hgvsName, String clinVarId ) {

        // there could be dozens of transcripts in a gene
        // we don't want to load accessions for all of those transcripts/proteins
        if(true) return;

        if( hgvsType.equals("coding") ) {
            int pos = hgvsName.indexOf(':');
            if( pos>0 ) {
                String refSeqAcc = hgvsName.substring(0, pos);
                if( refSeqAcc.startsWith("NP_") || refSeqAcc.startsWith("XP_") ) {
                    rec.getXdbIds().addIncomingXdbId(XdbId.XDB_KEY_GENEBANKPROT, refSeqAcc, clinVarId);
                }
                else if( refSeqAcc.startsWith("NM_") || refSeqAcc.startsWith("XM_") ) {
                    rec.getXdbIds().addIncomingXdbId(XdbId.XDB_KEY_GENEBANKNU, refSeqAcc, clinVarId);
                }
                else {
                    log.warn("addRefSeqXref problem: unrecognized prefix in "+refSeqAcc);
                }
            }
        }
    }

    void handleSoAccId(String soAccId) throws Exception {

        // special handling for obsolete SO terms without replacements
        if( soAccId.equals("SO:1000064") ) {
            // this is an obsolete SO term 'sequence_variation_affecting_reading_frame'
            soAccId = "";
        }

        if( soAccId.isEmpty() ) {
            // SO ACC ID not given -- evaluate measure type
            String varType = rec.getVarIncoming().getObjectType();
            switch (varType) {
                case "deletion":
                    soAccId = "SO:0000159";
                    break;
                case "duplication":
                    soAccId = "SO:1000035";
                    break;
                case "insertion":
                    soAccId = "SO:0000667";
                    break;
                case "indel":
                    soAccId = "SO:1000032";
                    break;
                case "single nucleotide variant":
                    soAccId = "SO:0001483";
                    break;
                case "copy number gain":
                    soAccId = "SO:0001742";
                    break;
                case "copy number loss":
                    soAccId = "SO:0001743";
                    break;
                case "inversion":
                    soAccId = "SO:1000036";
                    break;
                case "microsatellite":
                    soAccId = "SO:0000289";
                    break;
                case "structural variant":
                    soAccId = "SO:0001537";
                    break;
                case "fusion":
                    soAccId = "SO:0000806";
                    break;
                case "translocation":
                    soAccId = "SO:0000199";
                    break;
                case "complex":
                    soAccId = "SO:0001784";
                    break;
                case "tandem duplication":
                    soAccId = "SO:1000173";
                    break;
                case "variation":
                    String molecularConsequence = Utils.defaultString(rec.getVarIncoming().getMolecularConsequence()).replace('_', ' ');
                    if (molecularConsequence.equals("synonymous variant")) {
                        soAccId = "SO:0001819";
                    } else if (molecularConsequence.equals("missense variant")) {
                        soAccId = "SO:0001583";
                    } else if (molecularConsequence.equals("exon loss")) {
                        soAccId = "SO:0001572";
                    } else if (molecularConsequence.isEmpty()) {
                        String varAltName = Utils.defaultString(rec.getVariantAltName()).toLowerCase();
                        if (!varAltName.isEmpty()) {
                            logDebug.info("  alt name: " + varAltName);
                        }
                        if (varAltName.contains("duplication") && varAltName.contains("exon")) {
                            // there is no SO term for 'exon duplication'; therefore we use 'duplication'
                            soAccId = "SO:1000035";
                        } else {
                            soAccId = "SO:0001059"; // sequence alteration
                            logDebug.info("SO ACC ID problem - no molecular consequence - assuming 'sequence alteration'");
                        }
                    } else {
                        log.warn("unknown variation");
                    }
                    break;
                case "protein only":
                    soAccId = "SO:0001816"; // non-synonymous change

                    logDebug.info("SO ACC ID problem - variation=protein_only - assuming 'non-synonymous change'");
                    break;
                default:
                    logDebug.info("handleSoAccId - unsupported object type [" + varType + "]");
                    log.warn("handleSoAccId - unsupported object type [" + varType + "]");
                    break;
            }
        }

        soAccId = qc.getDao().validateSoAccId(soAccId);
        rec.getVarIncoming().setSoAccId(soAccId);
    }

    public Element parseRecord(Element element) {

        if( this.rec !=null ) {

            try {
                if( Utils.defaultString(this.rec.getVarIncoming().getClinicalSignificance()).contains("not provided") ) {
                    GlobalCounters.getInstance().incrementCounter("CLINVAR_ENTRY_CLINICAL_SIGNIFICANCE_NOT_PROVIDED", 1);
                }
                else if( this.rec.getVarIncoming().getTraitName()==null ||
                        this.rec.getVarIncoming().getTraitName().contains("not provided") ||
                        this.rec.getVarIncoming().getTraitName().contains("not specified") ) {
                    GlobalCounters.getInstance().incrementCounter("CLINVAR_ENTRY_CONDITION_NOT_PROVIDED", 1);
                }
                else {
                    GlobalCounters.getInstance().incrementCounter("CLINVAR_ENTRY_OTHER", 1);
                }

                qc.run(rec);
                loader.run(rec);

            }catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }
}
