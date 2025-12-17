package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.xml.XomAnalyzer;
import nu.xom.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jaxen.JaxenException;
import org.jaxen.XPath;
import org.jaxen.xom.XOMXPath;

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
    private String title;


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
        var.setName(title);
        rec.setVarIncoming(var);
    }

    // parse only one element: 'ReferenceClinVarAssertion', skip the others
    public Element parseSubrecord(Element element) {
        try {
            String elName = element.getLocalName();;
            if( elName.equals("ClassifiedRecord") ) {

                Elements simpleAlleles = element.getChildElements("SimpleAllele");
                if( simpleAlleles.size()>1 ) {
                    GlobalCounters.getInstance().incrementCounter("MULTI_ALLELE_VARIANTS_SKIPPED", 1);
                    String clinVarId = simpleAlleles.get(0).getAttributeValue("VariationID");
                    logDebug.info("MULTI_ALLELE_VARIANTS_SKIPPED for "+clinVarId);
                    this.rec = null;
                    return null;
                }
                Element simpleAllele = simpleAlleles.get(0);
                String clinVarId = simpleAllele.getAttributeValue("VariationID");
                createRecord(clinVarId);

                rec.getVarIncoming().setName( xpName.stringValueOf(simpleAllele));
                rec.getVarIncoming().setObjectType( xpVarType.stringValueOf(simpleAllele).toLowerCase() );
                handleSoAccId("");
                rec.setVariantAltName( xpAltName.stringValueOf(simpleAllele) );

                Element geneList = simpleAllele.getFirstChildElement("GeneList");
                if( geneList!=null ) {
                    Elements genes = geneList.getChildElements();
                    for (int i = 0; i < genes.size(); i++) {
                        Element gene = genes.get(i);
                        String geneSymbol = gene.getAttributeValue("Symbol");
                        String geneId = gene.getAttributeValue("GeneID");
                        String hgncId = gene.getAttributeValue("HGNC_ID");
                        rec.getGeneAssociations().add(geneId, geneSymbol);
                        rec.getXdbIds().addIncomingXdbId(XdbId.XDB_KEY_NCBI_GENE, geneId);
                        rec.getXdbIds().addIncomingXdbId(XdbId.XDB_KEY_HGNC, hgncId);
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

                        String positionVCF = el.getAttributeValue("positionVCF");
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

                Element hgvsList = simpleAllele.getFirstChildElement("HGVSlist");
                if( hgvsList!=null ) {
                    Elements hgvss = hgvsList.getChildElements();
                    for( int i=0; i< hgvss.size(); i++ ) {
                        Element hgvs = hgvss.get(i);
                        String hgvsType = hgvs.getAttributeValue("Type");
                        hgvsType = hgvsType.replace(", ", "_").replace(" ","").toLowerCase().replace("hgvs_","");

                        Element e1 = hgvs.getFirstChildElement("NucleotideExpression");
                        if( e1!=null ) {
                            String val = e1.getFirstChildElement("Expression").getValue();
                            if( !Utils.isStringEmpty(val) ) {
                                rec.getHgvsNames().addIncomingHgvsName(hgvsType, val);
                                addRefSeqXref(hgvsType, val);
                            }
                        }
                        Element e2 = hgvs.getFirstChildElement("ProteinExpression");
                        if( e2!=null ) {
                            String val = e2.getFirstChildElement("Expression").getValue();
                            if( !Utils.isStringEmpty(val) ) {
                                rec.getHgvsNames().addIncomingHgvsName(hgvsType, val);
                                addRefSeqXref(hgvsType, val);
                            }
                        }
                        Element e3 = hgvs.getFirstChildElement("MolecularConsequence");
                        if( e3!=null ) {
                            String consequence = e3.getAttributeValue("Type");
                            if( !Utils.isStringEmpty(consequence) ) {
                                rec.getVarIncoming().setMolecularConsequence(consequence);
                            }
                        }
                    }
                }

                Element xrefList = simpleAllele.getFirstChildElement("XRefList");
                if( xrefList!=null ) {
                    parseXRefs(xrefList);
                }

                String classifiedCondition = null;
                Elements rcvLists = element.getChildElements( "RCVList" );
                for( int z=0; z<rcvLists.size(); z++ ) {
                    Element rcvList = rcvLists.get(z);
                    Elements rcvAcc = rcvList.getChildElements("RCVAccession");
                    for( int i=0; i<rcvAcc.size(); i++ ) {
                        Element el = rcvAcc.get(i);
                        String rcv = el.getAttributeValue("Accession");
                        rec.getXdbIds().addIncomingXdbId(52, rcv);

                        Element list = el.getFirstChildElement("ClassifiedConditionList");
                        Elements classifiedConditions = list.getChildElements("ClassifiedCondition");
                        for( int f=0; f<classifiedConditions.size(); f++ ) {
                            Element classifiedConditionEl = classifiedConditions.get(f);
                            if( classifiedCondition==null ) {
                                classifiedCondition = classifiedConditionEl.getValue();
                            }
                        }
                    }
                }

                Elements classifications = element.getFirstChildElement("Classifications").getChildElements();
                for( int i=0; i<classifications.size(); i++ ) {
                    Element classification = classifications.get(0);
                    switch( classification.getLocalName() ) {
                        case "GermlineClassification", "NoClassification" -> {

                        }
                        default -> {
                            System.out.println("todo  unknown classification type");
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
                            System.out.println("unexpected");
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
                                    rec.mergeSubmitterForVarIncoming(Utils.NVL(orgAbbreviation, submitter));
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
                                        switch (el2.getLocalName()) {
                                            case "ReviewStatus" -> {
                                                String reviewStatus = el2.getValue().toLowerCase();
                                                rec.mergeReviewStatusForVarIncoming(reviewStatus);
                                            }
                                            case "GermlineClassification", "NoClassification" -> {
                                                String value = el2.getValue().toLowerCase();
                                                rec.mergeClinicalSignificanceForVarIncoming(value);
                                            }
                                            case "Citation" -> {
                                                parseCitation(el2);
                                            }
                                            case "Comment" -> {
                                                // skip
                                            }
                                            default -> {
                                                System.out.println("handle it");
                                            }
                                        }
                                    }
                                }
                                case "Assertion" -> {

                                }
                                case "AttributeSet" -> {
                                    Elements citations = el.getChildElements("Citation");
                                    for( int c=0; c< citations.size(); c++ ) {
                                        Element citation = citations.get(c);
                                        parseCitation(citation);
                                    }
                                }
                                case "ObservedInList" -> {
                                    Elements observedInList = el.getChildElements();
                                    for( int h=0; h<observedInList.size(); h++ ) {
                                        Element observedIn = observedInList.get(h);
                                        String methodType = observedIn.getFirstChildElement("Method").getFirstChildElement("MethodType").getValue();
                                        methodType = methodType.toLowerCase();
                                        rec.mergeMethodTypeForVarIncoming(methodType);
                                    }
                                }
                                case "SimpleAllele", "Citation", "SubmissionNameList", "Comment" -> {

                                }
                                case "TraitSet" -> {
                                    Elements traitList = el.getChildElements("Trait");
                                    for( int h=0; h<traitList.size(); h++ ) {
                                        Element trait = traitList.get(h);
                                        parseXRefs(trait);
                                    }
                                }
                                default -> {
                                    System.out.println("TODO");
                                }
                            }
                        }
                    }
                }

                String preferredTrait = null;
                List<String> traits = new ArrayList<>(); // traits other than preferred
                Element traitMappingList = element.getFirstChildElement("TraitMappingList");
                {
                    Elements traitMappings = traitMappingList.getChildElements();
                    for( int t=0; t<traitMappings.size(); t++ ) {
                        Element traitMapping = traitMappings.get(t);
                        String mappingRef = traitMapping.getAttributeValue("MappingRef");
                        String mappingValue = traitMapping.getAttributeValue("MappingValue");
                        if( mappingRef.equals("Preferred") ) {
                            preferredTrait = mappingValue;
                        }

                        Elements medGens = traitMapping.getChildElements("MedGen");
                        for( int m=0; m< medGens.size(); m++ ) {
                            Element medGen = medGens.get(m);
                            String medGenCUI = medGen.getAttributeValue("CUI");
                            String medGenName = medGen.getAttributeValue("Name");
                            if( !medGenCUI.equals("None") ) {
                                rec.getXdbIds().addIncomingXdbId(54, medGenCUI); // MedGen xref
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
                    rec.getVarIncoming().setTraitName(preferredTrait);
                }

                // the remaining traits are made aliases
                for( String trait: traits ) {
                    rec.getAliases().addIncomingAlias(trait, clinVarId, preferredTrait);
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
            else {
                System.out.println("unknown sub");
            }
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    void parseCitation( Element citation ) {
        Elements ids = citation.getChildElements("ID");
        for( int x=0; x<ids.size(); x++ ) {
            Element id = ids.get(x);
            if( id.getAttributeValue("Source").equals("PubMed") ) {
                rec.getXdbIds().addIncomingXdbId(XdbId.XDB_KEY_PUBMED, id.getValue());
            }
        }
    }

    void parseXRefs( Element el ) {
        Elements xrefs = el.getChildElements("XRef");
        for( int i=0; i<xrefs.size(); i++ ) {
            Element xref = xrefs.get(i);
            String type = xref.getAttributeValue("Type");
            String id = xref.getAttributeValue("ID");
            String db = xref.getAttributeValue("DB");

            switch (db) {
                case "OMIM" -> {
                    if (id.contains(".")) {
                        rec.getXdbIds().addIncomingXdbId(53, id); // OMIM Allele, f.e. "613665.004"
                        int dotPos = id.indexOf('.');
                        rec.getXdbIds().addIncomingXdbId(6, id.substring(0, dotPos)); // OMIM, f.e. "613665"
                    } else {
                        rec.getXdbIds().addIncomingXdbId(6, id); // OMIM, f.e. "613665"
                    }
                }
                case "dbSNP" -> {
                    if (type.equals("rs")) {
                        rec.getXdbIds().addIncomingXdbId(48, "rs" + id); //  f.e. "rs587776507"
                    } else {
                        System.out.println("unknown dbSNP type");
                    }
                }
                case "MedGen" -> {
                    rec.getXdbIds().addIncomingXdbId(54, id);
                }
                case "ClinGen" -> {
                    // just ignore it
                }
                default -> {
                    System.out.println("unknown xref type: "+db);
                }
            }
        }
    }

    void addRefSeqXref( String hgvsType, String hgvsName ) {

        // there could be dozens of transcripts in a gene
        // we don't want to load accessions for all of those transcripts/proteins
        if(true) return;

        if( hgvsType.equals("coding") ) {
            int pos = hgvsName.indexOf(':');
            if( pos>0 ) {
                String refSeqAcc = hgvsName.substring(0, pos);
                if( refSeqAcc.startsWith("NP_") || refSeqAcc.startsWith("XP_") ) {
                    rec.getXdbIds().addIncomingXdbId(XdbId.XDB_KEY_GENEBANKPROT, refSeqAcc);
                }
                else if( refSeqAcc.startsWith("NM_") || refSeqAcc.startsWith("XM_") ) {
                    rec.getXdbIds().addIncomingXdbId(XdbId.XDB_KEY_GENEBANKNU, refSeqAcc);
                }
                else {
                    System.out.println("*** addRefSeqXref PROBLEM!");
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

    static XPath xpName, xpVarType, xpAltName;

    static {
        try {
            xpName = new XOMXPath("Name");
            xpVarType = new XOMXPath("VariantType");
            xpAltName = new XOMXPath("OtherNameList/Name");
        }
        catch(Exception e) {
            Utils.printStackTrace(e, LogManager.getLogger("loader"));
            e.printStackTrace();
        }
    }
}


/*


 */