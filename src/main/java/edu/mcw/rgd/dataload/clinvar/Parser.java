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
            if( !element.getLocalName().equals("ReferenceClinVarAssertion") ) {
                if( element.getLocalName().equals("ClinVarAssertion") && rec!=null ) {
                    // extract ClinVarAssertion/Comment
                    Element elComment = element.getFirstChildElement("Comment");
                    if( elComment!=null ) {
                        rec.mergeNotesForVarIncoming(elComment.getValue());
                    }

                    // extract ClinVarAssertion/submitter
                    Element elSubmission = element.getFirstChildElement("ClinVarSubmissionID");
                    if( elSubmission!=null ) {
                        rec.mergeSubmitterForVarIncoming(elSubmission.getAttributeValue("submitter"));
                    }

                    parsePubMedId(element, rec);

                    List<Attribute> ids = xpOmimAllele.selectNodes(element);
                    for( Attribute a: ids ) {
                        rec.getXdbIds().addIncomingXdbId(53, a.getValue(), rec.getClinVarId());
                    }
                }
                else if( element.getLocalName().equals("Title") ) {
                    title = element.getValue();

                    // the original name [USH2A:c.4440C>T (p.Ser1480=) AND AllHighlyPenetrant]
                    // we only keep the first part: [USH2A:c.4440C>T (p.Ser1480=)]
                    int andPos = title.lastIndexOf(" AND ");
                    if( andPos>0 )
                        title = title.substring(0, andPos).trim();
                }
                return null;
            }

            String clinVarId = xpClinVarAcc.stringValueOf(element);
            logDebug.info(clinVarId);

            // parse Measures from MeasureSet
            Elements measureSets = element.getChildElements("MeasureSet");
            for( int i=0; i<measureSets.size(); i++ ) {
                Element measureSet = measureSets.get(i);
                Elements measures = measureSet.getChildElements("Measure");
                if( measures.size()>1 ) {
                    GlobalCounters.getInstance().incrementCounter("MULTI_ALLELE_VARIANTS_SKIPPED", 1);
                    logDebug.info("MULTI_ALLELE_VARIANTS_SKIPPED for "+clinVarId);
                    this.rec = null;
                    break;
                }
                for( int j=0; j<measures.size(); j++ ) {
                    Element measure = measures.get(j);
                    createRecord(measure.getAttributeValue("ID"));
                    rec.getVarIncoming().setObjectType(measure.getAttributeValue("Type").toLowerCase());

                    parseElementsForMeasure(measure, rec, clinVarId);
                }
            }

            if( rec!=null ) {
                rec.getXdbIds().addIncomingXdbId(52, clinVarId, clinVarId);
                rec.getXdbIds().addIncomingXdbId(54, xpMedGen.stringValueOf(element), clinVarId);
                rec.getXdbIds().addIncomingXdbId(55, xpSnomedCt.stringValueOf(element), clinVarId);

                parseClinicalSignificance(element, rec);

                parseTraits(element, clinVarId);

                String ageOfOnset = xpAgeOfOnset.stringValueOf(element);
                if( !ageOfOnset.isEmpty() )
                    rec.getVarIncoming().setAgeOfOnset(ageOfOnset.toLowerCase());

                String prevalence = xpPrevalence.stringValueOf(element);
                if( !prevalence.isEmpty() )
                    rec.getVarIncoming().setPrevalence(prevalence.toLowerCase());

                String methodType = xpMethodType.stringValueOf(element);
                if( !methodType.isEmpty() )
                    rec.getVarIncoming().setMethodType(methodType.toLowerCase());

                parseOmimIds(element, rec, clinVarId);
            }

        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    void parseTraits(Element element, String clinVarId) throws JaxenException {

        // 1st preferred trait name is the official trait name for the variant
        // the other preferred traits and alternate traits are aliases
        // if there is no preferred trait (very rare), first alternate trait becomes the trait name
        parseTraits(xpPreferredTrait.selectNodes(element), clinVarId);
        parseTraits(xpAlternateTrait.selectNodes(element), clinVarId);
    }

    void parseTraits(List<Element> traits, String clinVarId) {
        if( !traits.isEmpty() ) {

            // first trait parsed, becomes the trait name
            int i=0;
            if ( Utils.isStringEmpty(rec.getVarIncoming().getTraitName()) ) {
                rec.getVarIncoming().setTraitName(parseTraitName(traits.get(0)) + " [" + clinVarId + "]");
                i++;
            }

            // the remaining traits are made aliases
            for( ; i<traits.size(); i++ ) {
                rec.getAliases().addIncomingAlias(parseTraitName(traits.get(i)), clinVarId, rec.getVarIncoming().getTraitName());
            }
        }
    }

    String parseTraitName(Element el) {
        String traitName = el.getValue();
        String lcTraitName = traitName.toLowerCase();
        if( lcTraitName.endsWith(" (1 patient)") ) {
            traitName = traitName.substring(0, traitName.length()-12);
        } else
        if( lcTraitName.endsWith(" (1 family)") ) {
            traitName = traitName.substring(0, traitName.length()-11);
        }
        return traitName;
    }

    void parseOmimIds(Element element, Record rec, String clinVarId) throws Exception {
        List<Attribute> ids = xpOmim.selectNodes(element);
        for( Attribute a: ids ) {
            rec.getXdbIds().addIncomingXdbId(XdbId.XDB_KEY_OMIM, a.getValue(), clinVarId);
        }
    }

    public Element parseRecord(Element element) {

        if( this.rec !=null ) {
            try {
                if( this.rec.getVarIncoming().getClinicalSignificance().contains("not provided") ) {
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

    /**
     * parse SO_ACC_ID, if available [  AttributeSet/XRef/@ID(@DB='SequenceOntology')  ]
     * parse variant name            [  Name/ElementValue
     */
    void parseElementsForMeasure(Element measure, Record rec, String clinVarId) throws Exception {

        //rec.getVarIncoming().setName(xpVariantName.stringValueOf(measure));
        rec.setVariantAltName(xpVariantAltName.stringValueOf(measure));
        rec.getXdbIds().addIncomingXdbId(48, xpRsId.stringValueOf(measure), clinVarId);

        String refSeqId = xpRefSeqId.stringValueOf(measure);
        int pos = refSeqId.indexOf(':');
        if( pos>=0 ) // truncate after colon part
            refSeqId = refSeqId.substring(0, pos);
        rec.getXdbIds().addIncomingXdbId(XdbId.XDB_KEY_GENEBANKNU, refSeqId, clinVarId);

        // parse hgvs names
        for( Element el: (List<Element>)xpHgvsName.selectNodes(measure) ) {
            String hgvsNameType = el.getAttributeValue("Type");
            hgvsNameType = hgvsNameType.replace(", ", "_").replace(" ","").toLowerCase().replace("hgvs_","");
            String hgvsName = el.getValue();
            if( !Utils.isStringEmpty(hgvsName) ) {
                rec.getHgvsNames().addIncomingHgvsName(hgvsNameType, hgvsName);
            }
        }

        String nucleotideChange = xpNucChange.stringValueOf(measure).toLowerCase();
        if( !nucleotideChange.isEmpty() )
            rec.getVarIncoming().setNucleotideChange(nucleotideChange);

        String molecularConsequence = xpMolConsequence.stringValueOf(measure).toLowerCase();
        if( !molecularConsequence.isEmpty() )
            rec.getVarIncoming().setMolecularConsequence(molecularConsequence);

        Elements elements = measure.getChildElements("SequenceLocation");
        for( int i=0; i<elements.size(); i++ ) {
            Element loc = elements.get(i);

            // if there is 'Accession' attribute, it must start with 'NC_'
            String acc = loc.getAttributeValue("Accession");
            if( acc!=null && !acc.startsWith("NC_") ) {
                continue;
            }

            String start = loc.getAttributeValue("start");
            if( start==null )
                start = loc.getAttributeValue("innerStart");
            if( start==null )
                start = loc.getAttributeValue("outerStart");

            String stop = loc.getAttributeValue("stop");
            if( stop==null )
                stop = loc.getAttributeValue("innerStop");
            if( stop==null )
                stop = loc.getAttributeValue("outerStop");

            String strand = loc.getAttributeValue("Strand");

            if( !Utils.isStringEmpty(start) && !Utils.isStringEmpty(stop) ) {
                rec.getMapPositions().addPos(
                        loc.getAttributeValue("Assembly"),
                        loc.getAttributeValue("Chr"),
                        loc.getAttributeValue("Accession"),
                        start,
                        stop,
                        strand
                        );
            }

            String refNuc = loc.getAttributeValue("referenceAlleleVCF");
            if( refNuc!=null && refNuc.length()>0 ) {
                rec.getVarIncoming().setRefNuc(refNuc);
            }

            String varNuc = loc.getAttributeValue("alternateAlleleVCF");
            if( varNuc!=null && varNuc.length()>0 ) {
                rec.getVarIncoming().setVarNuc(varNuc);
            }
        }

        elements = measure.getChildElements("CytogeneticLocation");
        for( int i=0; i<elements.size(); i++ ) {
            Element loc = elements.get(i);
            rec.getMapPositions().addCytoPos(loc.getValue());
        }

        elements = measure.getChildElements("MeasureRelationship");
        for( int i=0; i<elements.size(); i++ ) {
            Element mrs = elements.get(i);
            Elements els = mrs.getChildElements();
            String geneId=null, geneSymbol=null, geneName=null;
            for( int j=0; j<els.size(); j++ ) {
                Element el = els.get(j);
                if( el.getLocalName().equals("Symbol") ) {
                    Elements symbols = el.getChildElements("ElementValue");
                    for( int k=0; k<symbols.size(); k++ ) {
                        Element symbol = symbols.get(k);
                        if( symbol.getAttributeValue("Type").equals("Preferred") )
                            geneSymbol = symbol.getValue();
                    }
                }
                else if( el.getLocalName().equals("XRef") ) {
                    if( el.getAttributeValue("DB").equals("Gene") ) {
                        geneId = el.getAttributeValue("ID");
                        rec.getXdbIds().addIncomingXdbId(XdbId.XDB_KEY_NCBI_GENE, geneId, clinVarId);
                    }
                }
                if( el.getLocalName().equals("Name") ) {
                    Elements names = el.getChildElements("ElementValue");
                    for( int k=0; k<names.size(); k++ ) {
                        Element name = names.get(k);
                        if( name.getAttributeValue("Type").equals("Preferred") )
                            geneName = name.getValue();
                    }
                }
            }
            rec.getGeneAssociations().add(geneId, geneSymbol!=null ? geneSymbol : geneName);
        }

        handleSoAccId(measure, rec);
    }

    void parseClinicalSignificance(Element element, Record rec) throws Exception {

        rec.getVarIncoming().setClinicalSignificance(xpClinicalSignificance.stringValueOf(element).toLowerCase());
        rec.getVarIncoming().setReviewStatus(xpReviewStatus.stringValueOf(element));
        String dateLastEvaluated = xpDateLastEvaluated.stringValueOf(element);
        if( !dateLastEvaluated.isEmpty() ) {
            Date dt;
            synchronized (SDT_YYYYMMDD) { // Date.parse() must be synchronized among all threads, to avoid data corruption
                dt = SDT_YYYYMMDD.parse(dateLastEvaluated);
            }
            rec.getVarIncoming().setDateLastEvaluated(dt);
        }
    }

    void parsePubMedId(Element element, Record rec) throws Exception {

        String clinVarId = rec.getClinVarId();
        for( Element el: (List<Element>)xpPubmedId.selectNodes(element) ) {
            rec.getXdbIds().addIncomingXdbId(XdbId.XDB_KEY_PUBMED, el.getValue(), clinVarId);
        }
    }

    void handleSoAccId(Element measure, Record rec) throws Exception {

        String soAccId = xpSoAccId.stringValueOf(measure);

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

    static XPath xpClinicalSignificance, xpReviewStatus, xpDateLastEvaluated, xpSnomedCt;
    static XPath xpClinVarAcc, xpPubmedId, xpAgeOfOnset, xpPrevalence, xpMethodType, xpOmim;
    static XPath xpVariantName, xpVariantAltName, xpSoAccId, xpRsId, xpOmimAllele, xpRefSeqId, xpMolConsequence;
    static XPath xpMedGen, xpHgvsName, xpPreferredTrait, xpAlternateTrait, xpNucChange, xpRefAllele, xpAltAllele;

    static {
        try {
            xpClinicalSignificance = new XOMXPath("ClinicalSignificance/Description");
            xpReviewStatus = new XOMXPath("ClinicalSignificance/ReviewStatus");
            xpDateLastEvaluated = new XOMXPath("ClinicalSignificance/@DateLastEvaluated");

            xpClinVarAcc = new XOMXPath("ClinVarAccession[@Type='RCV']/@Acc");
            xpPubmedId = new XOMXPath(".//Citation/ID[@Source='PubMed']");
            xpMethodType = new XOMXPath("ObservedIn/Method/MethodType");
            xpOmimAllele = new XOMXPath("ExternalID[@DB='OMIM' and @Type='Allelic variant']/@ID");

            // within Measure
            xpVariantName = new XOMXPath("Name/ElementValue[@Type='preferred name' or @Type='Preferred']");
            xpVariantAltName = new XOMXPath("Name/ElementValue[@Type='Alternate']");
            xpSoAccId = new XOMXPath("AttributeSet/XRef[@DB='Sequence Ontology']/@ID");
            xpRsId = new XOMXPath("XRef[@DB='dbSNP' and @Type='rs']/@ID");
            xpRefSeqId = new XOMXPath("AttributeSet/XRef[@DB='RefSeq']/@ID");
            xpHgvsName = new XOMXPath("AttributeSet/Attribute[starts-with(@Type,'HGVS')]");
            xpNucChange = new XOMXPath("AttributeSet/Attribute[@Type='nucleotide change']");
            xpMolConsequence = new XOMXPath("AttributeSet/Attribute[@Type='MolecularConsequence']");

            // traits (within ReferenceClinVarAssertion)
            xpOmim = new XOMXPath(".//XRef[@DB='OMIM' and @Type='MIM']/@ID");
            xpMedGen = new XOMXPath("TraitSet/Trait/XRef[@DB='MedGen']/@ID");
            xpSnomedCt = new XOMXPath("TraitSet/Trait/Name/XRef[@DB='SNOMED CT']/@ID");
            xpPreferredTrait = new XOMXPath("TraitSet/Trait/Name/ElementValue[@Type='Preferred']");
            xpAlternateTrait = new XOMXPath("TraitSet/Trait/Name/ElementValue[@Type='Alternate']");
            xpAgeOfOnset = new XOMXPath("TraitSet/Trait/AttributeSet/Attribute[@Type='age of onset']");
            xpPrevalence = new XOMXPath("TraitSet/Trait/AttributeSet/Attribute[@Type='prevalence']");
        }
        catch(Exception e) {
            Utils.printStackTrace(e, LogManager.getLogger("loader"));
            e.printStackTrace();
        }
    }
}
