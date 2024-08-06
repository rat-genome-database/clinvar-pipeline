package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.dao.impl.*;
import edu.mcw.rgd.dao.impl.variants.VariantDAO;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.ontology.Annotation;
import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.datamodel.ontologyx.TermSynonym;
import edu.mcw.rgd.datamodel.ontologyx.TermWithStats;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.process.CounterPool;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.object.BatchSqlUpdate;

import java.sql.Types;
import java.util.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 2/11/14
 * @since 2:42 PM
 * wrapper to centralize all code accessing database
 */
public class Dao {

    public static final String ASSOC_TYPE = "variant_to_gene";

    Logger logInsertedVariants = LogManager.getLogger("insertedVariants");
    Logger logUpdatedVariants = LogManager.getLogger("updatedVariants");
    Logger logGeneAssociations = LogManager.getLogger("geneAssociations");
    Logger logMapPos = LogManager.getLogger("mapPos");
    Logger logXdbIds = LogManager.getLogger("xdbIds");
    Logger logHgvsNames = LogManager.getLogger("hgvsNames");
    Logger logAliases = LogManager.getLogger("aliases");
    Logger logAnnotations = LogManager.getLogger("annotations");
    Logger logAnnotator = LogManager.getLogger("annotator");
    Logger logTraitNames = LogManager.getLogger("traitNameUpdates");
    Logger logSubmitters = LogManager.getLogger("submitterUpdates");
    Logger logNotes = LogManager.getLogger("notesUpdates");

    private AliasDAO aliasDAO = new AliasDAO();
    private AnnotationDAO annotationDAO = new AnnotationDAO();
    private AssociationDAO associationDAO = new AssociationDAO();
    private GeneDAO geneDAO = associationDAO.getGeneDAO();
    private GenomicElementDAO geDAO = new GenomicElementDAO();
    private MapDAO mapDAO = new MapDAO();
    private OntologyXDAO ontologyDAO = new OntologyXDAO();
    private RGDManagementDAO rgdIdDAO = new RGDManagementDAO();
    private VariantInfoDAO variantInfoDAO = new VariantInfoDAO();
    private XdbIdDAO xdbIdDAO = new XdbIdDAO();
    private VariantDAO vdao = new VariantDAO();
    private String deleteThresholdForStaleXdbIds;


    public String getConnectionInfo() {
        return geneDAO.getConnectionInfo();
    }

    /**
     * get variant object given its symbol and object type
     * @param symbol symbol of variant
     * @return VariantInfo object for variant matching the symbol or null
     * @throws Exception when unexpected error in spring framework occurs
     */
    public VariantInfo getVariantBySymbol(String symbol) throws Exception {

        List<VariantInfo> results = variantInfoDAO.getVariantsBySymbol(symbol);
        if( results.isEmpty() )
            return null;
        if( results.size()>1 ) {
            throw new Exception("Unexpected: multiple elements with OBJECT_KEY=7 and symbol="+symbol);
        }
        return results.get(0);
    }

    /**
     * insert variant into RGD database:
     * <ol>
     * <li>create a new RGD_ID (insert a new row into RGD_IDS)</li>
     * <li>insert variant with this new RGD_ID into GENOMIC_ELEMENTS</li>
     * <li>insert variant with this new RGD_ID into CLINVAR</li>
     * </ol>
     * Note: method must be synchronized to prevent issue with parallel calls to this method
     *       by multiple threads
     * @param var VariantInfo object
     * @return count of rows affected
     * @throws Exception when unexpected error in spring framework occurs
     */
    synchronized public int insertVariant(VariantInfo var) throws Exception{
        notesFixup(var);

        RgdId rgdId = rgdIdDAO.createRgdId(RgdId.OBJECT_KEY_VARIANTS, "ACTIVE", Manager.SOURCE, SpeciesType.HUMAN);
        var.setRgdId(rgdId.getRgdId());
        var.setObjectKey(rgdId.getObjectKey());
        var.setSpeciesTypeKey(rgdId.getSpeciesTypeKey());
        int rowsAffected = 1 + variantInfoDAO.insertVariantInfo(var);
        logInsertedVariants.debug(var.dump("|"));
        return rowsAffected;
    }

    void notesFixup( VariantInfo info ) {
        String newNotes1 = info.getNotes();
        if( newNotes1 != null ) {
            if( newNotes1.length() > 3900 ) {
                String newNotes2 = newNotes1.substring(0, 3900)+" ...";
                logNotes.debug("### NOTES FIXUP  RGD:" + info.getRgdId());
                info.setNotes(newNotes2);
            }
        }
    }

    /**
     * Update variant in tables GENOMIC_ELEMENTS,CLINVAR given rgdID
     *
     * @return true if the variant has been updated;
     *   false otherwise (incoming variant is the same as variant in RGD)
     * @throws Exception when unexpected error in spring framework occurs
     */
    public boolean updateVariant(VariantInfo varNew, VariantInfo varOld) throws Exception{

        varNew.setRgdId(varOld.getRgdId());

        // trait names should stay unchanged: it will be updated by TraitNameCollection
        varNew.setTraitName(varOld.getTraitName());

        notesFixup(varNew);

        String varOldDump = varOld.dump("|");
        String varNewDump = varNew.dump("|");
        if( varOldDump.equals(varNewDump) ) {
            return false;
        }
        logUpdatedVariants.debug("OLD: "+varOldDump);
        logUpdatedVariants.debug("NEW: "+varNewDump);
        try {
            variantInfoDAO.updateVariant(varNew);
        } catch(Exception e) {
            Logger l = LogManager.getLogger("dbg");
            l.warn("EXCEPTION\n"+
                    "OLD: "+varOldDump+"\n"+
                    "NEW: "+varNewDump+"\n"+
                    e.toString()
            );
            throw new RuntimeException(e);
        }
        return true;
    }

    public void updateTraitName(int rgdId, String oldTraitName, String newTraitName) throws Exception{

        String sql = "UPDATE clinvar SET trait_name=? WHERE rgd_id=?";

        logTraitNames.debug(rgdId
                + "\n    OLD " + oldTraitName
                + "\n    NEW " + newTraitName);

        variantInfoDAO.update(sql, newTraitName, rgdId);

        updateVariantLastModifiedDate(rgdId);
    }

    public void updateSubmitter(int rgdId, String oldSubmitter, String newSubmitter) throws Exception{

        String sql = "UPDATE clinvar SET submitter=? WHERE rgd_id=?";

        logSubmitters.debug(rgdId
                + "\n    OLD " + oldSubmitter
                + "\n    NEW " + newSubmitter);

        variantInfoDAO.update(sql, newSubmitter, rgdId);

        updateVariantLastModifiedDate(rgdId);
    }

    public void updateNotes(int rgdId, String oldNotes, String newNotes) throws Exception{

        String sql = "UPDATE genomic_elements SET notes=? WHERE rgd_id=?";

        logNotes.debug(rgdId
                + "\n    OLD " + oldNotes
                + "\n    NEW " + newNotes);

        geDAO.update(sql, newNotes, rgdId);

        updateVariantLastModifiedDate(rgdId);
    }

    public int getTotalNotesLength() throws Exception {
        String sql = "SELECT SUM(LENGTH(notes)) FROM genomic_elements WHERE source='CLINVAR'";
        return geDAO.getCount(sql);
    }

    /**
     * update last modified date for specified rgd id
     * @param rgdId rgd id
     * @throws Exception when unexpected error in spring framework occurs
     */
    public void updateVariantLastModifiedDate(int rgdId) throws Exception {
        rgdIdDAO.updateLastModifiedDate(rgdId);
    }

    public List<VariantInfo> getActiveVariants() throws Exception {
        return variantInfoDAO.getVariantsBySource("CLINVAR");

        /*
        String sql = "SELECT v.*,ge.*,r.species_type_key,r.object_status,r.object_key FROM clinvar v,genomic_elements ge, rgd_ids r "+
                "WHERE ge.source=? AND ge.rgd_id=r.rgd_id AND r.object_key=? AND v.rgd_id=ge.rgd_id "+
                "and Trait_name like '%Myoclonic-atonic epilepsy%'";
        VariantQuery q = new VariantQuery(geneDAO.getDataSource(), sql);
        return geneDAO.execute(q, new Object[]{"CLINVAR", 7});
        */
    }

    // =========== ASSOCIATED GENES =================

    public Gene getGene(int rgdId) throws Exception {
        return geneDAO.getGene(rgdId);
    }

    public List<Gene> getHomologs(int geneRgdId) throws Exception {
        return geneDAO.getHomologs(geneRgdId);
    }

    synchronized public List<Gene> getHumanGenesByGeneId(String geneId) throws Exception {
        List<Gene> geneList = _cacheGeneLists.get(geneId);
        if( geneList==null ) {
            geneList = xdbIdDAO.getActiveGenesByXdbId(XdbId.XDB_KEY_NCBI_GENE, geneId);
            Iterator<Gene> it = geneList.iterator();
            while( it.hasNext() ) {
                Gene gene = it.next();
                if( gene.getSpeciesTypeKey()!=SpeciesType.HUMAN )
                    it.remove();
            }
            _cacheGeneLists.put(geneId, geneList);
        }
        return geneList;
    }
    private static Map<String,List<Gene>> _cacheGeneLists = new HashMap<>();

    synchronized public List<Integer> getGeneRgdIdsBySymbol(String geneSymbol) throws Exception {
        List<Integer> geneList = _cacheGeneRgdIdsBySymbolMap.get(geneSymbol);
        if( geneList==null ) {
            geneList = geneDAO.getAllGeneRgdIdsBySymbol(geneSymbol, SpeciesType.HUMAN);
            _cacheGeneRgdIdsBySymbolMap.put(geneSymbol, geneList);
        }
        return geneList;
    }
    private static Map<String, List<Integer>> _cacheGeneRgdIdsBySymbolMap = new HashMap<>();


    public List<Integer> getAssociatedGenes(int variantRgdId) throws Exception {
        List<Association> associations = associationDAO.getAssociationsForMasterRgdId(variantRgdId, ASSOC_TYPE);
        List<Integer> rgdIds = new ArrayList<>(associations.size());
        for( Association assoc: associations ) {
            rgdIds.add(assoc.getDetailRgdId());
        }
        return rgdIds;
    }

    public int createGeneAssociation(int variantRgdId, int geneRgdId) throws Exception {

        Association assoc = new Association();
        assoc.setAssocType(ASSOC_TYPE);
        assoc.setMasterRgdId(variantRgdId);
        assoc.setDetailRgdId(geneRgdId);
        assoc.setSrcPipeline(Manager.SOURCE);
        associationDAO.insertAssociation(assoc);
        logGeneAssociations.debug("INSERT "+assoc.dump("|"));
        return assoc.getAssocKey();
    }

    public int deleteGeneAssociation(int variantRgdId, int geneRgdId) throws Exception {

        Association assoc = new Association();
        assoc.setAssocType(ASSOC_TYPE);
        assoc.setMasterRgdId(variantRgdId);
        assoc.setDetailRgdId(geneRgdId);
        assoc.setSrcPipeline(Manager.SOURCE);
        logGeneAssociations.debug("DELETE "+assoc.dump("|"));

        return associationDAO.deleteAssociations(variantRgdId, geneRgdId, ASSOC_TYPE);
    }

    // =========== XDB IDS =================

    public List<XdbId> getXdbIds(int variantRgdId) throws Exception {

        XdbId filter = new XdbId();
        filter.setRgdId(variantRgdId);
        filter.setSrcPipeline(Manager.SOURCE);
        return xdbIdDAO.getXdbIds(filter);
    }

    public List<XdbId> getXdbIds(int variantRgdId, int xdbKey) throws Exception {
        return xdbIdDAO.getXdbIdsByRgdId(xdbKey, variantRgdId);
    }

    public List<XdbId> getXdbIds(int variantRgdId, List<Integer> xdbKeys) throws Exception {
        return xdbIdDAO.getXdbIdsByRgdId(xdbKeys, variantRgdId);
    }

    /// xdb-id count for ClinVar pipeline
    public int getXdbIdCount() throws Exception {
        String sql = "SELECT COUNT(*) FROM rgd_acc_xdb WHERE src_pipeline='CLINVAR'";
        return xdbIdDAO.getCount(sql);
    }

    public int deleteStaleXdbIds(int originalXdbIdCount, Date staleXdbIdsCutoffDate, Logger log) throws Exception {

        int staleXdbIdsDeleteThresholdPerc = Integer.parseInt(getDeleteThresholdForStaleXdbIds().substring(0, getDeleteThresholdForStaleXdbIds().length()-1));
        int staleAnnotDeleteThresholdCount = (staleXdbIdsDeleteThresholdPerc*originalXdbIdCount) / 100;

        List<XdbId> obsoleteXdbIds = xdbIdDAO.getXdbIdsModifiedBefore(staleXdbIdsCutoffDate, "CLINVAR", 0);
        if( !obsoleteXdbIds.isEmpty() ) {
            GlobalCounters.getInstance().incrementCounter("XDB_IDS_OBSOLETE_COUNT", obsoleteXdbIds.size());
        }

        if( obsoleteXdbIds.size() > staleAnnotDeleteThresholdCount ) {

            log.warn("WARNING: OBSOLETE XDB IDS COUNT: "+obsoleteXdbIds.size());
            log.warn("WARNING: OBSOLETE XDB IDS DELETE THRESHOLD OF "+getDeleteThresholdForStaleXdbIds() + " IS: "+ staleAnnotDeleteThresholdCount);
            log.warn("WARNING: OBSOLETE XDB IDS NOT DELETED: "+getDeleteThresholdForStaleXdbIds()+" THRESHOLD VIOLATED!");
            return 0;
        }

        for( XdbId id: obsoleteXdbIds ) {
            logXdbIds.debug("DELETE "+id.dump("|"));
        }

        xdbIdDAO.deleteXdbIds(obsoleteXdbIds);
        if( !obsoleteXdbIds.isEmpty() ) {
            GlobalCounters.getInstance().incrementCounter("XDB_IDS_DELETED", obsoleteXdbIds.size());
        }
        return obsoleteXdbIds.size();
    }

    public int insertXdbIds(List<XdbId> xdbIds) throws Exception {
        int rowsAffected = xdbIdDAO.insertXdbs(xdbIds);
        for( XdbId id: xdbIds ) {
            logXdbIds.debug("INSERT "+id.dump("|"));
        }
        return rowsAffected;
    }

    public int updateXdbIds(List<XdbId> xdbIds) throws Exception {

        List<Integer> accXdbKeys = new ArrayList<>(xdbIds.size());
        for( XdbId id: xdbIds ) {
            accXdbKeys.add(id.getKey());
        }
        return xdbIdDAO.updateModificationDate(accXdbKeys);
    }

    // =========== MAP DATA ===================

    public List<MapData> getMapData(int varRgdId) throws Exception {
        List<MapData> mds = mapDAO.getMapData(varRgdId);

        // filter out data with not-conforming source
        Iterator<MapData> it = mds.iterator();
        while( it.hasNext() ) {
            MapData md = it.next();
            if( !Utils.stringsAreEqualIgnoreCase(md.getSrcPipeline(), Manager.SOURCE) )
                it.remove();
        }
        return mds;
    }

    public void updateMapData(List<MapData> mds) throws Exception {

        mapDAO.updateMapData(mds);

        for( MapData md: mds ) {
            logMapPos.debug("UPDATE "+md.dump("|"));
        }
    }

    // must be synchronized unless MAPS_DATA table will start using sequences to insert new data
    synchronized public void insertMapData(List<MapData> mds) throws Exception {

        mapDAO.insertMapData(mds);

        for( MapData md: mds ) {
            logMapPos.debug("INSERT "+md.dump("|"));
        }
    }

    public void deleteMapData(List<MapData> mds) throws Exception {

        mapDAO.deleteMapData(mds);

        for( MapData md: mds ) {
            logMapPos.debug("DELETE "+md.dump("|"));
        }
    }

    // =========== HGVS NAMES =================

    public List<HgvsName> getHgvsNames(int rgdId) throws Exception {
        return variantInfoDAO.getHgvsNames(rgdId);
    }

    public int insertHgvsNames(List<HgvsName> hgvsNames) throws Exception {
        int rowCount = variantInfoDAO.insertHgvsNames(hgvsNames);
        for( HgvsName hgvsName: hgvsNames ) {
            logHgvsNames.debug("INSERT "+hgvsName.dump("|"));
        }
        return rowCount;
    }

    public int deleteHgvsNames(List<HgvsName> hgvsNames) throws Exception {
        for( HgvsName hgvsName: hgvsNames ) {
            logHgvsNames.debug("DELETE "+hgvsName.dump("|"));
        }
        return variantInfoDAO.deleteHgvsNames(hgvsNames);
    }

    // =========== ALIASES =================

    public List<Alias> getAliases(int rgdId) throws Exception {
        return aliasDAO.getAliases(rgdId);
    }

    // must be synchronized as long as we won't use Oracle sequence to insert new alias
    synchronized public int insertAliases(List<Alias> aliases) throws Exception {
        int rowCount = aliasDAO.insertAliases(aliases);
        for( Alias alias: aliases ) {
            logAliases.debug("INSERT "+alias.dump("|"));
        }
        return rowCount;
    }

    synchronized public int deleteAliases(List<Alias> aliases) throws Exception {
        for( Alias alias: aliases ) {
            logAliases.debug("DELETE "+alias.dump("|"));
        }
        return aliasDAO.deleteAliases(aliases);
    }

    // =========== SO ACC ID =================

    public String validateSoAccId(String soAccId) throws Exception {

        Term term = ontologyDAO.getTermByAccId(soAccId);
        if( term!=null && term.isObsolete() ) {
            // the matching term is obsolete: try to find substitute term
            //
            // 1. see if the current term has a 'replaced_by' synonym
            for( TermSynonym tsyn: ontologyDAO.getTermSynonyms(soAccId) ) {
                if( Utils.stringsAreEqual(tsyn.getType(), "replaced_by") ) {
                    GlobalCounters.getInstance().incrementCounter("VARIANTS_WITH_SUBSTITUTE_SO_ACC_ID", 1);
                    return validateSoAccId(tsyn.getName());
                }
            }

            // 2. see if there is another term having our SO acc id as alt_id
            for( Term term2: ontologyDAO.getTermsBySynonym("SO", soAccId, "exact") ) {
                GlobalCounters.getInstance().incrementCounter("VARIANTS_WITH_SUBSTITUTE_SO_ACC_ID", 1);
                term = term2;
                break;
            }

            // 3. hard-coded substitutions
            if( soAccId.equals("SO:1000184") ) {
                soAccId = "SO:0001572"; // replace 'sequence_variant_causes_exon_loss' with 'exon_loss_variant'
                GlobalCounters.getInstance().incrementCounter("VARIANTS_WITH_SUBSTITUTE_SO_ACC_ID", 1);
                return validateSoAccId(soAccId);
            }
        }

        if( term==null || term.isObsolete() )
            throw new Exception("problematic so acc id: "+soAccId);
        return soAccId;
    }

    synchronized public List<Term> getRdoTermsBySynonym(String accId) throws Exception {
        // try the cache first
        List<Term> rdoTerms = _rdoTermsCache.get(accId);
        if( rdoTerms==null ) {
            // not in cache - query db
            rdoTerms = ontologyDAO.getTermsBySynonym("RDO", accId, "exact");
            Iterator<Term> it = rdoTerms.iterator();
            while( it.hasNext() ) {
                // skip obsolete terms
                Term term = it.next();
                if( term.isObsolete() )
                    it.remove();
                else
                    term.setComment(accId);
            }
            _rdoTermsCache.put(accId, rdoTerms);
        }
        return rdoTerms;
    }

    static HashMap<String, List<Term>> _rdoTermsCache = new HashMap<>();

    /**
     * get all active terms in given ontology
     * @param ontologyId ontology id
     * @return List of Term objects
     * @throws Exception if something wrong happens in spring framework
     */
    public List<Term> getActiveTerms(String ontologyId) throws Exception {
        return ontologyDAO.getActiveTerms(ontologyId);
    }

    /**
     * get synonyms for all active terms in given ontology
     * @param ontologyId ontology id
     * @return List of TermSynonym objects
     * @throws Exception if something wrong happens in spring framework
     */
    public List<TermSynonym> getActiveSynonyms(String ontologyId) throws Exception {
        return ontologyDAO.getActiveSynonyms(ontologyId);
    }

    /**
     * get an ontology term given term accession id;
     * return null if accession id is invalid
     * @param termAcc term accession id
     * @return Term object if given term found in database or null otherwise
     * @throws Exception if something wrong happens in spring framework
     */
    public TermWithStats getTermByAccId(String termAcc) throws Exception {
        return ontologyDAO.getTermWithStatsCached(termAcc, null);
    }

    public boolean isDescendantOf(String termAcc, String ancestorTermAcc) throws Exception {
        return ontologyDAO.isDescendantOf(termAcc, ancestorTermAcc);
    }

    // =========== ANNOTATIONS =================

    public int getAnnotationKey(Annotation annot) throws Exception {
        return annotationDAO.getAnnotationKey(annot);
    }

    public Annotation getAnnotation(int annotKey) throws Exception {
        return annotationDAO.getAnnotation(annotKey);
    }

    public void updateLastModifiedDateForAnnotation(int fullAnnotKey, int lastModifiedBy) throws Exception {
        annotationDAO.updateLastModified(fullAnnotKey, lastModifiedBy);
    }

    public int updateLastModified(List<Integer> fullAnnotKeys) throws Exception{
        return annotationDAO.updateLastModified(fullAnnotKeys);
    }

    public void updateAnnotation(Annotation annot) throws Exception {
        annotationDAO.updateAnnotation(annot);
    }

    public void insertAnnotation(Annotation annot) throws Exception {
        annotationDAO.insertAnnotation(annot);

        logAnnotations.debug("INSERT "+annot.dump("|"));
    }

    public int getCountOfAnnotationsByReference(int refRgdId, String dataSource, String aspect) throws Exception {
        return annotationDAO.getCountOfAnnotationsByReference(refRgdId, dataSource, aspect);
    }

    public int getAnnotationCount(int rgdId, String termAcc, String qualifier, int refRgdId) throws Exception {

        String key = rgdId+"|"+termAcc+"|"+qualifier;
        Integer cnt = _annotCache2.get(key);
        if( cnt!=null ) {
            return cnt;
        }

        List<Annotation> annots = annotationDAO.getAnnotations(rgdId, termAcc);
        Iterator<Annotation> it = annots.iterator();
        while( it.hasNext() ) {
            Annotation a = it.next();
            if( refRgdId==a.getRefRgdId() ) {
                it.remove();
                continue;
            }
            if( !Utils.stringsAreEqual(qualifier, a.getQualifier()) ) {
                it.remove();
            }
        }
        _annotCache2.put(key, annots.size());
        return annots.size();
    }
    static ConcurrentHashMap<String, Integer> _annotCache2 = new ConcurrentHashMap<>();

    public int deleteObsoleteAnnotations(int createdBy, Date dt, String staleAnnotDeleteThresholdStr, int refRgdId, String dataSource, int origAnnotCount, CounterPool counters, String aspect) throws Exception{

        // convert delete-threshold string to number; i.e. '5%' --> '5'
        int staleAnnotDeleteThresholdPerc = Integer.parseInt(staleAnnotDeleteThresholdStr.substring(0, staleAnnotDeleteThresholdStr.length()-1));
        // compute maximum allowed number of stale annots to be deleted
        int annotCount = getCountOfAnnotationsByReference(refRgdId, dataSource, aspect);
        int staleAnnotDeleteLimit = (staleAnnotDeleteThresholdPerc * origAnnotCount) / 100;

        List<Annotation> staleAnnots = annotationDAO.getAnnotationsModifiedBeforeTimestamp(createdBy, dt, aspect);

        logAnnotator.info(aspect+" total annotation count: "+annotCount);
        logAnnotator.info(aspect+" stale annotation delete limit ("+staleAnnotDeleteThresholdStr+"): "+staleAnnotDeleteLimit);
        logAnnotator.info(aspect+"stale annotations to be deleted: "+staleAnnots.size());

        int newAnnotCount = annotCount - staleAnnots.size();
        int annotDiffCount = newAnnotCount - origAnnotCount;
        if( annotDiffCount<0 && annotDiffCount+staleAnnotDeleteLimit<0 ) {
            logAnnotator.info("*** DELETE of stale annots aborted! *** "+staleAnnotDeleteThresholdStr+" delete threshold exceeded!");
            return 0;
        }

        List<Integer> staleAnnotKeys = new ArrayList<>();
        for( Annotation ann: staleAnnots ) {
            logAnnotations.debug("DELETE "+ann.dump("|"));
            staleAnnotKeys.add(ann.getKey());

            if( ann.getRgdObjectKey()==1 ) {
                counters.increment(aspect+" annotations - gene - ALL SPECIES - deleted");
                switch(ann.getSpeciesTypeKey()) {
                    case 1: counters.increment(aspect+" annotations - gene - rat - deleted"); break;
                    case 2: counters.increment(aspect+" annotations - gene - mouse - deleted"); break;
                    case 3: counters.increment(aspect+" annotations - gene - human - deleted"); break;
                    default: counters.increment(aspect+" annotations - gene - other - deleted"); break;
                }
            } else {
                counters.increment(aspect+" annotations - variant - deleted");
            }
        }
        return annotationDAO.deleteAnnotations(staleAnnotKeys);
    }

    public List<VariantMapData> getVariantByRgdId(int rgdId) throws Exception{
        return vdao.getVariantsByRgdId(rgdId);
    }

    public void updateVariantRsID(List<VariantMapData> mapsData) throws Exception {
        BatchSqlUpdate sql2 = new BatchSqlUpdate(this.vdao.getDataSource(),
                "update variant set RS_ID=? where RGD_ID=?",
                new int[]{Types.VARCHAR,Types.INTEGER});
        sql2.compile();
        for( VariantMapData v: mapsData) {
            long id = v.getId();
            sql2.update(v.getRsId(),id);
        }
        sql2.flush();
    }

    public void setDeleteThresholdForStaleXdbIds(String deleteThresholdForStaleXdbIds) {
        this.deleteThresholdForStaleXdbIds = deleteThresholdForStaleXdbIds;
    }

    public String getDeleteThresholdForStaleXdbIds() {
        return deleteThresholdForStaleXdbIds;
    }
}
