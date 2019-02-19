package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.dao.impl.*;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.ontology.Annotation;
import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.datamodel.ontologyx.TermSynonym;
import edu.mcw.rgd.datamodel.ontologyx.TermWithStats;
import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.Map;

/**
 * @author 2/11/14
 * @since 2:42 PM
 * wrapper to centralize all code accessing database
 */
public class Dao {

    public static final String ASSOC_TYPE = "variant_to_gene";

    Logger logInsertedVariants = Logger.getLogger("insertedVariants");
    Logger logUpdatedVariants = Logger.getLogger("updatedVariants");
    Logger logGeneAssociations = Logger.getLogger("geneAssociations");
    Logger logMapPos = Logger.getLogger("mapPos");
    Logger logXdbIds = Logger.getLogger("xdbIds");
    Logger logHgvsNames = Logger.getLogger("hgvsNames");
    Logger logAliases = Logger.getLogger("aliases");
    Logger logAnnotations = Logger.getLogger("annotations");
    Logger logAnnotator = Logger.getLogger("annotator");

    private AliasDAO aliasDAO = new AliasDAO();
    private AnnotationDAO annotationDAO = new AnnotationDAO();
    private AssociationDAO associationDAO = new AssociationDAO();
    private GeneDAO geneDAO = associationDAO.getGeneDAO();
    private MapDAO mapDAO = new MapDAO();
    private OntologyXDAO ontologyDAO = new OntologyXDAO();
    private RGDManagementDAO rgdIdDAO = new RGDManagementDAO();
    private VariantInfoDAO variantInfoDAO = new VariantInfoDAO();
    private XdbIdDAO xdbIdDAO = new XdbIdDAO();


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
        RgdId rgdId = rgdIdDAO.createRgdId(RgdId.OBJECT_KEY_VARIANTS, "ACTIVE", Manager.SOURCE, SpeciesType.HUMAN);
        var.setRgdId(rgdId.getRgdId());
        var.setObjectKey(rgdId.getObjectKey());
        var.setSpeciesTypeKey(rgdId.getSpeciesTypeKey());
        int rowsAffected = 1 + variantInfoDAO.insertVariantInfo(var);
        logInsertedVariants.info(var.dump("|"));
        return rowsAffected;
    }

    /**
     * Update variant in tables GENOMIC_ELEMENTS,CLINVAR given rgdID
     *
     * @return count of rows affected
     * @throws Exception when unexpected error in spring framework occurs
     */
    public int updateVariant(VariantInfo varNew, VariantInfo varOld) throws Exception{

        varNew.setRgdId(varOld.getRgdId());

        logUpdatedVariants.info("OLD: "+varOld.dump("|"));
        logUpdatedVariants.info("NEW: "+varNew.dump("|"));
        return variantInfoDAO.updateVariant(varNew);
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
        logGeneAssociations.info("INSERT "+assoc.dump("|"));
        return assoc.getAssocKey();
    }

    public int deleteGeneAssociation(int variantRgdId, int geneRgdId) throws Exception {

        Association assoc = new Association();
        assoc.setAssocType(ASSOC_TYPE);
        assoc.setMasterRgdId(variantRgdId);
        assoc.setDetailRgdId(geneRgdId);
        assoc.setSrcPipeline(Manager.SOURCE);
        logGeneAssociations.info("DELETE "+assoc.dump("|"));

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

    public int deleteXdbIds(List<XdbId> xdbIds) throws Exception {
        for( XdbId id: xdbIds ) {
            logXdbIds.info("DELETE "+id.dump("|"));
        }
        return xdbIdDAO.deleteXdbIds(xdbIds);
    }

    public int insertXdbIds(List<XdbId> xdbIds) throws Exception {
        int rowsAffected = xdbIdDAO.insertXdbs(xdbIds);
        for( XdbId id: xdbIds ) {
            logXdbIds.info("INSERT "+id.dump("|"));
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
            logMapPos.info("UPDATE "+md.dump("|"));
        }
    }

    // must be synchronized unless MAPS_DATA table will start using sequences to insert new data
    synchronized public void insertMapData(List<MapData> mds) throws Exception {

        mapDAO.insertMapData(mds);

        for( MapData md: mds ) {
            logMapPos.info("INSERT "+md.dump("|"));
        }
    }

    public void deleteMapData(List<MapData> mds) throws Exception {

        mapDAO.deleteMapData(mds);

        for( MapData md: mds ) {
            logMapPos.info("DELETE "+md.dump("|"));
        }
    }

    // =========== HGVS NAMES =================

    public List<HgvsName> getHgvsNames(int rgdId) throws Exception {
        return variantInfoDAO.getHgvsNames(rgdId);
    }

    public int insertHgvsNames(List<HgvsName> hgvsNames) throws Exception {
        int rowCount = variantInfoDAO.insertHgvsNames(hgvsNames);
        for( HgvsName hgvsName: hgvsNames ) {
            logHgvsNames.info("INSERT "+hgvsName.dump("|"));
        }
        return rowCount;
    }

    public int deleteHgvsNames(List<HgvsName> hgvsNames) throws Exception {
        for( HgvsName hgvsName: hgvsNames ) {
            logHgvsNames.info("DELETE "+hgvsName.dump("|"));
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
            logAliases.info("INSERT "+alias.dump("|"));
        }
        return rowCount;
    }

    synchronized public int deleteAliases(List<Alias> aliases) throws Exception {
        for( Alias alias: aliases ) {
            logAliases.info("DELETE "+alias.dump("|"));
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

    public void updateLastModifiedDateForAnnotation(int fullAnnotKey, int lastModifiedBy) throws Exception {
        annotationDAO.updateLastModified(fullAnnotKey, lastModifiedBy);
    }

    public void insertAnnotation(Annotation annot) throws Exception {
        annotationDAO.insertAnnotation(annot);

        logAnnotations.info("INSERT "+annot.dump("|"));
    }

    /**
     * delete all pipeline annotations older than given date
     *
     * @return count of annotations deleted
     * @throws Exception on spring framework dao failure
     */
    public int deleteObsoleteAnnotations(int createdBy, Date dt, String staleAnnotDeleteThresholdStr, int refRgdId, String dataSource) throws Exception{

        // convert delete-threshold string to number; i.e. '5%' --> '5'
        int staleAnnotDeleteThresholdPerc = Integer.parseInt(staleAnnotDeleteThresholdStr.substring(0, staleAnnotDeleteThresholdStr.length()-1));
        // compute maximum allowed number of stale annots to be deleted
        int annotCount = annotationDAO.getCountOfAnnotationsByReference(refRgdId, dataSource, "D");
        int staleAnnotDeleteLimit = (staleAnnotDeleteThresholdPerc * annotCount) / 100;

        List<Annotation> staleAnnots = annotationDAO.getAnnotationsModifiedBeforeTimestamp(createdBy, dt, "D");

        logAnnotator.info("total annotation count: "+annotCount);
        logAnnotator.info("stale annotation delete limit ("+staleAnnotDeleteThresholdStr+"): "+staleAnnotDeleteLimit);
        logAnnotator.info("stale annotations to be deleted: "+staleAnnots.size());

        if( staleAnnots.size()> staleAnnotDeleteLimit ) {
            logAnnotator.info("*** DELETE of stale annots aborted! *** "+staleAnnotDeleteThresholdStr+" delete threshold exceeded!");
            return 0;
        }

        List<Integer> staleAnnotKeys = new ArrayList<>();
        for( Annotation ann: staleAnnots ) {
            logAnnotations.info("DELETE "+ann.dump("|"));
            staleAnnotKeys.add(ann.getKey());
        }
        return annotationDAO.deleteAnnotations(staleAnnotKeys);
    }
}
