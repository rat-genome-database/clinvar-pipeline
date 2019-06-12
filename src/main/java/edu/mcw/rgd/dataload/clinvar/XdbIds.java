package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.XdbId;
import edu.mcw.rgd.process.Utils;

import java.util.*;

/**
 * represents external database identifiers associated with given variant
 */
public class XdbIds {

    private List<XdbId> incomingXdbIds = new ArrayList<>();
    private List<XdbId> inRgdXdbIds;

    private List<XdbId> insXdbIds = new ArrayList<>();
    private List<XdbId> updXdbIds = new ArrayList<>();

    boolean addIncomingXdbId(int xdbKey, String accId, String clinVarId) {

        if( Utils.isStringEmpty(accId) )
            return false;

        // fixup for acc ids
        if( xdbKey==XdbId.XDB_KEY_PUBMED ) {
            // validate PMID accession id: strip non-digit characters
            accId = accId.replaceAll("\\D", "");
        }

        for( XdbId xdbId: this.incomingXdbIds ) {
            if( xdbId.getXdbKey()==xdbKey && xdbId.getAccId().equals(accId) )
                return false; // duplicate detected!
        }

        XdbId xdbId = new XdbId();
        xdbId.setAccId(accId);
        xdbId.setXdbKey(xdbKey);
        xdbId.setSrcPipeline(Manager.SOURCE);
        xdbId.setNotes(clinVarId);

        // fixup for some xdb keys
        if( xdbKey==48 ) {
            xdbId.setLinkText("rs"+accId);
        } else if( xdbKey==53 ) { // omim allele: replace '.' with '#' to have a working link to Omim allele
            xdbId.setAccId(accId.replace('.','#'));
            xdbId.setLinkText(accId);
        }

        this.incomingXdbIds.add(xdbId);
        return true; // added
    }

    String getClinVarId() {
        for( XdbId xdbId: incomingXdbIds ) {
            if( xdbId.getXdbKey()==52 )
                return xdbId.getAccId();
        }
        return null;
    }

    Set<String> getClinVarIds() {
        Set<String> clinVarIds = new HashSet<>();
        for( XdbId xdbId: inRgdXdbIds ) {
            if( xdbId.getXdbKey()==52 ) {
                clinVarIds.add(xdbId.getAccId());
            }
        }
        for( XdbId xdbId: insXdbIds ) {
            if( xdbId.getXdbKey()==52 ) {
                clinVarIds.add(xdbId.getAccId());
            }
        }
        return clinVarIds;
    }

    /**
     * perform qc
     * @param dao
     * @throws Exception
     */
    public void qc(int varRgdId, Dao dao) throws Exception {

        // all GeneIds must have their link-text set to gene symbol, if available
        updateLinkText(dao);

        // load in-rgd xdb ids
        if( varRgdId!=0 )
            inRgdXdbIds = dao.getXdbIds(varRgdId);
        else
            inRgdXdbIds = Collections.emptyList();
        List<XdbId> inRgdIds = new ArrayList<>(inRgdXdbIds);

        // determine which xdb ids must be inserted
        for( XdbId id: incomingXdbIds ) {
            if( isIncomingXdbIdInRgd(id, inRgdIds) )
                updXdbIds.add(id);
            else
                insXdbIds.add(id);
        }

        if( !insXdbIds.isEmpty() ) {
            GlobalCounters.getInstance().incrementCounter("XDB_IDS_INSERTED", insXdbIds.size());
        }
        GlobalCounters.getInstance().incrementCounter("XDB_IDS_UPDATED_LAST_MODIFIED_DATE", updXdbIds.size());
    }

    boolean isIncomingXdbIdInRgd(XdbId id, List<XdbId> inRgdIds) {

        Iterator<XdbId> it = inRgdIds.iterator();
        while( it.hasNext() ) {
            XdbId xdbId = it.next();
            if( xdbId.getXdbKey()==id.getXdbKey() && xdbId.getAccId().equals(id.getAccId()) ) {
                id.setKey(xdbId.getKey()); // transfer KEY to incoming xdb id
                it.remove();
                return true;
            }
        }
        return false;
    }
    /**
     * sync incoming xdb ids with RGD database
     */
    public void sync(int variantRgdId, Dao dao) throws Exception {

        if( !insXdbIds.isEmpty() ) {
            // set rgd_id for all to-be-inserted xdb ids
            for( XdbId xdbId: insXdbIds ) {
                xdbId.setRgdId(variantRgdId);
            }

            dao.insertXdbIds(insXdbIds);
        }

        if( !updXdbIds.isEmpty() ) {
            dao.updateXdbIds(updXdbIds);
        }
    }

    void updateLinkText(Dao dao) throws Exception {
        for( XdbId xdbId: incomingXdbIds ) {
            if( xdbId.getXdbKey()==XdbId.XDB_KEY_NCBI_GENE ) {
                for(Gene gene: dao.getHumanGenesByGeneId(xdbId.getAccId())) {
                    xdbId.setLinkText(gene.getSymbol());
                    break;
                }
            }
        }
    }
}
