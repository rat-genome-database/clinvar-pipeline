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

        // Fixups run before the duplicate check below, because that check compares the incoming acc
        // id against the acc ids already stored -- which are the fixed up ones. Rewriting the acc id
        // afterwards, as this used to do for OMIM alleles, made the check compare '606272.0005'
        // against the stored '606272#0005' and never match, so the same allele was added once per
        // occurrence. Those extra copies then had nothing left to match in RGD and were reported as
        // inserts that the database quietly refused, ~105,000 of them per run.
        String linkText = null;
        if( xdbKey==XdbId.XDB_KEY_PUBMED ) {
            // validate PMID accession id: strip non-digit characters
            accId = accId.replaceAll("\\D", "");
        } else if( xdbKey==48 ) {
            linkText = "rs"+accId;
        } else if( xdbKey==53 ) { // omim allele: replace '.' with '#' to have a working link to Omim allele
            linkText = accId;
            accId = accId.replace('.','#');
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
        if( linkText!=null ) {
            xdbId.setLinkText(linkText);
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

        // note: XDB_IDS_INSERTED is counted in sync(), from rows the database actually accepted
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
     * @return true if there were any changes
     */
    public boolean sync(int variantRgdId, Dao dao) throws Exception {

        int changes = 0;

        if( !insXdbIds.isEmpty() ) {
            // set rgd_id for all to-be-inserted xdb ids
            for( XdbId xdbId: insXdbIds ) {
                xdbId.setRgdId(variantRgdId);
            }

            // the insert is a no-op for a row that is already in RGD, so count what was really
            // written; anything refused is reported separately rather than inflating the insert
            // count, which is how ~105,000 phantom inserts a run went unnoticed
            int inserted = dao.insertXdbIds(insXdbIds);
            GlobalCounters.getInstance().incrementCounter("XDB_IDS_INSERTED", inserted);

            int refused = insXdbIds.size() - inserted;
            if( refused > 0 ) {
                GlobalCounters.getInstance().incrementCounter("XDB_IDS_INSERT_REFUSED_ALREADY_IN_RGD", refused);
            }

            changes++;
        }

        if( !updXdbIds.isEmpty() ) {
            dao.updateXdbIds(updXdbIds);
            changes++;
        }

        return changes!=0;
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
