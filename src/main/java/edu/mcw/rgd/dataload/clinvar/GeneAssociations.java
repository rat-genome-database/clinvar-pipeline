package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mtutaj
 * @since 2/12/14
 * convenience class to handle all logic connected to gene associations;
 * one variant can have multiple gene associations
 */
public class GeneAssociations {

    private List<String> geneIds = new ArrayList<String>();
    private List<String> geneSymbols = new ArrayList<String>();

    private List<Integer> incomingAssocGeneRgdIds = new ArrayList<Integer>();
    private List<Integer> inRgdAssocGeneRgdIds;

    public void add(String geneId, String geneSymbol) {
        if( geneId==null || geneSymbol==null ) {
            return;
        }
        if( geneId.isEmpty() || geneSymbol.isEmpty() ) {
            return;
        }
        geneIds.add(geneId);
        geneSymbols.add(geneSymbol);
    }

    /**
     * perform qc
     * @param dao
     * @throws Exception
     */
    public void qc(Dao dao) throws Exception {

        for( int i=0; i<geneIds.size(); i++ ) {
            String geneId = geneIds.get(i);
            String geneSymbol = geneSymbols.get(i);
            boolean match = false;

            // match a gene based on gene id
            List<Gene> genes = dao.getHumanGenesByGeneId(geneId);
            if( genes.size()==1 ) {
                match = true;
                incomingAssocGeneRgdIds.add(genes.get(0).getRgdId());
                GlobalCounters.getInstance().incrementCounter("GENES_MATCHED_BY_GENE_ID", 1);
            }
            else if( genes.size()>1 ) {
                GlobalCounters.getInstance().incrementCounter("GENES_MULTIS_BY_GENE_ID", 1);
            } else {
                GlobalCounters.getInstance().incrementCounter("GENES_NOMATCH_BY_GENE_ID", 1);
            }

            // if match by gene-id failed, try to match by gene symbol
            if( !match ) {
                List<Integer> geneRgdIds = dao.getGeneRgdIdsBySymbol(geneSymbol);
                if( geneRgdIds.size()==1 ) {
                    incomingAssocGeneRgdIds.add(geneRgdIds.get(0));
                    GlobalCounters.getInstance().incrementCounter("GENES_MATCHED_BY_GENE_SYMBOL", 1);
                }
                else if( geneRgdIds.size()>1 ) {
                    GlobalCounters.getInstance().incrementCounter("GENES_MULTIS_BY_GENE_SYMBOL", 1);
                } else {
                    GlobalCounters.getInstance().incrementCounter("GENES_NOMATCH_BY_GENE_SYMBOL", 1);
                }
            }
        }
    }

    /**
     * sync incoming gene associations with RGD database
     */
    public void sync(int variantRgdId, Dao dao) throws Exception {

        // load gene rgd id for gene associated with this variant
        inRgdAssocGeneRgdIds = dao.getAssociatedGenes(variantRgdId);

        // determine matching rgd ids
        List<Integer> arr = new ArrayList<Integer>(inRgdAssocGeneRgdIds);
        arr.retainAll(incomingAssocGeneRgdIds);
        GlobalCounters.getInstance().incrementCounter("GENE_VAR_ASSOC_MATCHED", arr.size());

        // determine new gene rgd ids
        arr = new ArrayList<Integer>(incomingAssocGeneRgdIds);
        arr.removeAll(inRgdAssocGeneRgdIds);
        for( Integer incomingAssocGeneRgdId: arr ) {
            dao.createGeneAssociation(variantRgdId, incomingAssocGeneRgdId);
            GlobalCounters.getInstance().incrementCounter("GENE_VAR_ASSOC_INSERTED", 1);
        }

        // determine obsolete gene rgd ids
        arr = new ArrayList<Integer>(inRgdAssocGeneRgdIds);
        arr.removeAll(incomingAssocGeneRgdIds);
        for( Integer inRgdAssocGeneRgdId: arr ) {
            dao.deleteGeneAssociation(variantRgdId, inRgdAssocGeneRgdId);
            GlobalCounters.getInstance().incrementCounter("GENE_VAR_ASSOC_DELETED", 1);
        }
    }
}
