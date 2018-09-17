package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.VariantInfo;
import edu.mcw.rgd.process.Utils;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author mtutaj
 * @since 2/11/14
 * represents a line read from variant file
 */
public class Record {

    public int recno;
    private VariantInfo varIncoming;
    private VariantInfo varInRgd;
    private boolean updateRecordFlag;
    private GeneAssociations geneAssociations = new GeneAssociations();
    private XdbIds xdbIds = new XdbIds();
    private MapPositions mapPositions = new MapPositions();
    private HgvsNames hgvsNames = new HgvsNames();
    private Aliases aliases = new Aliases();
    private String variantAltName;

    public VariantInfo getVarIncoming() {
        return varIncoming;
    }

    public void setVarIncoming(VariantInfo varIncoming) {
        this.varIncoming = varIncoming;
    }

    public VariantInfo getVarInRgd() {
        return varInRgd;
    }

    public void setVarInRgd(VariantInfo varInRgd) {
        this.varInRgd = varInRgd;
    }

    public boolean isUpdateRecordFlag() {
        return updateRecordFlag;
    }

    public void setUpdateRecordFlag(boolean updateRecordFlag) {
        this.updateRecordFlag = updateRecordFlag;
    }

    public GeneAssociations getGeneAssociations() {
        return geneAssociations;
    }

    public XdbIds getXdbIds() {
        return xdbIds;
    }

    public MapPositions getMapPositions() {
        return mapPositions;
    }

    public HgvsNames getHgvsNames() {
        return hgvsNames;
    }

    public Aliases getAliases() {
        return aliases;
    }

    public String getClinVarId() {
        return xdbIds.getClinVarId();
    }

    public String getVariantAltName() {
        return variantAltName;
    }

    public void setVariantAltName(String variantAltName) {
        this.variantAltName = variantAltName;
    }

    public void mergeNotesForVarIncoming(String notes) {
        if( !notes.isEmpty() ) {
            getVarIncoming().setNotes(getVarIncoming().getNotes()!=null
                    ? getVarIncoming().getNotes() + "; " + notes
                    : notes
            );

            // ensure that the notes are no longer than 4000 characters
            notes = getVarIncoming().getNotes();
            if( notes.length()>4000 ) {
                System.out.println("combined notes too long! RGD_ID="+getVarIncoming().getRgdId());
                getVarIncoming().setNotes(notes.substring(0, 3996) + " ...");
            }
        }
    }

    public void mergeSubmitterForVarIncoming(String submitter) {
        if( !submitter.isEmpty() ) {
            String submitters = getVarIncoming().getSubmitter();
            if( submitters!=null ) {
                Set<String> set = new TreeSet<>(Arrays.asList(submitters.split("[\\|]")));
                if( set.add(submitter) ) {
                    getVarIncoming().setSubmitter(Utils.concatenate(set, "|"));
                }
            } else {
                getVarIncoming().setSubmitter(submitter);
            }
        }
    }
}
