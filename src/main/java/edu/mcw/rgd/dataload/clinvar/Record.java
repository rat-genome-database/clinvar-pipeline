package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.VariantInfo;
import edu.mcw.rgd.process.Utils;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author mtutaj
 * @since 2/11/14
 * represents a line read from variant file
 */
public class Record {

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

        if( notes!=null && !notes.isEmpty() ) {

            if( getVarIncoming().getNotes() == null ) {
                getVarIncoming().setNotes(notes);
            } else {
                // merging notes -- only if new
                getVarIncoming().setNotes( getVarIncoming().getNotes() + "|" + notes );
            }
        }
    }

    public void mergeSubmitterForVarIncoming(String submitter) {
        if( Utils.isStringEmpty(submitter) ) {
            return;
        }

        String submitterList = merge(submitter, getVarIncoming().getSubmitter());
        getVarIncoming().setSubmitter(submitterList);
    }

    public void mergeReviewStatusForVarIncoming(String reviewStatus) {
        String reviewStatusList = merge(reviewStatus, getVarIncoming().getReviewStatus());
        getVarIncoming().setReviewStatus(reviewStatusList);
    }

    public void mergeMethodTypeForVarIncoming(String methodType) {
        String resultList = merge(methodType, getVarIncoming().getMethodType());
        getVarIncoming().setMethodType(resultList);
    }

    public void mergeClinicalSignificanceForVarIncoming(String clinicalSignificance) {
        String resultList = merge(clinicalSignificance, getVarIncoming().getClinicalSignificance());
        getVarIncoming().setClinicalSignificance(resultList);
    }

    String merge(String value, String valueList) {
        if( value.isEmpty() ) {
            return valueList;
        }
        value = value.trim();

        String result = valueList;

        if( !value.isEmpty() ) {
            if( valueList!=null ) {
                Set<String> set = new TreeSet<>();
                set.add(value);

                String[] valueArray = valueList.split("[\\|]");
                Collections.addAll(set, valueArray);

                result = Utils.concatenate(set, "|");
            } else {
                result = value;
            }
        }

        return result;
    }
}
