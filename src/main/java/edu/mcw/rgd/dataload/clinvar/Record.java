package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.VariantInfo;
import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

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
        if( !notes.isEmpty() ) {
            getVarIncoming().setNotes(getVarIncoming().getNotes()!=null
                    ? getVarIncoming().getNotes() + "; " + notes
                    : notes
            );

            handleNotes4000LimitForVarIncoming();
        }
    }

    public void handleNotes4000LimitForVarIncoming() {

        // ensure that the notes are no longer than 4000 characters
        String notes = getVarIncoming().getNotes();
        if( notes!=null && notes.length() > 3980 ) {
            // take into account UTF8 encoding
            try {
                String notes2;
                int len = notes.length();
                if( len > 4000 ) {
                    len = 4000;
                }
                int utf8Len = 0;
                do {
                    notes2 = notes.substring(0, len);
                    len--;
                    utf8Len = notes2.getBytes("UTF-8").length;
                } while (utf8Len > 3996);

                // warn only once per ClinVar object about combined notes too long
                Logger log = Logger.getLogger("loader");
                String msg = "  combined notes too long for " + getVarIncoming().getSymbol() + "! UTF8 str len:" + (4+utf8Len);
                Object prevMsg = _combinedNotesTooLongMap.putIfAbsent(msg, "");
                if( prevMsg==null ) {
                    log.info(msg);
                }

                getVarIncoming().setNotes(notes2 + " ...");
            } catch (UnsupportedEncodingException e) {
                // totally unexpected
                throw new RuntimeException(e);
            }
        }
    }

    static ConcurrentHashMap<String,Object> _combinedNotesTooLongMap = new ConcurrentHashMap<>();

    public void mergeSubmitterForVarIncoming(String submitter) {
        if( submitter.isEmpty() ) {
            return;
        }
        if( submitter.endsWith(",") ) { // remove trailing ',' from submitter name
            submitter = submitter.substring(0, submitter.length()-1);
        }
        submitter = submitter.trim();

        if( !submitter.isEmpty() ) {
            String submitters = getVarIncoming().getSubmitter();
            if( submitters!=null ) {
                Set<String> set = new TreeSet<>();
                set.add(submitter);

                String[] submitterArray = submitters.split("[\\|]");
                Collections.addAll(set, submitterArray);

                getVarIncoming().setSubmitter(Utils.concatenate(set, "|"));
            } else {
                getVarIncoming().setSubmitter(submitter);
            }
        }
    }
}
