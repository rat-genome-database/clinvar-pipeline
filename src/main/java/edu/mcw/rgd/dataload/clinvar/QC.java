package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.VariantInfo;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * @author mtutaj
 * @since 2/11/14
 * qc data against database
 */
public class QC {

    Logger log = LogManager.getLogger("loader");

    private Dao dao;

    public Dao getDao() {
        return dao;
    }

    public void setDao(Dao dao) {
        this.dao = dao;
    }

    public void run(Record rec) throws Exception {

        VariantInfo var = dao.getVariantBySymbol(rec.getVarIncoming().getSymbol());
        rec.setVarInRgd(var);

        if( var!=null ) {

            VariantInfo var2 = rec.getVarIncoming();
            var2.setNotes(merge(var2.getNotes(), var.getNotes(), rec));
            int notesMergeCount = rec.handleNotes4000LimitForVarIncoming();
            if( notesMergeCount>0 ) {
                GlobalCounters.getInstance().incrementCounter("NOTES_MERGED_DUE_TO_4000_ORACLE_LIMIT", notesMergeCount);
            }

            // check if object type, name, source or so_acc_id changed
            if( !Utils.stringsAreEqual(var.getObjectType(), var2.getObjectType()) ||
                !Utils.stringsAreEqual(var.getName(), var2.getName()) ||
                !Utils.stringsAreEqual(var.getSoAccId(), var2.getSoAccId()) ||

                !Utils.stringsAreEqual(var.getNucleotideChange(), var2.getNucleotideChange()) ) {

                rec.setUpdateRecordFlag(true);
            }

            if( Utils.isStringEmpty(var2.getName()) ) {
                log.warn(" warning: empty name");
            }

            // age-of-onset could be combined from multiple RCV entries
            var2.setAgeOfOnset(merge(var2.getAgeOfOnset(), var.getAgeOfOnset(), rec));

            // clinical-significance could be combined from multiple RCV entries
            var2.setClinicalSignificance(mergeCS(var2.getClinicalSignificance(), var.getClinicalSignificance(), rec));

            // method-type could be combined from multiple RCV entries
            var2.setMethodType(merge(var2.getMethodType(), var.getMethodType(), rec));

            // molecular-consequence could be combined from multiple RCV entries
            var2.setMolecularConsequence(merge(var2.getMolecularConsequence(), var.getMolecularConsequence(), rec));

            // prevalence could be combined from multiple RCV entries
            var2.setPrevalence(merge(var2.getPrevalence(), var.getPrevalence(), rec));

            // review-status could be combined from multiple RCV entries
            var2.setReviewStatus(merge(var2.getReviewStatus(), var.getReviewStatus(), rec));

            TraitNameCollection.getInstance().add(var.getRgdId(), var2.getTraitName(), var.getTraitName());
            SubmitterCollection.getInstance().add(var.getRgdId(), var2.getSubmitter(), var.getSubmitter());

            // if incoming last-evaluated-date is newer, use it
            updateLastEvaluatedDate(var, var2);
        }

        rec.getGeneAssociations().qc(getDao());

        rec.getXdbIds().qc(var!=null ? var.getRgdId() : 0, getDao());

        rec.getMapPositions().qc(var!=null ? var.getRgdId() : 0, getDao());

        rec.getHgvsNames().qc(var!=null ? var.getRgdId() : 0, getDao());

        // NOTE: aliases QC must be performed *after* xdb ids QC
        rec.getAliases().qc(var!=null ? var.getRgdId() : 0, getDao(), rec.getClinVarId(), rec.getXdbIds().getClinVarIds());
    }

    void updateLastEvaluatedDate(VariantInfo varInRgd, VariantInfo varIncoming) {
        // if incoming last-evaluated-date is null, use the in-rgd last-evaluated-date
        if( varIncoming.getDateLastEvaluated()==null )
            varIncoming.setDateLastEvaluated(varInRgd.getDateLastEvaluated());
        // if incoming last-evaluated-date is not null
        else {
            // and in-rgd last-evaluated-date is null, use the incoming date
            if( varInRgd.getDateLastEvaluated()==null ) {
                // no-op
            } else if( varInRgd.getDateLastEvaluated().after(varIncoming.getDateLastEvaluated()) ) {
                // if in-rgd last-evaluated-date is more up-to-date, use it
                varIncoming.setDateLastEvaluated(varInRgd.getDateLastEvaluated());
            }
        }
    }

    String merge(String incoming, String inRgd, Record rec) {
        // case 1. incoming is null
        if( incoming==null )
            // this has no effect on in-rgd value
            return inRgd;

        // case 2. non-null incoming, non-null in-rgd
        if( inRgd!=null ) {
            // delete all occurrences of 'incoming' from 'inRgd', regardless of case
            String oldInRgd = inRgd;
            while( inRgd.toUpperCase().contains(incoming.toUpperCase()) ) {
                // yes, in-rgd, there is already an entry with different case: replace it
                int pos = inRgd.toUpperCase().indexOf(incoming.toUpperCase());
                inRgd = inRgd.substring(0, pos) + inRgd.substring(pos+incoming.length());
            }

            // in-rgd does not contain incoming value: merge in-rgd with incoming
            Set<String> inRgdSet = new TreeSet<String>(Arrays.asList(inRgd.split("[\\|]")));
            Collections.addAll(inRgdSet, incoming.split("\\|"));
            String newInRgd = Utils.concatenate(inRgdSet,"|");
            if( !oldInRgd.equals(newInRgd) ) {
                rec.setUpdateRecordFlag(true);
            }
            return newInRgd;
        }

        // case 3. non-null incoming, null in-rgd
        // use the incoming value
        rec.setUpdateRecordFlag(true);
        return incoming;
    }

    // merge for clinical significance
    String mergeCS(String incoming, String inRgd, Record rec) {
        // case 1. incoming is null
        if( incoming==null ) {
            // this has no effect on in-rgd value
            return inRgd;
        }

        // case 2. non-null incoming, non-null in-rgd
        if( inRgd!=null ) {

            Set<String> set = new TreeSet<>(new Comparator<String>(){
                public int compare(String o1, String o2) {
                    return getRank(o1) - getRank(o2);
                }

                int getRank(String clinicalSignificance) {
                    switch( clinicalSignificance ) {
                        case "pathogenic": return 0;
                        case "likely pathogenic": return 10;
                        case "risk factor": return 20;
                        case "association": return 30;
                        case "affects": return 35;
                        case "benign": return 40;
                        case "likely benign": return 50;
                        case "conflicting interpretations of pathogenicity": return 60;
                        case "drug response": return 70;
                        case "protective": return 80;
                        case "confers sensitivity": return 85;
                        case "uncertain significance": return 90;
                        case "conflicting data from submitters": return 100;
                        case "association not found": return 110;
                        case "other": return 120;
                        case "not provided": return 2000;
                        default:
                            log.warn("unhandled clinical significance: "+clinicalSignificance);

                            Logger dbg = LogManager.getLogger("dbg");
                            dbg.debug("unhandled clinical significance: "+clinicalSignificance);
                            dbg.debug("mergeCS INCOMING={"+incoming+"}  INRGD={"+inRgd+"}");
                            return 999;
                    }
                }
            });

            // case 2a. split 'incoming' and 'inRgd' by separators: ',', ';', '/' and '|'
            final String regex = "\\s*[,;|/]\\s*";
            String[] inRgdParts = inRgd.split(regex);
            set.addAll(Arrays.asList(inRgdParts));

            String[] incomingParts = incoming.split(regex);
            set.addAll(Arrays.asList(incomingParts));

            String clinicalSignificance = Utils.concatenate(set, "|");

            if( inRgd.equals(clinicalSignificance) ) {
                return inRgd; // in-rgd already contains the incoming value
            }

            // in-rgd does not contain incoming value: merge in-rgd with incoming
            rec.setUpdateRecordFlag(true);

            return clinicalSignificance;
        }

        // case 3. non-null incoming, null in-rgd
        // use the incoming value
        rec.setUpdateRecordFlag(true);
        return incoming;
    }

}
