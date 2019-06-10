package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.VariantInfo;
import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author mtutaj
 * @since 2/11/14
 * qc data against database
 */
public class QC {

    Logger log = Logger.getLogger("loader");

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
            // check if object type, name, source or so_acc_id changed
            VariantInfo var2 = rec.getVarIncoming();
            if( !Utils.stringsAreEqual(var.getObjectType(), var2.getObjectType()) ||
                !Utils.stringsAreEqual(var.getName(), var2.getName()) ||
                !Utils.stringsAreEqual(var.getSoAccId(), var2.getSoAccId()) ||
                !Utils.stringsAreEqual(var.getNotes(), var2.getNotes()) ||

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

            // submitter could be combined from multiple RCV entries
            var2.setSubmitter(merge(var2.getSubmitter(), var.getSubmitter(), rec));

            // traits could be combined from multiple RCV entries
            var2.setTraitName(merge(var2.getTraitName(), var.getTraitName(), rec));
            if( qcTraitName(var2) ) {
                // removed 'not provided' redundant entry from trait name
                // f.e.
                // Brugada syndrome 3 [RCV000019201]|not provided [RCV000058286]|Brugada syndrome [RCV000058286]
                // ==>
                // Brugada syndrome 3 [RCV000019201]|Brugada syndrome [RCV000058286]
                rec.setUpdateRecordFlag(true);
            }

            // if incoming last-evaluated-date is newer, use it
            updateLastEvaluatedDate(var, var2);
        }

        rec.getGeneAssociations().qc(getDao());

        rec.getXdbIds().qc(var!=null ? var.getRgdId() : 0, getDao());

        rec.getMapPositions().qc(var!=null ? var.getRgdId() : 0, getDao());

        rec.getHgvsNames().qc(var!=null ? var.getRgdId() : 0, getDao());

        rec.getAliases().qc(var!=null ? var.getRgdId() : 0, getDao());
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
            if( inRgd.contains(incoming) )
                return inRgd; // in-rgd already contains the incoming value

            // in-rgd does not contain incoming value: merge in-rgd with incoming
            Set<String> inRgdSet = new TreeSet<String>(Arrays.asList(inRgd.split("[\\|]")));
            int inRgdSetSize = inRgdSet.size();
            Collections.addAll(inRgdSet, incoming.split("\\|"));
            if( inRgdSet.size()>inRgdSetSize ) {
                // new value added
                rec.setUpdateRecordFlag(true);
                return Utils.concatenate(inRgdSet,"|");
            }
            else {
                return inRgd; // in-rgd already contains the incoming value
            }
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
                        case "other": return 110;
                        case "not provided": return 2000;
                        default:
                            log.warn("unhandled clinical significance: "+clinicalSignificance);
                            return 999;
                    }
                }
            });
            String[] inRgdParts = inRgd.split("[/]|[\\|]|\\s*,\\s*");
            set.addAll(Arrays.asList(inRgdParts));

            // case 2a. if 'incoming' contains ',' or '/', it must be split into multiple incoming tokens
            if( incoming.indexOf(',')>=0 || incoming.indexOf('/')>=0 ) {
                String[] incomingParts = incoming.split("[/]|\\s*,\\s*");
                set.addAll(Arrays.asList(incomingParts));
            } else {
                set.add(incoming);
            }

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

    // after a number of ClinVar updates, condition name could become like that one:
    // Brugada syndrome 3 [RCV000019201]|not provided [RCV000058286]|Brugada syndrome [RCV000058286]
    // you could see redundant 'not provided' for [RCV000058286]
    // we remove these 'not provided' redundant entries
    // return true if trait name changed and must be updated in db
    boolean qcTraitName(VariantInfo var) {
        String traitName = var.getTraitName();

        // remove from condition name "(1 patient)" and "(1 family)"
        boolean result = false;
        if( traitName.contains(" (1 family) ") ) {
            result = true;
            traitName = traitName.replace(" (1 family) ", " ");
            var.setTraitName(traitName);
        }
        if( traitName.contains(" (1 patient) ") ) {
            result = true;
            traitName = traitName.replace(" (1 patient) ", " ");
            var.setTraitName(traitName);
        }

        if( !traitName.contains("not provided") )
            return result;
        String[] conditions = var.getTraitName().split("\\|", -1);
        if( conditions.length==1 )
            return result;

        for( int i=0; i<conditions.length; i++ ) {
            String condition = conditions[i];
            // look for 'not provided' condition
            if( condition.startsWith("not provided") ) {
                // extract rcv
                int pos = condition.indexOf(" [RCV");
                String rcv = condition.substring(pos);

                // look for multiple conditions for same rcv
                for( int j=0; j<conditions.length; j++ ) {
                    if( i==j )
                        continue;
                    if( conditions[j].contains(rcv) ) {
                        // we found different condition with same rcv!
                        // remove 'not provided' duplicate
                        List<String> condList = new ArrayList<String>(conditions.length-1);
                        for( int k=0; k<conditions.length; k++ ) {
                            if( k!=i )
                                condList.add(conditions[k]);
                        }
                        traitName = Utils.concatenate(condList,"|");
                        var.setTraitName(traitName);
                        return true;
                    }
                }
            }
        }
        return result;
    }
}
