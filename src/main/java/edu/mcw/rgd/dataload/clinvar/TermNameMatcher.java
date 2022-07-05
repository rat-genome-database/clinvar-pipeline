package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.datamodel.ontologyx.TermSynonym;
import edu.mcw.rgd.datamodel.ontologyx.TermWithStats;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * @author mtutaj
 * @since 10/16/14
 */
public class TermNameMatcher {

    Logger log = LogManager.getLogger("duplicates");

    Map<String, Set<String>> map = new HashMap<>();

    String ontId;

    public TermNameMatcher(String ontId) {
        this.ontId = ontId;
    }

    public void indexTermsAndSynonyms(Dao dao) throws Exception {
        indexTerms(dao);
        indexSynonyms(dao);
    }

    public void indexTerms(Dao dao) throws Exception {

        Map<String,String> duplicates = new HashMap<>();

        log.info("DUPLICATES {count of annotations}");

        for( Term t: dao.getActiveTerms(ontId) ) {
            // sanity check
            if( t.getTerm() == null ) {
                log.warn("ERROR: No term name for "+t.getAccId());
            }

            String normalizedName = normalizeTerm(t.getTerm());
            Collection<String> accIds = map.get(normalizedName);
            if( accIds==null ) {
                addTermAccId(normalizedName, t.getAccId());
                continue;
            }

            if( accIds.size()>1 ) {
                accIds = new ArrayList<>(accIds);
            }

            for( String accId: accIds ) {
                if( accId.equals(t.getAccId()) )
                    continue;

                // DUPLICATE: one term name appears for two terms
                String higherRankedTerm = pickHigherRankedTerm(accId, t.getAccId(), dao);
                addTermAccId(normalizedName, higherRankedTerm);

                addDuplicate(t.getTerm(), accId, t.getAccId(), duplicates, dao, "");
            }
        }

        // dump duplicate terms only for RDO (for HPO they are not very useful)
        if( ontId.equals("RDO") ) {
            dumpDuplicates(" DUPLICATE TERMS: ", duplicates);
        }
        log.info("");
    }

    public void indexSynonyms(Dao dao) throws Exception {

        Map<String,String> duplicates = new HashMap<>();

        for( TermSynonym s: dao.getActiveSynonyms(ontId) ) {
            String normalizedName = normalizeTerm(s.getName());
            Collection<String> accIds = map.get(normalizedName);
            if( accIds==null ) {
                addTermAccId(normalizedName, s.getTermAcc());
                continue;
            }

            if( accIds.size()>1 ) {
                accIds = new ArrayList<>(accIds);
            }

            for( String accId: accIds ) {
                if( accId.equals(s.getTermAcc()) )
                    continue;

                // DUPLICATE: one synonyms appears for two terms
                if( s.getType().equals("narrow_synonym") || s.getType().equals("broad_synonym") ) {
                    // if synonym is narrow or broad, skip it, and use the term that is already in the map
                    // because the more likely more specific term that is in the map is more accurate
                    continue;
                }

                String extraInfo;
                // synonym is not narrow/broad, see if terms are on separate ontology branches
                if( termsOnSeparateOntBranches(accId, s.getTermAcc(), dao) ) {
                    // yes, terms are on separate term branches
                    // add both accession ids
                    addTermAccId(normalizedName, accId);
                    addTermAccId(normalizedName, s.getTermAcc());
                    extraInfo = "g2 "; // separate ontology branches
                } else {

                    // synonym is not narrow/broad, terms are on same ontology branch: pick higher ranked term
                    String pickedTerm = pickHigherRankedTerm(accId, s.getTermAcc(), dao);
                    addTermAccId(normalizedName, pickedTerm);
                    extraInfo = "g1 "; // same ontology branch
                }

                addDuplicate(s.getName(), accId, s.getTermAcc(), duplicates, dao, extraInfo);
            }
        }

        // dump duplicate synonyms only for RDO (not very useful for HPO)
        if( ontId.equals("RDO") ) {
            dumpDuplicates(" DUPLICATE SYNONYMS: ", duplicates);
        }
        log.info("");
    }

    boolean termsOnSeparateOntBranches(String acc1, String acc2, Dao dao) throws Exception {

        return !dao.isDescendantOf(acc1, acc2)
           &&  !dao.isDescendantOf(acc2, acc1);
    }

    String normalizeTerm(String term) {
        // arg validation check
        if( term == null ) {
            return "";
        }

        // special handling for terms RDO:0012607 and RDO:0012696, that are falsely reported as duplicates
        if( term.contains("T Cell-") && term.contains("B Cell-") && term.contains("NK Cell-") ) {
            term = term.replace("T Cell-","TCell").replace("B Cell-","BCell").replace("NK Cell-","NKCell");
        }

        String[] words = term.replace('-',' ').replace(',',' ').replace('(',' ').replace(')',' ').replace('/',' ')
                .toLowerCase().split("[\\s]");
        Arrays.sort(words);
        return Utils.concatenate(Arrays.asList(words), ".");
    }

    void addDuplicate(String synonym, String acc1, String acc2, Map<String,String> duplicates, Dao dao,
                      String extraInfo) throws Exception {
        TermWithStats ts1 = dao.getTermByAccId(acc1);
        TermWithStats ts2 = dao.getTermByAccId(acc2);
        int annotCount1 = ts1.getAnnotObjectCountForTermAndChildren();
        int annotCount2 = ts2.getAnnotObjectCountForTermAndChildren();
        duplicates.put(synonym, extraInfo+acc1+"{"+annotCount1+"} "+acc2+"{"+annotCount2+"}");
    }

    void dumpDuplicates(String title, Map<String,String> duplicates) {

        // remove 1-, 2- or 3-char long synonyms, all capital letters from the list of duplicates
        Map<String,String> duplicates2 = new HashMap<>(duplicates.size());
        for( Map.Entry<String, String> entry: duplicates.entrySet() ) {
            String syn = entry.getKey();
            if( syn.length()<=3 && syn.toUpperCase().equals(syn) ) {
                // skip it: up to 3 char long synonym, all uppercase characters
                continue;
            }
            duplicates2.put(syn, entry.getValue());
        }
        duplicates.clear();

        log.info(duplicates2.size()+title);

        // display additional info what g1/g2 means
        if( duplicates2.size()>10 ) {
            log.info("  g1 - conflicting terms on same ontology branch, one better annotated/more general term will be annotated");
            log.info("  g2 - conflicting terms on separate ontology branches, both terms will be annotated");
        }

        for( Map.Entry<String, String> entry: duplicates2.entrySet() ) {
            log.info("  ["+entry.getKey()+"]: "+entry.getValue());
        }
    }

    /**
     * higher ranked term is the term that is higher in the ontology tree
     * heuristics to implement that:
     * 1. having higher number rat genes annotated to the term and its children
     * 2. having higher number of child terms
     * 3. having lower number of parent terms
     * @param acc1 accession id for first term
     * @param acc2 accession id for second term
     * @return higher ranked term
     */
    String pickHigherRankedTerm(String acc1, String acc2, Dao dao) throws Exception {
        TermWithStats t1 = dao.getTermByAccId(acc1);
        TermWithStats t2 = dao.getTermByAccId(acc2);

        // 1. having higher number rat genes annotated to the term and its children
        int r = t1.getRatGeneCountForTermAndChildren() - t2.getRatGeneCountForTermAndChildren();
        if( r==0 ) {
            // 2. having higher number of child terms
            r = t1.getChildTermCount() - t2.getChildTermCount();
            if( r==0 ) {
                // 3. having lower number of parent terms
                r = t2.getParentTermCount() - t1.getParentTermCount();
            }
        }
        return r>0 ? acc1 : acc2;
    }

    public Set<String> getTermAccIds(String termName) {
        return map.get(normalizeTerm(termName));
    }

    boolean addTermAccId(String normalizedTermName, String value) {
        Set<String> set = map.get(normalizedTermName);
        if( set==null ) {
            set = new HashSet<>();
            map.put(normalizedTermName, set);
        }
        return set.add(value);
    }
}
