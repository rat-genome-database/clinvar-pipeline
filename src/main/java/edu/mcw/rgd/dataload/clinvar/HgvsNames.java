package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.HgvsName;
import edu.mcw.rgd.process.Utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author mtutaj
 * @since 3/12/14
 * helper class to handle hgvs names
 */
public class HgvsNames {

    private List<HgvsName> incomingNames = new ArrayList<HgvsName>();
    private List<HgvsName> matchingNames = new ArrayList<HgvsName>();
    private List<HgvsName> forInsertNames = new ArrayList<HgvsName>();
    private List<HgvsName> forDeleteNames;

    public void addIncomingHgvsName(String type, String name) {
        HgvsName hgvsName = new HgvsName();
        hgvsName.setType(type);
        hgvsName.setName(name);
        incomingNames.add(hgvsName);
    }

    public void qc(int varRgdId, Dao dao) throws Exception {

        List<HgvsName> inRgdNames = dao.getHgvsNames(varRgdId);
        List<HgvsName> inRgdNamesCopy = new ArrayList<HgvsName>(inRgdNames);

        for( HgvsName hgvsName: incomingNames ) {
            HgvsName matchingHgvsName = detach(inRgdNamesCopy, hgvsName);
            if( matchingHgvsName!=null ) {
                // matches RGD
                matchingNames.add(matchingHgvsName);
            }
            else {
                // no match with RGD -- insert it
                forInsertNames.add(hgvsName);
            }
        }

        forDeleteNames = inRgdNamesCopy;
    }

    private HgvsName detach(List<HgvsName> hgvsNames, HgvsName hgvsName) {

        Iterator<HgvsName> it = hgvsNames.iterator();
        while( it.hasNext() ) {
            HgvsName hgvsNameMatch = it.next();
            if( hgvsName.equalsByValue(hgvsNameMatch) ) {
                it.remove();
                return hgvsNameMatch;
            }
        }
        return null;
    }

    public void sync(int varRgdId, Dao dao) throws Exception {

        if( !matchingNames.isEmpty() ) {
            GlobalCounters.getInstance().incrementCounter("HGVS_NAME_MATCHED", matchingNames.size());
        }

        if( !forInsertNames.isEmpty() ) {
            for( HgvsName hgvsName: forInsertNames ) {
                hgvsName.setRgdId(varRgdId);
            }

            dao.insertHgvsNames(forInsertNames);
            GlobalCounters.getInstance().incrementCounter("HGVS_NAME_INSERTED", forInsertNames.size());
        }

        if( !forDeleteNames.isEmpty() ) {
            dao.deleteHgvsNames(forDeleteNames);
            GlobalCounters.getInstance().incrementCounter("HGVS_NAME_DELETED", forDeleteNames.size());
        }
    }
}
