package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.process.Utils;

import java.util.*;

/**
 * manages trait name updates for existing ClinVar variants
 */
public class TraitNameCollection {

    // THREAD SAFE SINGLETON -- start
    // private instance, so that it can be accessed by only by getInstance() method
    private static TraitNameCollection instance;

    private TraitNameCollection() {
        // private constructor
    }

    //synchronized method to control simultaneous access
    synchronized public static TraitNameCollection getInstance() {
        if (instance == null) {
            // if instance is null, initialize
            instance = new TraitNameCollection();
        }
        return instance;
    }
    // THREAD SAFE SINGLETON -- end


    // map of clinvar rgd id to trait name info
    private Map<Integer, TraitNameInfo> map = new HashMap<>();

    public synchronized void add(int rgdId, String traitNameIncoming, String traitNameInRgd) {
        TraitNameInfo info = map.get(rgdId);
        if( info==null ) {
            info = new TraitNameInfo();
            map.put(rgdId, info);
        }
        info.traitNamesIncoming.add(traitNameIncoming);
        if( info.traitNameInRgd!=null && !info.traitNameInRgd.equals(traitNameInRgd) ) {
            System.out.println("WARNING: trait name in RGD override RGD:"+rgdId+"  OLD ["+info.traitNameInRgd+"] NEW ["+traitNameInRgd+"]");
        }
        info.traitNameInRgd = traitNameInRgd;
    }

    public void qcAndLoad(Dao dao) {

        List<Integer> rgdIds = new ArrayList<>(map.keySet());

        rgdIds.parallelStream().forEach( rgdId -> {

            TraitNameInfo info = map.get(rgdId);

            Set<String> parts = new TreeSet<>();
            for( String incoming: info.traitNamesIncoming ) {
                Collections.addAll(parts, incoming.split("[\\|]"));
            }
            String traitNameIncoming = Utils.concatenate(parts, "|");

            if( Utils.stringsAreEqual(traitNameIncoming, info.traitNameInRgd) ) {
                GlobalCounters.getInstance().incrementCounter("TRAIT_NAMES_UNCHANGED", 1);
            } else {
                try {
                    dao.updateTraitName(rgdId, info.traitNameInRgd, traitNameIncoming);
                } catch(Exception e) {
                    throw new RuntimeException(e);
                }
                GlobalCounters.getInstance().incrementCounter("TRAIT_NAMES_MODIFIED", 1);
            }
        });
    }

    class TraitNameInfo {
        public String traitNameInRgd;
        public List<String> traitNamesIncoming = new ArrayList<>();
    }
}

