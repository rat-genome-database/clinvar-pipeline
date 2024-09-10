package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class SubmitterCollection {

    // THREAD SAFE SINGLETON -- start
    // private instance, so that it can be accessed by only by getInstance() method
    private static SubmitterCollection instance;

    private SubmitterCollection() {
        // private constructor
    }

    //synchronized method to control simultaneous access
    synchronized public static SubmitterCollection getInstance() {
        if (instance == null) {
            // if instance is null, initialize
            instance = new SubmitterCollection();
        }
        return instance;
    }
    // THREAD SAFE SINGLETON -- end


    // map of clinvar rgd id to submitter info
    private Map<Integer, SubmitterCollection.SubmitterInfo> map = new HashMap<>();

    private Logger logDebug = LogManager.getLogger("dbg");

    public synchronized void add(int rgdId, String submitterIncoming, String submitterInRgd) {
        SubmitterCollection.SubmitterInfo info = map.get(rgdId);
        if( info==null ) {
            info = new SubmitterCollection.SubmitterInfo();
            map.put(rgdId, info);
        }
        info.submittersIncoming.add(submitterIncoming);
        if( info.submitterInRgd!=null && !info.submitterInRgd.equals(submitterInRgd) ) {
            logDebug.warn("WARNING: submitter in RGD override RGD:"+rgdId+"  OLD ["+info.submitterInRgd+"] NEW ["+submitterInRgd+"]");
        }
        info.submitterInRgd = submitterInRgd;
    }

    public void qcAndLoad(Dao dao) {

        List<Integer> rgdIds = new ArrayList<>(map.keySet());

        rgdIds.parallelStream().forEach( rgdId -> {

            SubmitterCollection.SubmitterInfo info = map.get(rgdId);

            Set<String> parts = new TreeSet<>();
            for( String incoming: info.submittersIncoming ) {
                Collections.addAll(parts, incoming.split("[\\|]"));
            }
            String submitter = Utils.concatenate(parts, "|");
            String submitterIncoming = Manager.trimTo4000(submitter, rgdId, "SUBMITTERS");

            if( Utils.stringsAreEqual(submitterIncoming, info.submitterInRgd) ) {
                GlobalCounters.getInstance().incrementCounter("SUBMITTERS_UNCHANGED", 1);
            } else {
                try {
                    dao.updateSubmitter(rgdId, info.submitterInRgd, submitterIncoming);
                } catch(Exception e) {
                    throw new RuntimeException(e);
                }
                GlobalCounters.getInstance().incrementCounter("SUBMITTERS_MODIFIED", 1);
            }
        });
    }

    class SubmitterInfo {
        public String submitterInRgd;
        public List<String> submittersIncoming = new ArrayList<>();
    }

}
