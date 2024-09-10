package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.*;

public class NotesCollection {

    // THREAD SAFE SINGLETON -- start
    // private instance, so that it can be accessed by only by getInstance() method
    private static NotesCollection instance;

    private NotesCollection() {
        // private constructor
    }

    //synchronized method to control simultaneous access
    synchronized public static NotesCollection getInstance() {
        if (instance == null) {
            // if instance is null, initialize
            instance = new NotesCollection();
        }
        return instance;
    }
    // THREAD SAFE SINGLETON -- end


    // map of clinvar rgd id to notes info
    private Map<Integer, NotesCollection.NotesInfo> map = new HashMap<>();

    private Logger logDebug = LogManager.getLogger("dbg");

    public synchronized void add(int rgdId, String notesIncoming, String notesInRgd) {
        if( notesIncoming==null || notesIncoming.isBlank() ) {
            return;
        }

        NotesCollection.NotesInfo info = map.get(rgdId);
        if( info==null ) {
            info = new NotesCollection.NotesInfo();
            map.put(rgdId, info);
        }
        info.notesIncoming.add(notesIncoming);

        if( info.notesInRgd!=null && !info.notesInRgd.equals(notesInRgd) ) {
            logDebug.warn("WARNING: notes in RGD override RGD:"+rgdId+"  OLD ["+info.notesInRgd+"] NEW ["+notesInRgd+"]");
        }
        info.notesInRgd = notesInRgd;
    }

    public void qcAndLoad(Dao dao) {

        List<Integer> rgdIds = new ArrayList<>(map.keySet());

        rgdIds.parallelStream().forEach( rgdId -> {

            NotesCollection.NotesInfo info = map.get(rgdId);

            String notesIncoming = null;
            if( info.notesIncoming != null ) {
                Set<String> parts = new TreeSet<>();
                for (String incoming : info.notesIncoming) {
                    Collections.addAll(parts, incoming.split("[\\|]"));
                }
                String notes = Utils.concatenate(parts, "; ");

                notesIncoming = Manager.trimTo4000(notes, rgdId, "NOTES");
            }

            if( Utils.stringsAreEqual(notesIncoming, info.notesInRgd) ) {
                GlobalCounters.getInstance().incrementCounter("NOTES_UNCHANGED", 1);
            } else {
                try {
                    dao.updateNotes(rgdId, info.notesInRgd, notesIncoming);
                } catch(Exception e) {
                    throw new RuntimeException(e);
                }
                GlobalCounters.getInstance().incrementCounter("NOTES_MODIFIED", 1);
            }
        });
    }

    class NotesInfo {
        public String notesInRgd;
        public List<String> notesIncoming = new ArrayList<>();
    }

}
