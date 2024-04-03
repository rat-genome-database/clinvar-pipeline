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

                notesIncoming = handleNotes4000LimitForVarIncoming(notes, rgdId);
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

    static public String handleNotes4000LimitForVarIncoming( String notes, int rgdId ) {

        String newNotes = notes;
        int combinedNotesTooLong = 0;

        // ensure that the notes are no longer than 4000 characters
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

                String msg = "  combined notes too long for RGD:" + rgdId + "! UTF8 str len:" + (4+utf8Len);
                LogManager.getLogger("dbg").debug(msg);
                combinedNotesTooLong++;

                newNotes = (notes2 + " ...");
            } catch (UnsupportedEncodingException e) {
                // totally unexpected
                throw new RuntimeException(e);
            }
        }
        if( combinedNotesTooLong > 0 ) {
            GlobalCounters.getInstance().incrementCounter("NOTES_MERGED_DUE_TO_4000_ORACLE_LIMIT", 1);
        }
        return newNotes;
    }


    class NotesInfo {
        public String notesInRgd;
        public List<String> notesIncoming = new ArrayList<>();
    }

}
