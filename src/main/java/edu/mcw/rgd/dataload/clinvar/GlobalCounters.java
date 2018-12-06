package edu.mcw.rgd.dataload.clinvar;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author mtutaj
 * @since 2/12/14
 * global flags
 */
public class GlobalCounters {
    private static GlobalCounters ourInstance = new GlobalCounters();

    private Map<String, Integer> counters = new TreeMap<String, Integer>();

    public static GlobalCounters getInstance() {
        return ourInstance;
    }

    private GlobalCounters() {
    }

    synchronized public int incrementCounter(String counterName, int inc) {

        Integer val = counters.get(counterName);
        if( val==null )
            val = inc;
        else
            val += inc;
        counters.put(counterName, val);
        return val;
    }

    public String dump() {
        StringBuilder buf = new StringBuilder("COUNTERS:\n========\n");
        for( Map.Entry<String, Integer> entry: counters.entrySet() ) {
            buf.append(entry.getKey())
                    .append(" : ")
                    .append(entry.getValue())
                    .append("\n");
        }
        buf.append("=========\n");
        return buf.toString();
    }
}
