package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.process.CounterPool;

/**
 * @author mtutaj
 * @since 2/12/14
 * global flags
 */
public class GlobalCounters {

    private static GlobalCounters ourInstance = new GlobalCounters();

    private CounterPool counters = new CounterPool();

    public static GlobalCounters getInstance() {
        return ourInstance;
    }

    private GlobalCounters() {
    }

    synchronized public void incrementCounter(String counterName, int inc) {
        counters.add(counterName, inc);
    }

    public String dump() {
        return counters.dumpAlphabetically();
    }
}
