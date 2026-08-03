package edu.mcw.rgd.dataload.clinvar;

import org.apache.logging.log4j.Logger;

/**
 * Implemented by every module Manager can run from the command line.
 * <p>
 * Each module writes to its own log file, so Manager uses the default logger to report
 * run-wide information -- currently the memory usage summary -- into the log of whichever
 * module actually ran, rather than into a single shared file.
 */
public interface ClinVarModule {

    /** the logger this module writes its summary output to */
    Logger getDefaultLogger();
}
