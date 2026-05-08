package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

/**
 * @author mtutaj
 * @since 11/20/2017
 * <ul>
 *     <li>split the incoming file into 'n' chunks</li>
 *     <li>run xml parser for every chunk simultaneously</li>
 * </ul>
 */
public class ParseGroup {

    public QC qc;
    public Loader loader;

    private int chunkSize;
    private String recordEnd;
    private String chunkHeader;
    private String chunkTrailer;
    private String chunkDir;

    private List<String> chunks = new ArrayList<>(); // names of file chunks
    private Logger logDebug = LogManager.getLogger("dbg");
    private Logger log = LogManager.getLogger("loader");

    private Boolean debug = true;
    private Boolean parallelProcessing = true;
    private Boolean reuseChunks = true;
    private int reuseChunksMinCount = 2000;
    private int reuseChunksMaxAgeHours = 24;

    public void dbgSetup() {

        chunks.clear();

        File dir = new File(chunkDir);
        File[] files = dir.listFiles(
            (dir1, name) -> name.startsWith("chunk") && name.endsWith(".xml.gz"));
        if( files != null ) {
            for( File f : files ) {
                chunks.add(f.getPath());
            }
        }
        Collections.shuffle(chunks);

        log.info(" CHUNKS TO BE PROCESSED: "+chunks.size());
    }

    static String formatHrMinSec(long ms) {
        long s = ms / 1000;
        return (s / 3600) + " hr " + ((s % 3600) / 60) + " min " + (s % 60) + " s";
    }

    /** If chunkDir has reuseChunksMinCount+ chunk files modified within the last
     *  reuseChunksMaxAgeHours hours AND newer than the source file (chronology: source
     *  must precede the chunks split from it), populate 'chunks' from them and return
     *  true so the (slow) re-split step is skipped. Disabled when reuseChunks=false. */
    boolean reuseRecentChunks(String sourceFile) {
        if( !Boolean.TRUE.equals(reuseChunks) ) return false;
        File dir = new File(chunkDir);
        if( !dir.isDirectory() ) return false;
        File[] files = dir.listFiles(
            (dir1, name) -> name.startsWith("chunk") && name.endsWith(".xml.gz"));
        if( files == null || files.length < reuseChunksMinCount ) return false;

        long sourceTs = sourceFile == null ? 0L : new File(sourceFile).lastModified();
        long cutoff = System.currentTimeMillis() - reuseChunksMaxAgeHours * 60L * 60 * 1000;
        List<String> recent = new ArrayList<>();
        for( File f : files ) {
            long ts = f.lastModified();
            if( ts >= cutoff && ts > sourceTs ) recent.add(f.getPath());
        }
        if( recent.size() < reuseChunksMinCount ) {
            if( sourceTs > 0 ) {
                log.info(" reuse-chunks skipped: only "+recent.size()+" chunks newer than source "+sourceFile);
            }
            return false;
        }

        chunks.addAll(recent);
        Collections.shuffle(chunks);
        log.info(" REUSING "+chunks.size()+" recent chunks (mtime within "+reuseChunksMaxAgeHours+"h, newer than source) from "+chunkDir);
        return true;
    }

    public void parse(String fileName) throws IOException {

        if( this.debug == false ) {
            splitInputFileIntoChunks(fileName);
        } else {
            dbgSetup();
        }

        // process in parallel all the chunks

        // note: when one worker threads is terminated due to RuntimeException,
        // other threads are still allowed to run :-)

        Stream<String> stream;
        if( this.parallelProcessing ) {
            stream = chunks.parallelStream();
        } else {
            stream = chunks.stream();
        }
        final int totalChunks = chunks.size();
        final java.util.concurrent.atomic.AtomicInteger chunkProgress = new java.util.concurrent.atomic.AtomicInteger();
        final long startMs = System.currentTimeMillis();
        stream.forEach( chunk -> {

            int n = chunkProgress.incrementAndGet();
            long elapsedMs = System.currentTimeMillis() - startMs;
            long etaMs = n > 0 ? (long)((totalChunks - n) * (double)elapsedMs / n) : 0;
            String msg = n+"/"+totalChunks+". processing "+chunk
                    +", threads "+Thread.activeCount()
                    +", time-to-finish "+formatHrMinSec(etaMs);
            logDebug.info(msg);
            System.out.println(msg);

            File file = new File(chunk);
            Parser parser = new Parser();
            parser.qc = qc;
            parser.loader = loader;
            parser.setValidate(false);
            try {
                parser.parse(file);
            } catch (InterruptedException e) {
                logDebug.warn("*** intercepted InterruptedException "+chunk);
            } catch (Exception e) {
                log.warn("*** problem parsing file "+chunk);
                parser.requestTermination();
                throw new RuntimeException(e);
            }
            logDebug.info("  done with "+chunk+ " active threads: "+Thread.activeCount());
        });
    }

    void splitInputFileIntoChunks(String fileName) throws IOException {

        // debug: one big file parsing
        if( getChunkSize()<=0 ) {
            chunks.add(fileName);
            return;
        }

        // reuse recently-produced chunks instead of re-splitting (chunk splitting is slow)
        if( reuseRecentChunks(fileName) ) {
            return;
        }

        BufferedReader reader = Utils.openReader(fileName);

        int chunkSize = 0;
        BufferedWriter out = startNewChunk();

        String line;
        while( (line=readLine(reader))!=null ) {

            chunkSize += line.length() + 1;

            if( chunkSize < getChunkSize() ) {
                writeLine(line, out);
            } else {
                // chunk is full: read the lines until end of record is found
                String line2 = line.trim();
                while( !line2.startsWith(getRecordEnd()) ) {
                    writeLine(line, out);
                    line = readLine(reader);
                    line2 = line.trim();
                    chunkSize += line.length() + 1;
                }
                // finish the record
                writeLine(line, out);
                // finish the root element
                out.write(getChunkTrailer());
                out.write('\n');
                out.close();
                chunkSize += 1 + getChunkTrailer().length();
                String chunkName = chunks.get(chunks.size()-1);
                logDebug.info("  "+chunkName+" written "+chunkSize);

                // start new chunk
                out = startNewChunk();
                out.write(getChunkHeader());
                chunkSize = getChunkHeader().length();
            }
        }

        // finish last chunk
        out.close();
        String chunkName = chunks.get(chunks.size()-1);
        logDebug.info("  "+chunkName+" written "+chunkSize);

        log.info(" input file split into "+chunks.size()+" chunks");

        // randomize chunks
        Collections.shuffle(chunks);
    }

    String readLine( BufferedReader reader ) throws IOException {

        String line = reader.readLine();
        if( line != null ) {

            // replaces minus sign 0xE28892 with 0x2D
            String line2 = line.replace("−", "-");

            line = line2;
        }
        return line;
    }

    void writeLine(String line, BufferedWriter out) throws IOException {
        out.write(line);
        out.write("\n");
    }

    BufferedWriter startNewChunk() throws IOException {
        String chunkName = getChunkDir() + "chunk" + chunks.size() + ".xml.gz";
        //BufferedWriter out = new BufferedWriter(new FileWriter(chunkName));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(chunkName)), "UTF8"));
        chunks.add(chunkName);
        return out;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setRecordEnd(String recordEnd) {
        this.recordEnd = recordEnd;
    }

    public String getRecordEnd() {
        return recordEnd;
    }

    public void setChunkHeader(String chunkHeader) {
        this.chunkHeader = chunkHeader;
    }

    public String getChunkHeader() {
        return chunkHeader;
    }

    public void setChunkTrailer(String chunkTrailer) {
        this.chunkTrailer = chunkTrailer;
    }

    public String getChunkTrailer() {
        return chunkTrailer;
    }

    public void setChunkDir(String chunkDir) {
        this.chunkDir = chunkDir;
    }

    public String getChunkDir() {
        return chunkDir;
    }

    public Boolean getDebug() {
        return debug;
    }

    public void setDebug(Boolean debug) {
        this.debug = debug;
    }

    public Boolean getParallelProcessing() {
        return parallelProcessing;
    }

    public void setParallelProcessing(Boolean parallelProcessing) {
        this.parallelProcessing = parallelProcessing;
    }

    public Boolean getReuseChunks() {
        return reuseChunks;
    }

    public void setReuseChunks(Boolean reuseChunks) {
        this.reuseChunks = reuseChunks;
    }

    public int getReuseChunksMinCount() {
        return reuseChunksMinCount;
    }

    public void setReuseChunksMinCount(int reuseChunksMinCount) {
        this.reuseChunksMinCount = reuseChunksMinCount;
    }

    public int getReuseChunksMaxAgeHours() {
        return reuseChunksMaxAgeHours;
    }

    public void setReuseChunksMaxAgeHours(int reuseChunksMaxAgeHours) {
        this.reuseChunksMaxAgeHours = reuseChunksMaxAgeHours;
    }
}
