package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;
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

    private boolean dbg = false;

    public void dbgSetup() {

        chunks.clear();
        for( int i=0; i<=208; i++ ) {
            String fname = "/tmp/clinvar/chunk" + i + ".xml.gz";
            chunks.add(fname);
        }
        Collections.shuffle(chunks);

        System.out.println(" CHUNKS TO BE PROCESSED: "+chunks.size());
    }

    public void parse(String fileName) throws IOException {

        if( dbg == false ) {
            splitInputFileIntoChunks(fileName);
        } else {
            dbgSetup();
        }

        // process in parallel all of the chunks

        // note: when one worker threads is terminated due to RuntimeException,
        // other threads are still allowed to run :-)

        chunks.parallelStream().forEach( chunk -> {
            logDebug.info("  processing "+chunk+ " active threads: "+Thread.activeCount());
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
}
