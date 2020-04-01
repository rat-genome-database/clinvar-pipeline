package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
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
    private Logger logDebug = Logger.getLogger("dbg");
    private Logger log = Logger.getLogger("loader");

    public void parse(String fileName) throws IOException {

        splitInputFileIntoChunks(fileName);

        // process in parallel all of the chunks
        chunks.parallelStream().forEach( chunk -> {
            logDebug.info("  processing "+chunk+ " active threads: "+Thread.activeCount());
            File file = new File(chunk);
            Parser parser = new Parser();
            parser.qc = qc;
            parser.loader = loader;
            parser.setValidate(false);
            try {
                parser.parse(file);
            } catch (Exception e) {
                log.warn("*** problem parsing file "+chunk);
                throw new RuntimeException(e);
            }
            logDebug.info("  done with "+chunk+ " active threads: "+Thread.activeCount());
        });
    }

    void splitInputFileIntoChunks(String fileName) throws IOException {

        BufferedReader reader = Utils.openReader(fileName);

        int chunkSize = 0;
        BufferedWriter out = startNewChunk();

        String line;
        while( (line=reader.readLine())!=null ) {
            chunkSize += line.length() + 1;

            if( chunkSize < getChunkSize() ) {
                writeLine(line, out);
            } else {
                // chunk is full: read the lines until end of record is found
                String line2 = line.trim();
                while( !line2.startsWith(getRecordEnd()) ) {
                    writeLine(line, out);
                    line = reader.readLine();
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
    }

    void writeLine(String line, BufferedWriter out) throws IOException {
        out.write(line);
        out.write("\n");
    }

    BufferedWriter startNewChunk() throws IOException {
        String chunkName = getChunkDir() + "chunk" + chunks.size() + ".xml.gz";
        //BufferedWriter out = new BufferedWriter(new FileWriter(chunkName));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(chunkName))));
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
