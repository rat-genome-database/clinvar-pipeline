package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.process.MemoryMonitor;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * @author mtutaj
 * @since 2/11/14
 */
public class Manager implements ClinVarModule {

    public static final String SOURCE = "CLINVAR";

    Logger log = LogManager.getLogger("loader");

    @Override
    public Logger getDefaultLogger() {
        return log;
    }

    private String version;
    private Dao dao;
    private QC qc;
    private Loader loader;
    private Downloader downloader;
    private ParseGroup parser;

    public static void main(String[] args) throws Exception {

        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        Manager manager = (Manager) (bf.getBean("manager"));

        // parse cmd line parameters
        VariantAnnotator annotator = null;
        VariantRsId rsId = null;
        Clinvar2Vcf clinvar2Vcf = null;
        boolean runLoader = false;
        boolean qcDuplicateTerms = false;
        boolean qcDuplicateTermsAndSynonyms = false;

        for( String arg: args ) {
            switch (arg) {
                case "--annotate":
                    annotator = (VariantAnnotator) (bf.getBean("annotator"));
                    break;
                case "--load":
                    runLoader = true;
                    break;
                case "--qcDuplicateTerms":
                    qcDuplicateTerms = true;
                    break;
                case "--qcDuplicateTermsAndSynonyms":
                    qcDuplicateTermsAndSynonyms = true;
                    break;
                case "--addRsIds":
                    rsId = (VariantRsId) (bf.getBean("variantRsId"));
                    break;
                case "--clinvar2vcf":
                    clinvar2Vcf = (Clinvar2Vcf) (bf.getBean("clinvar2vcf"));
                    break;
            }
        }

        // memory is sampled for the whole run; the summary goes to the log of every module that ran,
        // and it is reported from a finally block so it survives a failure -- that is when it matters most
        List<ClinVarModule> modulesRun = new ArrayList<>();
        MemoryMonitor memoryMonitor = new MemoryMonitor();
        memoryMonitor.start();

        try {
            if( qcDuplicateTerms ) {
                TermNameMatcher matcher = new TermNameMatcher("RDO");
                modulesRun.add(matcher);
                matcher.indexTerms(manager.getDao());
            }

            if( qcDuplicateTermsAndSynonyms ) {
                TermNameMatcher matcher = new TermNameMatcher("RDO");
                modulesRun.add(matcher);
                matcher.indexTermsAndSynonyms(manager.getDao());
            }

            if( runLoader ) {
                modulesRun.add(manager);
                manager.run();
            }

            if( annotator!=null ) {
                modulesRun.add(annotator);
                annotator.run(manager.getDao());
            }

            if( rsId!=null ) {
                modulesRun.add(rsId);
                rsId.run();
            }

            if( clinvar2Vcf!=null ) {
                modulesRun.add(clinvar2Vcf);
                clinvar2Vcf.run();
            }
        }catch (Exception e) {
            if( runLoader ) {
                Utils.printStackTrace(e, manager.log);
            }
            if( annotator!=null ) {
                Utils.printStackTrace(e, annotator.log);
            }
            e.printStackTrace();
            throw e;
        } finally {
            memoryMonitor.stop();
            String memorySummary = memoryMonitor.getSummary();
            for( ClinVarModule module: modulesRun ) {
                module.getDefaultLogger().info(memorySummary);
            }
        }
    }

    public void run() throws Exception {

        long time0 = System.currentTimeMillis();

        int originalNotesLength = dao.getTotalNotesLength();
        GlobalCounters.getInstance().incrementCounter("NOTES_LENGTH__INITIAL", originalNotesLength);

        int originalXdbIdCount = getDao().getXdbIdCount();
        GlobalCounters.getInstance().incrementCounter("XDB_IDS_COUNT_INITIAL", originalXdbIdCount);

        log.info(getVersion());
        log.info(getDao().getConnectionInfo());

        qc.setDao(getDao());
        loader.setDao(getDao());

        String variantFileName = downloadVariantFile();

        parser.qc = qc;
        parser.loader = loader;
        parser.parse(variantFileName);

        Date staleXdbIdsCutoffDate = Utils.addDaysToDate(new Date(time0), -1);
        getDao().deleteStaleXdbIds(originalXdbIdCount, staleXdbIdsCutoffDate, log);

        int lastXdbIdCount = getDao().getXdbIdCount();
        GlobalCounters.getInstance().incrementCounter("XDB_IDS_ZCOUNT_FINAL", lastXdbIdCount);

        NotesCollection.getInstance().qcAndLoad(getDao());
        TraitNameCollection.getInstance().qcAndLoad(getDao());
        SubmitterCollection.getInstance().qcAndLoad(getDao());

        int finalNotesLength = dao.getTotalNotesLength();
        GlobalCounters.getInstance().incrementCounter("NOTES_LENGTH_FINAL", finalNotesLength);

        log.info(GlobalCounters.getInstance().dump());
        log.info("TOTAL ELAPSED TIME "+Utils.formatElapsedTime(time0, System.currentTimeMillis()));
    }

    /**
     * download variant file from ClinVar ftp site
     * @return file name of local copy of variant file
     * @throws Exception
     */
    String downloadVariantFile() throws Exception {
        return downloader.run();
    }

    static public String trimTo4000( String text, int rgdId, String label ) {

        String newText = text;
        int combinedTextTooLong = 0;

        // ensure that the text is no longer than 4000 characters
        if( text!=null && text.length() > 3980 ) {
            // take into account UTF8 encoding
            try {
                String text2;
                int len = text.length();
                if( len > 4000 ) {
                    len = 4000;
                }
                int utf8Len = 0;
                do {
                    text2 = text.substring(0, len);
                    len--;
                    utf8Len = text2.getBytes("UTF-8").length;
                } while (utf8Len > 3996);

                String msg = "  combined "+label+" too long for RGD:" + rgdId + "! UTF8 str len:" + (4+utf8Len);
                LogManager.getLogger("dbg").debug(msg);
                combinedTextTooLong++;

                newText = (text2 + " ...");
            } catch (UnsupportedEncodingException e) {
                // totally unexpected
                throw new RuntimeException(e);
            }
        }
        if( combinedTextTooLong > 0 ) {
            GlobalCounters.getInstance().incrementCounter(label+"_TRIMMED_DUE_TO_4000_ORACLE_LIMIT", 1);
        }
        return newText;
    }


    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setDao(Dao dao) {
        this.dao = dao;
    }

    public Dao getDao() {
        return dao;
    }

    public void setQc(QC qc) {
        this.qc = qc;
    }

    public QC getQc() {
        return qc;
    }

    public void setLoader(Loader loader) {
        this.loader = loader;
    }

    public Loader getLoader() {
        return loader;
    }

    public void setDownloader(Downloader downloader) {
        this.downloader = downloader;
    }

    public Downloader getDownloader() {
        return downloader;
    }

    public void setParser(ParseGroup parser) {
        this.parser = parser;
    }

    public ParseGroup getParser() {
        return parser;
    }
}
