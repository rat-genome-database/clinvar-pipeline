package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.process.Utils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.util.*;

/**
 * @author mtutaj
 * @since 2/11/14
 */
public class Manager {

    public static final String SOURCE = "CLINVAR";

    Logger log = Logger.getLogger("loader");

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
        boolean runLoader = false;
        boolean qcDuplicateTerms = false;

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
            }
        }

        try {
            if( qcDuplicateTerms ) {
                TermNameMatcher matcher = new TermNameMatcher();
                matcher.indexTerms(manager.getDao());
            }

            if( runLoader ) {
                manager.run();
            }

            if( annotator!=null ) {
                annotator.run(manager.getDao());
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
        }
    }

    public void run() throws Exception {

        long time0 = System.currentTimeMillis();

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

        TraitNameCollection.getInstance().qcAndLoad(getDao());

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
