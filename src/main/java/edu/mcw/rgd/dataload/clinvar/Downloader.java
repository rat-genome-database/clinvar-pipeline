package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.process.FileDownloader2;

/**
 * @author mtutaj
 * @since 2/13/14
 * download a xml.gz file from the ClinVar site, and validate it;
 * keep daily copies of this file, to reuse it to conserve network resources
 */
public class Downloader {

    private String variantDataFile;

    public String run() throws Exception {

        FileDownloader2 downloader = new FileDownloader2();
        downloader.setExternalFile(getVariantDataFile());
        downloader.setLocalFile("data/ClinVarFullRelease.xml.gz");
        downloader.setPrependDateStamp(true);

        String localFileName = downloader.downloadNew();
        return localFileName;
    }

    public void setVariantDataFile(String variantDataFile) {
        this.variantDataFile = variantDataFile;
    }

    public String getVariantDataFile() {
        return variantDataFile;
    }
}
