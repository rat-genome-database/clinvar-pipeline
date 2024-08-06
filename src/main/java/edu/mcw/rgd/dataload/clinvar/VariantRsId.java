package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.VariantInfo;
import edu.mcw.rgd.datamodel.XdbId;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class VariantRsId {

    String version;
    Dao dao = new Dao();
    Logger log = LogManager.getLogger("rsStatus");

    public void run() throws Exception {
        // get clinvar xdbs
        SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        log.info(getVersion());
        long pipeStart = System.currentTimeMillis();
        log.info("Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");
        try {
            List<VariantInfo> clinVars = dao.getActiveVariants();
            List<VariantMapData> updatedRs = new ArrayList<>();
            List<Integer> rgdIds = new ArrayList<>();
            for (VariantInfo var : clinVars) {
                List<XdbId> xdbs = dao.getXdbIds(var.getRgdId(), 48); // 48 has the rsIds
                // loop through xdbs, use rgdId to find Variant and add rsID if variant exists
                for (XdbId xdb : xdbs) {
                    if (xdb.getLinkText().startsWith("rs")) {
                        List<VariantMapData> cnVars = dao.getVariantByRgdId(xdb.getRgdId());
                        // compare rs id and update accordingly
                        for (VariantMapData cnVar : cnVars) {
                            if (!Utils.stringsAreEqual(cnVar.getRsId(), xdb.getLinkText()) && !rgdIds.contains(var.getRgdId())) {
                                log.info("\t\trsID for variant "+cnVar.getId()+" being changed, old |"+cnVar.getRsId()+"|, new |"+xdb.getLinkText()+"|");
                                cnVar.setRsId(xdb.getLinkText());
                                updatedRs.add(cnVar);
                                rgdIds.add(var.getRgdId());
                            }
                        }
                    }
                }
            }

            if (!updatedRs.isEmpty()) {
                log.info("\tUpdating rsIds: "+updatedRs.size());
                dao.updateVariantRsID(updatedRs);
            }
        }
        catch (Exception e){
            log.info(e);
        }
        log.info("Clinvar rsID assignment runtime -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis()));
    }
    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }
}
