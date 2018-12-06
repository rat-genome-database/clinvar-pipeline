package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.MapData;
import edu.mcw.rgd.process.sync.MapDataSyncer;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mtutaj
 * @since 2/19/14
 * variant positional information
 */
public class MapPositions {

    private List<MapData> mdsIncoming = new ArrayList<MapData>();

    private MapDataSyncer syncer = new MapDataSyncer();

    public List<MapData> getMdsIncoming() {
        return mdsIncoming;
    }

    public void addPos(String assembly, String chr, String accession, String start, String stop, String strand) throws Exception {

        if( chr==null ) {
            throw new Exception("null chromosome");
        }
        int startPos = Integer.parseInt(start);
        int stopPos = Integer.parseInt(stop);

        MapData md = new MapData();
        md.setSrcPipeline(Manager.SOURCE);
        md.setChromosome(chr);
        md.setNotes(accession);
        md.setStrand(strand);
        if( startPos>stopPos ) {
            // ensure start_pos <= stop_pos
            md.setStartPos(stopPos);
            md.setStopPos(startPos);
        }
        else {
            md.setStartPos(startPos);
            md.setStopPos(stopPos);
        }

        switch(assembly) {
            case "NCBI36":
                md.setMapKey(13);
                break;
            case "GRCh37.p9":
            case "GRCh37.p10":
            case "GRCh37.p13":
            case "GRCh37":
                md.setMapKey(17);
                break;
            case "GRCh38":
                md.setMapKey(38);
                break;
            default:
                throw new Exception("Unsupported assembly map "+assembly);
        }

        mdsIncoming.add(md);
    }

    public void addCytoPos(String cytoPos) throws Exception {

        // extract chromosome from fish band, if possible
        String chr = null;
        int pos = cytoPos.indexOf("p");
        if( pos<0 )
            pos = cytoPos.indexOf("q");
        if( pos>0 ) {
            chr = cytoPos.substring(0, pos);
        }

        MapData md = new MapData();

        md.setSrcPipeline(Manager.SOURCE);
        if( chr==null ) {
            md.setChromosome(cytoPos);
        } else {
            md.setChromosome(chr);
            md.setFishBand(cytoPos);
        }

        md.setMapKey(11); // human cytomap

        mdsIncoming.add(md);
    }

    public List<MapData> getMdsInRgd() {
        return syncer.getObjectsInRgd();
    }

    /**
     * perform qc
     * @param dao
     * @throws Exception
     */
    public void qc(int varRgdId, Dao dao) throws Exception {

        if( varRgdId!=0 ) {
            for( MapData md: (List<MapData>)syncer.getObjectsForInsert() ) {
                md.setRgdId(varRgdId);
            }
            syncer.setObjectsInRgd(dao.getMapData(varRgdId));
        }

        syncer.addIncomingObjects(getMdsIncoming());

        // load in-rgd map data
        syncer.qc(null, true);
    }

    /**
     * sync incoming xdb ids with RGD database
     */
    public void sync(int variantRgdId, Dao dao) throws Exception {

        if( !syncer.getMatchingObjects().isEmpty() ) {
            GlobalCounters.getInstance().incrementCounter("MAPPOS_MATCHED", syncer.getMatchingObjects().size());
        }

        if( !syncer.getObjectsForInsert().isEmpty() ) {
            for( MapData md: (List<MapData>)syncer.getObjectsForInsert() ) {
                md.setRgdId(variantRgdId);
            }
            dao.insertMapData(syncer.getObjectsForInsert());
            GlobalCounters.getInstance().incrementCounter("MAPPOS_INSERTED", syncer.getObjectsForInsert().size());
        }

        if( !syncer.getObjectsForDelete().isEmpty() ) {
            dao.deleteMapData(syncer.getObjectsForDelete());
            GlobalCounters.getInstance().incrementCounter("MAPPOS_DELETED", syncer.getObjectsForDelete().size());
        }

        if( !syncer.getObjectsForUpdate().isEmpty() ) {
            dao.updateMapData(syncer.getObjectsForUpdate());
            GlobalCounters.getInstance().incrementCounter("MAPPOS_UPDATED", syncer.getObjectsForUpdate().size());
        }
    }
}
