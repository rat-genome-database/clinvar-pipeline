package edu.mcw.rgd.dataload.clinvar;

/**
 * @author mtutaj
 * @since 2/11/14
 * load data into database if needed
 */
public class Loader {

    private Dao dao;

    public Dao getDao() {
        return dao;
    }

    public void setDao(Dao dao) {
        this.dao = dao;
    }

    public void run(Record rec) throws Exception {

        // insert/update variants
        if( rec.getVarInRgd()==null ) {
            dao.insertVariant(rec.getVarIncoming());
            rec.setVarInRgd(rec.getVarIncoming());

            GlobalCounters.getInstance().incrementCounter("VARIANTS_INSERTED", 1);
        }
        else if( rec.isUpdateRecordFlag() ) {
            dao.updateVariant(rec.getVarIncoming(), rec.getVarInRgd());
            rec.setVarInRgd(rec.getVarIncoming());

            GlobalCounters.getInstance().incrementCounter("VARIANTS_UPDATED", 1);
        }
        else {
            dao.updateVariantLastModifiedDate(rec.getVarInRgd().getRgdId());

            GlobalCounters.getInstance().incrementCounter("VARIANTS_MATCHING", 1);
        }

        // insert/update/delete gene associations for variant
        rec.getGeneAssociations().sync(rec.getVarInRgd().getRgdId(), getDao());

        // insert/update/delete xdb ids for variant
        rec.getXdbIds().sync(rec.getVarInRgd().getRgdId(), getDao());

        // insert/update/delete map positions for variant
        rec.getMapPositions().sync(rec.getVarInRgd().getRgdId(), getDao());

        // insert/update/delete hgvs names for variant
        rec.getHgvsNames().sync(rec.getVarInRgd().getRgdId(), getDao());

        // insert/update/delete aliases for variant
        rec.getAliases().sync(rec.getVarInRgd().getRgdId(), getDao());
    }
}
