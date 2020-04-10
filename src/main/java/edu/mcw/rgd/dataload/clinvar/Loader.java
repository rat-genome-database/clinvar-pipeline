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

        boolean updateVariantLastModifiedDate = false;

        // insert/update variants
        if( rec.getVarInRgd()==null ) {
            dao.insertVariant(rec.getVarIncoming());
            rec.setVarInRgd(rec.getVarIncoming());

            GlobalCounters.getInstance().incrementCounter("VARIANTS_INSERTED", 1);
        }
        else if( rec.isUpdateRecordFlag() ) {
            if( dao.updateVariant(rec.getVarIncoming(), rec.getVarInRgd()) ) {
                rec.setVarInRgd(rec.getVarIncoming());
                updateVariantLastModifiedDate = true;

                GlobalCounters.getInstance().incrementCounter("VARIANTS_UPDATED", 1);
            } else {
                // incoming variant is the same as variant in rgd -- downgrading UPDATE to UP-TO-DATE
                GlobalCounters.getInstance().incrementCounter("VARIANTS_MATCHING (DOWNGRADED FROM UPDATE)", 1);
            }
        }
        else {
            GlobalCounters.getInstance().incrementCounter("VARIANTS_MATCHING", 1);
        }

        // insert/update/delete gene associations for variant
        if( rec.getGeneAssociations().sync(rec.getVarInRgd().getRgdId(), getDao()) ) {
            updateVariantLastModifiedDate = true;
        }

        // insert/update/delete xdb ids for variant
        if( rec.getXdbIds().sync(rec.getVarInRgd().getRgdId(), getDao()) ) {
            updateVariantLastModifiedDate = true;
        }

        // insert/update/delete map positions for variant
        if( rec.getMapPositions().sync(rec.getVarInRgd().getRgdId(), getDao()) ) {
            updateVariantLastModifiedDate = true;
        }

        // insert/update/delete hgvs names for variant
        if( rec.getHgvsNames().sync(rec.getVarInRgd().getRgdId(), getDao()) ) {
            updateVariantLastModifiedDate = true;
        }

        // insert/update/delete aliases for variant
        if( rec.getAliases().sync(rec.getVarInRgd().getRgdId(), getDao()) ) {
            updateVariantLastModifiedDate = true;
        }

        if( updateVariantLastModifiedDate ) {
            dao.updateVariantLastModifiedDate(rec.getVarInRgd().getRgdId());
        }
    }
}
