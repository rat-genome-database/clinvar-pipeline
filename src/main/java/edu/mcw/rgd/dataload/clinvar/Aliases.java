package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.Alias;
import edu.mcw.rgd.process.Utils;

import java.util.*;

/**
 * @author mtutaj
 * @since 3/13/14
 * helper class to handle aliases
 */
public class Aliases {

    private List<String> incoming = new ArrayList<String>();
    private List<Alias> matching = new ArrayList<Alias>();
    private List<Alias> forInsert = new ArrayList<Alias>();
    private List<Alias> forDelete;
    private String clinVarId;

    /**
     * add the alias to the incoming list, if not a duplicate;
     * skip aliases: 'not provided' and identical with a trait name
     * @param aliasName name of alias to be added to the incoming list
     * @param clinVarId clinvar id
     * @param traitName variant trait name
     * @return true if alias has been added; false if alias name is invalid or if alias is a duplicate
     */
    public boolean addIncomingAlias(String aliasName, String clinVarId, String traitName) {
        if( aliasName==null || aliasName.isEmpty() )
            return false;
        aliasName = aliasName.trim();

        // do not add incoming alias 'not provided'
        if( Utils.stringsAreEqualIgnoreCase(aliasName, "not provided") ||
            Utils.stringsAreEqualIgnoreCase(aliasName, "not specified") )
            return false;

        // do not add incoming alias if it is the same as trait name
        String normalizedAliasName = aliasName + " [" + clinVarId + "]";
        if( traitName!=null && traitName.toLowerCase().contains(normalizedAliasName.toLowerCase()) ) {
            return false;
        }

        this.clinVarId = clinVarId;

        for( String alias: incoming ) {
            if( Utils.stringsAreEqualIgnoreCase(alias, aliasName) )
                return false;
        }
        return incoming.add(aliasName.trim());
    }

    public void qc(int varRgdId, Dao dao) throws Exception {

        List<Alias> inRgd = dao.getAliases(varRgdId);
        List<Alias> inRgdCopy = new ArrayList<Alias>(inRgd);

        // filter out aliases of different clinVarId
        List<Alias> inRgdDiffClinVarId = new ArrayList<Alias>();
        Iterator<Alias> it = inRgd.iterator();
        while( it.hasNext() ) {
            Alias a = it.next();
            if( a.getNotes()!=null
             && a.getNotes().startsWith("RCV")
             && !a.getNotes().equals(clinVarId) ) {
                it.remove();
                inRgdDiffClinVarId.add(a);
            }
        }


        for( String aliasName: incoming ) {
            Alias matchingAlias = detach(inRgdCopy, aliasName);
            if( matchingAlias!=null ) {
                // matches RGD
                matching.add(matchingAlias);
            }
            else {
                // no match with RGD -- insert it
                Alias alias = new Alias();
                alias.setTypeName("alternate_id");
                alias.setValue(aliasName);
                alias.setRgdId(varRgdId);
                alias.setNotes(clinVarId);
                forInsert.add(alias);
            }
        }

        inRgdCopy.removeAll(inRgdDiffClinVarId);
        forDelete = inRgdCopy;
    }

    private Alias detach(List<Alias> aliases, String aliasName) {

        Iterator<Alias> it = aliases.iterator();
        while( it.hasNext() ) {
            Alias aliasMatch = it.next();
            if(Utils.stringsAreEqualIgnoreCase(aliasName, aliasMatch.getValue()) ) {
                it.remove();
                return aliasMatch;
            }
        }
        return null;
    }

    public void sync(int varRgdId, Dao dao) throws Exception {

        if( !matching.isEmpty() ) {
            GlobalCounters.getInstance().incrementCounter("ALIASES_MATCHED", matching.size());
        }

        if( !forInsert.isEmpty() ) {
            for( Alias alias: forInsert ) {
                alias.setRgdId(varRgdId);
            }

            dao.insertAliases(forInsert);
            GlobalCounters.getInstance().incrementCounter("ALIASES_INSERTED", forInsert.size());
        }

        if( !forDelete.isEmpty() ) {
            dao.deleteAliases(forDelete);
            GlobalCounters.getInstance().incrementCounter("ALIASES_DELETED", forDelete.size());
        }
    }
}
