package edu.mcw.rgd.dataload.clinvar;

import java.io.Serializable;

public class ClinVarInfo implements Serializable {
    public int rgdId;
    public String varType;
    public String refNuc;
    public String varNuc;
    public String rsId;
    public String chr;
    public int startPos;
    public int endPos;
    public String paddingBase;
    public boolean hasBadRef = false;
}
