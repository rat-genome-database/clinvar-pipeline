package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.process.CounterPool;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Clinvar2Vcf {

    Logger log = LogManager.getLogger("clinvar2vcf");

    final String SERIAL_DATA_FILE_NAME = "/tmp/clinvar.ser";

    public void run() {

        try {

            //fixRefAndAllele();

            List<ClinVarInfo> variants = new ArrayList<>(); //deserialize();
            if( variants.isEmpty() ) {
                log.info("retrieving clinvar variants ...");

                String[] _chrs = {
                    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
                        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
                        "21", "22", "X", "Y", "MT" };
                List<String> chromosomes = Arrays.asList(_chrs);

                chromosomes.parallelStream().forEach( chr -> {

                    try {
                        List<ClinVarInfo> variantsForChr = loadVariants(chr);
                        log.info("    " + variantsForChr.size());

                        validateRsIds(variantsForChr);

                        validateRef(variantsForChr);

                        synchronized (variants) {
                            variants.addAll(variantsForChr);
                        }
                    }
                    catch( Exception e) {
                        throw new RuntimeException(e);
                    }

                });
            } else {
                log.info("variants deserialized: "+variants.size());
            }

            log.info("normalizing...");
            normalize(variants);

            // group variants by a key: chr|pos|rsId -- skip variants with bad ref
            log.info("grouping...");
            Map<String, List<ClinVarInfo>> groups = groupVariants(variants);
            log.info( "group size: "+groups.size());


            BufferedWriter out = Utils.openWriter("/tmp/clinvar.vcf");

            String header = """
                ##fileformat=VCFv4.1
                ##fileDate=YYYYMMDD
                ##source=ClinVar_via_RGD
                ##reference=GRCh38
                ##INFO=<ID=dbSNP_156,Number=0,Type=Flag,Description="Variants (including SNPs and indels) imported from dbSNP">
                ##INFO=<ID=TSA,Number=1,Type=String,Description="Type of sequence alteration. Child of term sequence_alteration as defined by the sequence ontology project.">
                ##INFO=<ID=E_Freq,Number=0,Type=Flag,Description="Frequency.https://www.ensembl.org/info/genome/variation/prediction/variant_quality.html#evidence_status">
                #CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO
                """;

            SimpleDateFormat sdt = new SimpleDateFormat("YYYY-MM-dd");
            header = header.replace( "YYYYMMDD", sdt.format(new Date()));

            out.write(header);

            List<String> dataLines = new ArrayList<>(groups.size());

            for( List<ClinVarInfo> list: groups.values() ) {

                ClinVarInfo v = list.get(0);

                //if( v.startPos==10293167 || v.startPos==10293166 || v.startPos==10293168 ) {
                //    System.out.println("sss");
                //}

                // merge multiple alleles
                Set<String> refSet = new TreeSet<>();
                Set<String> varSet = new TreeSet<>();
                for( ClinVarInfo i: list ) {
                    refSet.add( Utils.NVL(i.refNuc,"-") );
                    varSet.add( Utils.NVL(i.varNuc, "-") );
                }
                String ref = Utils.concatenate(refSet, ",");
                String var = Utils.concatenate(varSet, ",");

                if( ref.length()>1 && var.length()>1 ) {
                    log.warn("skipped: "+v.chr+":"+v.startPos+"   "+ref+">"+var);
                    continue;
                }

                String rsId = Utils.NVL(v.rsId, ".");

                String info = "";
                if( rsId.startsWith("rs") ) {
                    info = "dbSNP_156";
                }
                if( !info.isEmpty() ) {
                    info += ";";
                }
                info += "TSA="+v.varType;
                if( !info.isEmpty() ) {
                    info += ";";
                }
                info += "E_Freq";

                String line = v.chr+"\t"+v.startPos+"\t"+rsId+"\t"+ref+"\t"+var+"\t.\t.\t"+info+"\n";
                dataLines.add(line);
            }

            log.info("sorting...");

            Collections.sort(dataLines, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {

                    String chr1, chr2, rest1, rest2;
                    int pos1, pos2;

                    int tab1pos = o1.indexOf('\t');
                    int tab2pos = o1.indexOf('\t', tab1pos+1);
                    chr1 = o1.substring(0, tab1pos);
                    rest1 = o1.substring(tab2pos+1);
                    pos1 = Integer.parseInt(o1.substring(tab1pos+1, tab2pos));

                    tab1pos = o2.indexOf('\t');
                    tab2pos = o2.indexOf('\t', tab1pos+1);
                    chr2 = o2.substring(0, tab1pos);
                    rest2 = o2.substring(tab2pos+1);
                    pos2 = Integer.parseInt(o2.substring(tab1pos+1, tab2pos));

                    int r = chr1.compareTo(chr2);
                    if( r!=0 ) {
                        return r;
                    }
                    r = pos1 - pos2;
                    if( r!=0 ) {
                        return r;
                    }
                    r = rest1.compareTo(rest2);
                    return r;
                }
            });

            for( String dataLine: dataLines ) {
                out.write(dataLine);
            }

            out.close();


        } catch( Exception e ) {
            e.printStackTrace();
        }

        log.info("====  THE END ====");
    }

    Map<String, List<ClinVarInfo>> groupVariants( List<ClinVarInfo> variants ) {

        Map<String, List<ClinVarInfo>> groups = new TreeMap<>();

        for( ClinVarInfo v: variants ) {

            if( v.hasBadRef ) {
                continue;
            }

            String key = v.chr+"|"+v.startPos+"|"+v.rsId+"|"+v.varType;
            List<ClinVarInfo> group = groups.get(key);
            if( group==null ) {
                group = new ArrayList<>();
                groups.put(key, group);
            }
            group.add(v);
        }

        return groups;
    }

    void normalize( List<ClinVarInfo> variants ) {

        Collections.shuffle(variants);

        variants.stream().forEach( v -> {

            //if( v.startPos==10293167 || v.startPos==10293166 || v.startPos==10293168 ) {
            //    System.out.println("sss");
            //}
            switch( v.varType ) {

                case "snv":
                    v.varType = "SNV";
                    break;

                case "insertion":
                    // it looks like position for insertion are OK
                    if( v.refNuc==null || v.refNuc.equals("-") ) {

                        String paddingBase = null;
                        try {
                            paddingBase = Utils.NVL(v.paddingBase, getBase(v.chr, v.startPos));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        v.refNuc = paddingBase;
                        v.varNuc = paddingBase + v.varNuc;
                    } else {
                        v.hasBadRef = true;
                        log.warn("unexpected non null refnuc for insertion");
                    }
                    break;

                case "deletion":
                    // it looks like position for insertion are OK
                    if( v.varNuc==null || v.varNuc.startsWith("-") ) {

                        String paddingBase = null;
                        try {
                            paddingBase = Utils.NVL(v.paddingBase, getBase(v.chr, v.startPos-1));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        v.varNuc = paddingBase;
                        v.refNuc = paddingBase + v.refNuc;
                    } else {
                        throw new RuntimeException("unexpected non null varnuc for deletion");
                    }
                    break;

                default:
                    System.out.println("BAD");
            }
        });
    }

    public String getBase( String chr, int pos ) throws Exception {
        return getBases(chr, pos, pos);
    }

    public String getBases( String chr, int pos1, int pos2 ) throws Exception {
        String url = "https://dev.rgd.mcw.edu/rgdweb/seqretrieve/retrieve.html?mapKey=38&chr="+chr+"&startPos="+pos1+"&stopPos="+pos2+"&format=text";
        FileDownloader fd = new FileDownloader();
        fd.setExternalFile(url);
        String response = fd.download();
        return response;
    }

    public List<ClinVarInfo> loadVariants(String chr) {

        String sql = """
            select v.rgd_id,variant_type,ref_nuc,var_nuc,rs_id,chromosome,start_pos,end_pos,padding_base
            from variant v,variant_sample_detail d,variant_map_data m
            where sample_id=2 and v.rgd_id=d.rgd_id and v.rgd_id=m.rgd_id and map_key=38
            """;
        if( chr!=null ) {
            sql += " and chromosome='"+chr+"'";
        }

        List<ClinVarInfo> results = new ArrayList<>();

        try( Connection conn = DataSourceFactory.getInstance().getCarpeNovoDataSource().getConnection() ) {

            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while( rs.next() ) {

                ClinVarInfo r = new ClinVarInfo();
                r.rgdId = rs.getInt("rgd_id");
                r.varType = rs.getString("variant_type");
                r.refNuc = rs.getString("ref_nuc");
                r.varNuc = rs.getString("var_nuc");
                r.rsId = rs.getString("rs_id");
                r.chr = rs.getString("chromosome");
                r.startPos = rs.getInt("start_pos");

                // possibly unused
                r.endPos = rs.getInt("end_pos");
                r.paddingBase = rs.getString("padding_base");

                results.add(r);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return results;
    }

    public void validateRsIds( List<ClinVarInfo> variants ) throws IOException {

        AtomicInteger rsIdInDbSnp = new AtomicInteger(0);
        AtomicInteger noRsIdInDbSnp = new AtomicInteger(0);

        //Collections.shuffle(variants);

        AtomicInteger i = new AtomicInteger(0);

        variants.parallelStream().forEach( v -> {
            if( Utils.isStringEmpty(v.rsId) ) {

               String rsId = getRsIs(v);
               if( Utils.isStringEmpty(rsId) ) {
                   noRsIdInDbSnp.incrementAndGet();
                   if( !v.varType.equals("snv") ) {
                       //System.out.println(v.chr + " " + v.startPos + "  " + v.refNuc + "/" + v.varNuc + "     " + v.varType + "  " + v.paddingBase);
                   }
               } else {
                   v.rsId = rsId;
                   rsIdInDbSnp.incrementAndGet();
               }
            }

            int ii = i.getAndIncrement();
            if( ii%10000 == 0 ) {
                log.info("  "+ii+"   "+rsIdInDbSnp+"  "+noRsIdInDbSnp);
            }
        });

        // hack:
        log.info("variants without RS ID in RGD, but with RS_ID in DB_SNP: "+rsIdInDbSnp);
        log.info("variants without RS ID in RGD, and without RS_ID in DB_SNP: "+noRsIdInDbSnp);

        /*
        // serialize content to disk
        FileOutputStream fos = new FileOutputStream(SERIAL_DATA_FILE_NAME);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(variants);
        oos.close();

        log.info("variants serialized to file");
        */
    }

    void validateRef( List<ClinVarInfo> variants ) throws Exception {

        log.info("   validating ref for snvs ... ");
        AtomicInteger snvs = new AtomicInteger(0);

        List<ClinVarInfo> snvList = new ArrayList<>(variants.size());
        int maxPos = 0;

        for( ClinVarInfo v: variants ) {

            if( v.varType.equals("snv") ) {
                snvList.add(v);

                if( v.startPos > maxPos ) {
                    maxPos = v.startPos;
                }
            }
        }
        if( snvList.isEmpty() ) {
            return;
        }

        ClinVarInfo v1 = snvList.get(0);
        String fasta0Based = getBases(v1.chr, 1, maxPos);


        AtomicInteger snvsBad = new AtomicInteger(0);

        for( ClinVarInfo v: snvList ) {

            String refInFasta = fasta0Based.substring(v.startPos-1, v.startPos);
            if( !v.refNuc.equals(refInFasta) ) {
                snvsBad.incrementAndGet();
                v.hasBadRef = true;
            }
        }

        log.info("   snvs="+snvList.size()+"      variants="+variants.size());
        log.info("   snvs with bad ref base ="+snvsBad);

        //System.exit(0);
    }

    List<ClinVarInfo> deserialize() throws IOException, ClassNotFoundException {

        File file = new File(SERIAL_DATA_FILE_NAME);
        if( !file.exists() ) {
            return null;
        }

        FileInputStream fis = new FileInputStream(SERIAL_DATA_FILE_NAME);
        ObjectInputStream ois = new ObjectInputStream(fis);
        List<ClinVarInfo> list = (List<ClinVarInfo>) ois.readObject();
        ois.close();

        return list;
    }

    String getRsIs( ClinVarInfo r ) {

        String rsId = null;

        String sql = "SELECT snp_name FROM db_snp WHERE source='dbSnp156' AND map_key=38 "+
                " AND chromosome=? AND position=? AND ref_allele=? AND allele=?";

        try( Connection conn = DataSourceFactory.getInstance().getCarpeNovoDataSource().getConnection() ) {

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, r.chr);
            ps.setInt(2, r.startPos);
            ps.setString(3, r.refNuc);
            ps.setString(4, r.varNuc);

            ResultSet rs = ps.executeQuery();
            while( rs.next() ) {

                rsId = rs.getString(1);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return rsId;
    }






    void fixRefAndAllele() throws IOException {

        // this is th fiile location
        // https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz

        // pick a file with random content
        Random random = new Random();
        int u1 = 1+random.nextInt(5);
        String fname = "/Users/mtutaj/Downloads/clinvar.rnd"+u1+".vcf";
        System.out.println(fname);

        CounterPool counters = new CounterPool();
        AtomicInteger li = new AtomicInteger(0);

        BufferedReader in = Utils.openReader(fname);
        String line;
        while( (line=in.readLine()) != null ) {

            // skip comment lines
            if( line.startsWith("#") ) {
                continue;
            }
            li.incrementAndGet();

            // #CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
            //1	69134	2205837	A	G	.	.	xxxxx
            String[] cols = line.split("[\\t]", -1);
            String chr = cols[0];
            int pos = Integer.parseInt(cols[1]);
            String ref = cols[3];
            String allele = cols[4];
            String info = cols[7];
            // extract optional rs id  from info
            String rsId = extractRsIdFromInfo(info);
            if( rsId==null ) {
                // null RS ID
                counters.increment("linesWithoutRsId");
            } else {
                // existing RS ID
                counters.increment("linesWithRsId");
            }

            boolean isSnv = info.contains("CLNVC=single_nucleotide_variant");
            boolean isDeletion = info.contains("CLNVC=Deletion");
            boolean isDuplication = info.contains("CLNVC=Duplication");
            boolean isInsertion = info.contains("CLNVC=Insertion");
            boolean isInversion = info.contains("CLNVC=Inversion");
            boolean isMicrosatellite = info.contains("CLNVC=Microsatellite");
            boolean isIndel = info.contains("CLNVC=Indel");
            boolean isVariation = info.contains("CLNVC=Variation");

            if( isSnv ) {

                if (ref.length() == 1 && allele.length() == 1) {
                    handleSnv( chr, pos, ref, allele, rsId, counters );

                } else {
                    System.out.println("unhandled");
                }

            } else if( isDuplication || isInsertion ) {
                handleIns(chr, pos, ref, allele, rsId, counters);

            } else if( isDeletion ) {
                handleDel(chr, pos, ref, allele, rsId, counters);

            } else if( isInversion ) {
                handleInversion(chr, pos, ref, allele, rsId, counters);

            } else if( isMicrosatellite ) {

                if( allele.length()==1 && ref.length()>1 ) {
                    counters.increment("microsatellites handled as deletion");
                    handleDel(chr, pos, ref, allele, rsId, counters);

                } else if( allele.length()>1 && ref.length()==1 ) {
                    counters.increment("microsatellites handled as insertion");
                    handleIns(chr, pos, ref, allele, rsId, counters);

                } else {
                    System.out.println("unhandled");
                }

            } else if( isIndel ) {
                handleIndel(chr, pos, ref, allele, rsId, counters);

            } else if( isVariation ) {
                handleVariation(chr, pos, ref, allele, rsId, counters);

            } else {
                System.out.println("unhandled");
            }

            if( li.get() % 10000 == 0 ) {
                System.out.println(counters.dumpAlphabetically());
            }
        }

        in.close();

        System.out.println( counters.dumpAlphabetically() );

        System.exit(0);
    }

    void handleSnv( String chr, int pos, String ref, String allele, String rsId, CounterPool counters ) {

        List<ClinVarInfo> variantsInRgd = loadVariants(chr, pos);
        if (variantsInRgd.size() == 0) {
            counters.increment("clinvar snv - not in rgd");
        } else {
            boolean variantUpdated = false;
            for (ClinVarInfo v : variantsInRgd) {
                if (v.varType.equals("snv") && !v.refNuc.equals(ref)) {
                    updateRefAndAlleleForVariant(v.rgdId, ref, allele);
                    counters.increment("clinvar snv - updated");
                    variantUpdated = true;
                    break;
                }
            }
            if( variantUpdated ) {
                return;
            }

            boolean alleleMatch = false;
            for (ClinVarInfo v : variantsInRgd) {
                if (v.varType.equals("snv") && v.varNuc.equals(allele)) {
                    alleleMatch = true;
                    break;
                }
            }
            if (alleleMatch) {
                counters.increment("clinvar snv matches rgd");
            } else {
                if( allele.equals(".") ) {
                    counters.increment("clinvar snv - allele is '.'");
                } else {
                    counters.increment("clinvar snv - allele not in rgd");
                }
            }
        }
    }

    void handleDel( String chr, int pos, String ref, String allele, String rsId, CounterPool counters ) {

        // example: ref='GTTTCAGT' allele 'G'

        // in RGD we do not store padding base -- adjust accordingly
        if( allele.length()!=1 ) {
            System.out.println("unexpected");
            return;
        }

        int posInRgd = pos+1;
        String refInRgd = ref.substring(1);
        String alleleInRgd = "-";

        List<ClinVarInfo> variantsInRgd = loadVariants(chr, posInRgd);
        variantsInRgd.removeIf( v -> v.varType.equals("snv") );
        variantsInRgd.removeIf( v -> v.varType.equals("insertion") );

        if( variantsInRgd.isEmpty() ) {
            counters.increment("clinvar deletion not in rgd");
        } else {

            boolean match = false;
            for( ClinVarInfo v: variantsInRgd ) {
                if( refInRgd.equals(v.refNuc) && v.varNuc.startsWith(alleleInRgd) ) {
                    match = true;
                    break;
                }
            }

            if( match ) {
                counters.increment("clinvar deletion matches rgd");
            } else {
                counters.increment("clinvar deletion does not match rgd");
            }
        }
    }

    void handleIns( String chr, int pos, String ref, String allele, String rsId, CounterPool counters ) {

        // example: ref='G' allele 'GT'

        if( ref.length()!=1 ) {
            System.out.println("unexpected");
            return;
        }

        int posInRgd = pos+1;
        String refInRgd = "-";
        String alleleInRgd = allele.substring(1);

        List<ClinVarInfo> variantsInRgd = loadVariants(chr, posInRgd);
        variantsInRgd.removeIf( v -> v.varType.equals("snv") );

        if( variantsInRgd.isEmpty() ) {
            counters.increment("clinvar insertion not in rgd");
        } else {
            boolean match = true;
            for( ClinVarInfo v: variantsInRgd ) {
                if( refInRgd.equals(v.refNuc) && alleleInRgd.equals(v.varNuc) ) {
                    match = true;
                    break;
                }
            }
            if( match ) {
                counters.increment("clinvar insertion matches rgd");
            } else {
                System.out.println("unhandled");
            }
        }
    }

    void handleIndel( String chr, int pos, String ref, String allele, String rsId, CounterPool counters ) {

        counters.increment("clinvar indel -- not handled");
        if( false) {
            // example: ref='G' allele 'GT'

            if (ref.length() != 1) {
                System.out.println("unexpected");
                return;
            }

            int posInRgd = pos + 1;
            List<ClinVarInfo> variantsInRgd = loadVariants(chr, posInRgd);

            System.out.println("unhandled");
        }
    }

    void handleVariation( String chr, int pos, String ref, String allele, String rsId, CounterPool counters ) {

        if( allele.equals(".") ) {
            counters.increment("clinvar variation - allele is '.'");
        } else {
            System.out.println("unhandled");
        }
    }

    void handleInversion( String chr, int pos, String ref, String allele, String rsId, CounterPool counters ) {

        counters.increment("clinvar inversion -- not handled");
    }

    String extractRsIdFromInfo( String info ) {

        String rsId = null;
        int pos = info.indexOf("RS=");
        if( pos>=0 ) {
            int pos2;
            for( pos2=pos+3; pos2<info.length(); pos2++ ) {
                if( !Character.isDigit(info.charAt(pos2)) ) {
                    break;
                }
            }

            rsId = "rs"+info.substring(pos+3, pos2);
        }
        return rsId;
    }

    public List<ClinVarInfo> loadVariants(String chr, int pos) {

        String sql = """
            select v.rgd_id,variant_type,ref_nuc,var_nuc,rs_id,chromosome,start_pos,end_pos,padding_base
            from variant v,variant_sample_detail d,variant_map_data m
            where sample_id=2 and v.rgd_id=d.rgd_id and v.rgd_id=m.rgd_id and map_key=38
             and chromosome=? and start_pos=?
            """;

        List<ClinVarInfo> results = new ArrayList<>();

        try( Connection conn = DataSourceFactory.getInstance().getCarpeNovoDataSource().getConnection() ) {

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, chr);
            ps.setInt(2, pos);

            ResultSet rs = ps.executeQuery();
            while( rs.next() ) {

                ClinVarInfo r = new ClinVarInfo();
                r.rgdId = rs.getInt("rgd_id");
                r.varType = rs.getString("variant_type");
                r.refNuc = rs.getString("ref_nuc");
                r.varNuc = rs.getString("var_nuc");
                r.rsId = rs.getString("rs_id");
                r.chr = rs.getString("chromosome");
                r.startPos = rs.getInt("start_pos");

                // possibly unused
                r.endPos = rs.getInt("end_pos");
                r.paddingBase = rs.getString("padding_base");

                results.add(r);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return results;
    }

    public void updateRefAndAlleleForVariant(int rgdId, String ref, String allele) {

        String sql = "update variant set ref_nuc=?,var_nuc=? where rgd_id=?";

        try( Connection conn = DataSourceFactory.getInstance().getCarpeNovoDataSource().getConnection() ) {

            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, ref);
            ps.setString(2, allele);
            ps.setInt(3, rgdId);

            ps.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
