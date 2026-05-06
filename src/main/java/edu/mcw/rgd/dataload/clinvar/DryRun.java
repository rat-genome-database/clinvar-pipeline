package edu.mcw.rgd.dataload.clinvar;

import edu.mcw.rgd.datamodel.VariantInfo;

import java.io.File;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Standalone parser dry-run: no Spring, no DB.
 * Prints any "unknown" / "TODO" / NPE that the Parser surfaces while reading chunks,
 * and aggregates which VariantInfo fields end up populated by the Parser
 * (relative to what the legacy RCV parser used to populate).
 *
 * usage:  java -cp ... edu.mcw.rgd.dataload.clinvar.DryRun &lt;chunk.xml.gz&gt; ...
 */
public class DryRun {

    static int recCount;
    static Set<String> populatedFields = new HashSet<>();
    static List<String> outputLines = new ArrayList<>();

    public static void main(String[] args) throws Exception {

        // Capture System.out so we can detect "unknown"/"TODO"/"ERROR" prints from the Parser
        PrintStream realOut = System.out;
        PrintStream tap = new PrintStream(realOut) {
            @Override public void println(String s) {
                outputLines.add(s);
                super.println(s);
            }
        };
        System.setOut(tap);

        StubDao dao = new StubDao();
        StubQC qc = new StubQC();
        qc.setDao(dao);
        StubLoader loader = new StubLoader();

        for (String fname : args) {
            realOut.println("== chunk: " + fname);
            Parser parser = new Parser();
            parser.qc = qc;
            parser.loader = loader;
            parser.setValidate(false);
            try {
                parser.parse(new File(fname));
            } catch (Throwable t) {
                realOut.println("FATAL while parsing " + fname + ": " + t);
                t.printStackTrace(realOut);
            }
        }

        System.setOut(realOut);

        realOut.println();
        realOut.println("==================== SUMMARY ====================");
        realOut.println("variants processed: " + recCount);
        realOut.println();
        realOut.println("VariantInfo fields populated at least once:");
        for (String f : new java.util.TreeSet<>(populatedFields)) realOut.println("  " + f);

        realOut.println();
        realOut.println("Parser stdout markers (unique):");
        java.util.Map<String,Integer> markers = new java.util.TreeMap<>();
        for (String l : outputLines) {
            if (l == null) continue;
            String key = null;
            if (l.startsWith("unknown")) key = l;
            else if (l.startsWith("ERROR")) key = l.substring(0, Math.min(80, l.length()));
            else if (l.startsWith("TODO") || l.contains("todo")) key = l;
            else if (l.startsWith("Classification unhandled")) key = l;
            else if (l.startsWith("unexpected")) key = l;
            else if (l.startsWith("handle it")) key = l;
            if (key != null) markers.merge(key, 1, Integer::sum);
        }
        for (java.util.Map.Entry<String,Integer> e : markers.entrySet()) {
            realOut.println("  " + e.getValue() + "x  " + e.getKey());
        }

        realOut.println();
        realOut.println("Counters:");
        realOut.println(GlobalCounters.getInstance().dump());
    }

    /** Bypass DB validation - return the SO acc id as-is (or empty if input empty). */
    static class StubDao extends Dao {
        @Override
        public String validateSoAccId(String soAccId) {
            return soAccId == null ? "" : soAccId;
        }
    }

    /** Mimics what QC.run does to incoming VariantInfo, but without any DB lookup. */
    static class StubQC extends QC {
        @Override
        public void run(Record rec) {
            recCount++;
            VariantInfo v = rec.getVarIncoming();
            if (v.getName() != null) populatedFields.add("name");
            if (v.getObjectType() != null) populatedFields.add("objectType");
            if (v.getSoAccId() != null && !v.getSoAccId().isEmpty()) populatedFields.add("soAccId");
            if (v.getRefNuc() != null) populatedFields.add("refNuc");
            if (v.getVarNuc() != null) populatedFields.add("varNuc");
            if (v.getNucleotideChange() != null) populatedFields.add("nucleotideChange");
            if (v.getMolecularConsequence() != null) populatedFields.add("molecularConsequence");
            if (v.getClinicalSignificance() != null) populatedFields.add("clinicalSignificance");
            if (v.getReviewStatus() != null) populatedFields.add("reviewStatus");
            if (v.getDateLastEvaluated() != null) populatedFields.add("dateLastEvaluated");
            if (v.getMethodType() != null) populatedFields.add("methodType");
            if (v.getAgeOfOnset() != null) populatedFields.add("ageOfOnset");
            if (v.getPrevalence() != null) populatedFields.add("prevalence");
            if (v.getTraitName() != null) populatedFields.add("traitName");
            if (v.getSubmitter() != null) populatedFields.add("submitter");
            if (v.getNotes() != null) populatedFields.add("notes");

            if (rec.getXdbIds().getClinVarId() != null) populatedFields.add("xdb:clinVar");
            // also note other extracted xdb keys
            try {
                java.lang.reflect.Field f = XdbIds.class.getDeclaredField("incomingXdbIds");
                f.setAccessible(true);
                @SuppressWarnings("unchecked")
                List<edu.mcw.rgd.datamodel.XdbId> ids = (List<edu.mcw.rgd.datamodel.XdbId>) f.get(rec.getXdbIds());
                Set<Integer> keys = new HashSet<>();
                for (edu.mcw.rgd.datamodel.XdbId x : ids) keys.add(x.getXdbKey());
                for (Integer k : keys) populatedFields.add("xdbKey:" + k);
            } catch (Exception ignore) {}

            if (!rec.getMapPositions().getMdsIncoming().isEmpty()) populatedFields.add("mapPositions");
            // hgvs/aliases/geneAssoc — accessed through known getters
        }
    }

    static class StubLoader extends Loader {
        @Override
        public void run(Record rec) { /* no-op */ }
    }
}
