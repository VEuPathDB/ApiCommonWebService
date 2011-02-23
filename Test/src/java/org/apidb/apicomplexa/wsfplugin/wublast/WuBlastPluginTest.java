package org.apidb.apicomplexa.wsfplugin.wublast;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.apidb.apicomplexa.wsfplugin.ncbiblast.NcbiBlastPlugin;
import org.gusdb.wsf.plugin.Plugin;
import org.gusdb.wsf.plugin.WsfRequest;
import org.gusdb.wsf.plugin.WsfResponse;
import org.gusdb.wsf.plugin.WsfServiceException;
import org.gusdb.wsf.util.Formatter;
import org.junit.Test;

/**
 * @author Jerric
 * @created Nov 2, 2005
 */
public class WuBlastPluginTest extends TestCase {

    private static Logger logger = Logger.getLogger(WuBlastPluginTest.class);

    @Test
    public static void testInvoke() throws WsfServiceException {
        // prepare parameters
        Map<String, String> params = new HashMap<String, String>();
        params.put(WuBlastPlugin.PARAM_ALGORITHM, "blastn");
        params.put(NcbiBlastPlugin.PARAM_SEQUENCE, "AGCTTTTCAT"
                + "TCTGACTGCAACGGGCAATATGTCTCTGTGTGGATTAAAAAAAGAGTGTCTG"
                + "ATAGCAGCTTCTGAACTGGTTACCTGCCGTGAGTAAATTAAAATTTTATTGA"
                + "CTTAGGTCACTAAATACTTTAACCAATATAGGCATAGCGCACAGACAGATAA"
                + "AAATTACAGAGTACACAACATCCATGAAACGCATTAGCACCACCATTACCAC"
                + "CACCATCACCATTACCACAGGTAACGGTGCGGGCTGACGCGTACAGGAAACA"
                + "CAGAAAAAAGCCCGCACCTGACAGTGCGGGCTTTTTTTTTCGACCAAAGGTA"
                + "ACGAGGTAACAACCATGCGAGTGTTGAAGTTCGGCGGTACATCAGTGGCAAA"
                + "TGCAGAACGTTTTCTGCGTGTTGCCGATATTCTGGAAAGCAATGCCAGGCAG"
                + "GGGCAGGTGGCCACCGTCCTCTCTGCCCCCGCCAAAATCACCAACCACCTGG"
                + "TGGCGATGATTGAAAAAACCATTAGCGGCCAGGATGCTTTACCCAATATCAG"
                + "CGATGCCGAACGTATTTTTGCCGAACTTTT");
        params.put(WuBlastPlugin.PARAM_DATABASE_TYPE, "Cparvum_nt.fsa");

        // prepare the columns
        String[] columns = { WuBlastPlugin.COLUMN_ID, WuBlastPlugin.COLUMN_ROW,
                WuBlastPlugin.COLUMN_BLOCK, WuBlastPlugin.COLUMN_HEADER,
                WuBlastPlugin.COLUMN_FOOTER };
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < columns.length; i++) {
            map.put(columns[i], i);
        }

        WsfRequest request = new WsfRequest();
        request.setParams(params);
        request.setOrderedColumns(columns);
        request.setContext(new HashMap<String, String>());

        // invoke the blast process
        Plugin processor = new WuBlastPlugin();
        WsfResponse wsfResult = processor.execute(request);

        logger.info("Result Message: " + wsfResult.getMessage());
        logger.info("Result Signal: " + wsfResult.getSignal());

        // print out results
        System.out.println("========= Motif Search JUnit Test ==========");
        // print parameters
        System.out.println("Parameters: ");
        for (String param : params.keySet()) {
            System.out.println("\t\t" + params + ": " + params.get(param));
        }
        System.out.println();
        // print results
        System.out.println(Formatter.printArray(columns));
        System.out.println(Formatter.printArray(wsfResult.getResult()));
    }
}
