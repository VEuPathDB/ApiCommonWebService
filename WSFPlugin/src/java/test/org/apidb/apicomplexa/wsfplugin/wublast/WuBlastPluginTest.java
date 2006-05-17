package test.org.apidb.apicomplexa.wsfplugin.wublast;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.apidb.apicomplexa.wsfplugin.ncbiblast.NcbiBlastPlugin;
import org.apidb.apicomplexa.wsfplugin.wublast.WuBlastPlugin;
import org.gusdb.wsf.plugin.IWsfPlugin;
import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author Jerric
 * @created Nov 2, 2005
 */
public class WuBlastPluginTest extends TestCase {

    private static Logger logger = Logger.getLogger(WuBlastPluginTest.class);

    /*
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
    }

    /*
     * Test method for 'NcbiProcessor.invoke(String[], String[], String[])'
     */
    public static void testInvoke() {
        // prepare parameters
        Map<String, String> params = new HashMap<String, String>();
        params.put(WuBlastPlugin.PARAM_QUERY_TYPE, "blastn");
        params.put(
                NcbiBlastPlugin.PARAM_SEQUENCE,
                "AGCTTTTCATTCTGACTGCAACGGGCAATATGTCTCTGTGTGGATTAAAAAAAGAGTGTCTG"
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

        // invoke the blast process
        try {
            IWsfPlugin processor = new WuBlastPlugin();
            String[][] result = processor.invoke(params, columns);

            // print out the result
            System.out.println(WsfPlugin.printArray(columns));
            System.out.println(WsfPlugin.printArray(result));
        } catch (WsfServiceException ex) {
            logger.error(ex);
            ex.printStackTrace();
            assertTrue(false);
        }
    }

    public static void main(String[] args) throws Exception {
        testInvoke();
    }
}
