package test.org.apidb.apicomplexa.wsfplugin.wublast;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apidb.apicomplexa.wsfplugin.wublast.WuBlastPlugin;
import org.gusdb.wsf.IWsfPlugin;
import org.gusdb.wsf.WsfServiceException;

/**
 * @author Jerric
 * @created Nov 2, 2005
 */
public class WuBlastPluginTest extends TestCase {

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
        String[] params = { WuBlastPlugin.PARAM_APPLICATION,
                WuBlastPlugin.PARAM_DATABASE, WuBlastPlugin.PARAM_SEQUENCE };
        String[] values = {
                "blastn",
                "Cparvum_nt.fsa",
                "ATGTACGGATTTATTAAGTTTTTTGTTGGGTTTTGCATTCCAGCATACCATTCCATTCTT"
                        + "GCTTTAAAAACTCAAAATCATCATTTAATTAAAATATGGCTAGTATATTTTTTTACAGTT"
                        + "GTCTTTTATGAGCTAATTTTATCATTCATTTTGGACCCTGTCTTTAAAGTTATAGATCCC"
                        + "AGGCTTCTACACTTTAAGACTTTATTTGTTGTATTATATATCTTCCCTGAAACAGGATTT"
                        + "CAAGAATCATATTTCAGTTTTTTTAGTAATTATTTAAGTAAATTATTTATTCAAGTATTT"
                        + "GAGTAC" };

        // prepare the columns
        String[] columns = { WuBlastPlugin.COLUMN_ID, WuBlastPlugin.COLUMN_ROW,
                WuBlastPlugin.COLUMN_BLOCK, WuBlastPlugin.COLUMN_HEADER,
                WuBlastPlugin.COLUMN_FOOTER };
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < columns.length; i++) {
            map.put(columns[i], i);
        }

        // invoke the blast process
        IWsfPlugin processor = new WuBlastPlugin();
        try {
            String[][] result = processor.invoke(params, values, columns);

            // print out the result
            System.out.println("");
            for (int i = 0; i < result.length; i++) {
                System.out.println("================ " + result[i][0]
                        + " ================");
                for (String col : columns) {
                    System.out.println("------------ " + col + " ------------");
                    System.out.println(result[i][map.get(col)]);
                }
                System.out.println();
            }
        } catch (WsfServiceException ex) {
            // TODO Auto-generated catch block
            ex.printStackTrace();
            // System.err.println(ex);
            assertTrue(false);
        }
    }

    public static void main(String[] args) throws Exception {
        testInvoke();
    }
}
