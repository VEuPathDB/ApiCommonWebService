/**
 * 
 */
package test.org.apidb.apicomplexa.wsfplugin.ncbiblast;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apidb.apicomplexa.wsfplugin.ncbiblast.NcbiBlastPlugin;
import org.gusdb.wsf.IWsfPlugin;
import org.gusdb.wsf.WsfServiceException;

/**
 * @author Jerric
 * @created Nov 2, 2005
 */
public class NcbiBlastPluginTest extends TestCase {

    /*
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
    }

    /*
     * Test method for 'NcbiProcessor.invoke(String[], String[], String[])'
     */
    public void testInvoke() {
        // prepare parameters
        String[] params = { NcbiBlastPlugin.PARAM_APPLICATION,
                NcbiBlastPlugin.PARAM_SEQUENCE, "-d" };
        String[] values = {
                "blastn",
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
                        + "CGATGCCGAACGTATTTTTGCCGAACTTTT", "c.parvum.nt" };

        // // no hits
        // String[] values = { "blastn", "QQQQQ", "c.parvum.nt" };

        // prepare the columns
        String[] columns = { NcbiBlastPlugin.COLUMN_ID, NcbiBlastPlugin.COLUMN_ROW,
                NcbiBlastPlugin.COLUMN_BLOCK, NcbiBlastPlugin.COLUMN_HEADER,
                NcbiBlastPlugin.COLUMN_FOOTER };
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < columns.length; i++) {
            map.put(columns[i], i);
        }

        // invoke the blast process
        IWsfPlugin processor = new NcbiBlastPlugin();
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
}
