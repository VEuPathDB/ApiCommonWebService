/**
 * 
 */
package test.org.apidb.apicomplexa.wsfplugin.ncbiblast;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.apidb.apicomplexa.wsfplugin.ncbiblast.NcbiBlastPlugin;
import org.gusdb.wsf.plugin.IWsfPlugin;
import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author Jerric
 * @created Nov 2, 2005
 */
public class NcbiBlastPluginTest extends TestCase {

    private static Logger logger = Logger.getLogger(NcbiBlastPluginTest.class);

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
        Map<String, String> params = new HashMap<String, String>();
        params.put(NcbiBlastPlugin.PARAM_APPLICATION, "blastn");
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
        params.put("-d", "c.parvum.nt");

        // prepare the columns
        String[] columns = { NcbiBlastPlugin.COLUMN_ID,
                NcbiBlastPlugin.COLUMN_ROW, NcbiBlastPlugin.COLUMN_BLOCK,
                NcbiBlastPlugin.COLUMN_HEADER, NcbiBlastPlugin.COLUMN_FOOTER };

        // invoke the blast process
        try {
            IWsfPlugin processor = new NcbiBlastPlugin();
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
}
