/**
 * 
 */
package test.org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.apidb.apicomplexa.wsfplugin.motifsearch.MotifSearchPlugin;
import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author Jerric
 * @created Feb 2, 2006
 */
public class MotifSearchPluginTest extends TestCase {

    private static Logger logger = Logger.getLogger(MotifSearchPluginTest.class);

    public static void main(String[] args) {
        junit.textui.TestRunner.run(MotifSearchPluginTest.class);
    }

    /*
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
    }

    /*
     * Test method for
     * 'org.plasmodb.wsfplugin.motifsearch.MotifSearchPlugin.invoke(String[],
     * String[], String[])'
     */
    public void testInvoke() {
        // prepare parameters
        Map<String, String> params = new HashMap<String, String>();
        params.put(MotifSearchPlugin.PARAM_EXPRESSION, "CC6+RK");
        params.put(MotifSearchPlugin.PARAM_DATASET, "Pfalciparum/PfalciparumAnnotatedProteins_plasmoDB-5.0.fasta");

        // prepare columns
        String[] orderedColumns = { MotifSearchPlugin.COLUMN_GENE_ID,
                //MotifSearchPlugin.COLUMN_PROJECT_ID,
                MotifSearchPlugin.COLUMN_MATCH_COUNT,
                MotifSearchPlugin.COLUMN_LOCATIONS,
                MotifSearchPlugin.COLUMN_SEQUENCE };

        try {
            MotifSearchPlugin search = new MotifSearchPlugin();
            // invoke the plugin and get result back
            String[][] result = search.invoke("", params, orderedColumns);

            // print out results
            System.out.println("========= Motif Search JUnit Test ==========");
            // print parameters
            System.out.println("Parameters: ");
            for (String param : params.keySet()) {
                System.out.println("\t\t" + params + ": " + params.get(param));
            }
            System.out.println();
            // print results
            System.out.println(WsfPlugin.printArray(orderedColumns));
            System.out.println(WsfPlugin.printArray(result));
        } catch (WsfServiceException ex) {
            logger.error(ex);
            ex.printStackTrace();
            assertTrue(false);
        }
    }
}
