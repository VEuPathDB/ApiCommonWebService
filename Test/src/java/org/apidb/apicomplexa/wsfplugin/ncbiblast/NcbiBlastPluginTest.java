/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.ncbiblast;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apidb.apicomplexa.wsfplugin.ncbiblast.NcbiBlastPlugin;
import org.gusdb.wsf.plugin.IWsfPlugin;
import org.gusdb.wsf.plugin.WsfResult;
import org.gusdb.wsf.plugin.WsfServiceException;
import org.gusdb.wsf.util.Formatter;
import org.junit.Test;

/**
 * @author Jerric
 * @created Nov 2, 2005
 */
public class NcbiBlastPluginTest {

    private static Logger logger = Logger.getLogger(NcbiBlastPluginTest.class);

    @Test
    public void testInvoke() throws WsfServiceException {
        // prepare parameters
        Map<String, String> params = new HashMap<String, String>();
        params.put(NcbiBlastPlugin.PARAM_ALGORITHM, "blastn");
        params.put(NcbiBlastPlugin.PARAM_DATABASE_TYPE, "ESTs");
        params.put(NcbiBlastPlugin.PARAM_DATABASE_ORGANISM, "Pfalciparum");
        params.put("-e", "0.1");
        params.put("-v", "5");
        params.put("-b", "5");
        params.put(NcbiBlastPlugin.PARAM_SEQUENCE,
                "TTGGAAGCTTGTTCAGCCTGTTCAGCAGCTTTTTCAGCTTCTTCAGCAGCTTTTTCA");

        // prepare the columns
        String[] columns = { NcbiBlastPlugin.COLUMN_ID,
                NcbiBlastPlugin.COLUMN_ROW, NcbiBlastPlugin.COLUMN_BLOCK,
                NcbiBlastPlugin.COLUMN_HEADER, NcbiBlastPlugin.COLUMN_FOOTER };

        // invoke the blast process
        IWsfPlugin processor = new NcbiBlastPlugin();
        WsfResult wsfResult = processor.invoke("PlasmoDB", null, params, columns);

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
