package org.apidb.apicomplexa.wsfplugin.spanlogic;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apidb.apicomplexa.wsfplugin.motifsearch.MotifSearchPlugin;
import org.apidb.apicomplexa.wsfplugin.motifsearch.MotifSearchPluginTest;
import org.gusdb.wsf.plugin.WsfRequest;
import org.gusdb.wsf.plugin.WsfResponse;
import org.gusdb.wsf.plugin.WsfServiceException;
import org.gusdb.wsf.util.Formatter;
import org.junit.Test;

public class SpanCompositionTest {

    private static Logger logger = Logger.getLogger(MotifSearchPluginTest.class);

    @Test
    public void testInvoke() throws WsfServiceException {
        // prepare parameters
        Map<String, String> params = new HashMap<String, String>();
        
        
        
        
        params.put(SpanCompositionPlugin.PARAM_OPERATION, "CC6+RK");
        params.put(MotifSearchPlugin.PARAM_DATASET,
                "Pfalciparum/PfalciparumAnnotatedProteins_plasmoDB-5.0.fasta");

        // prepare columns
        String[] columns = {
                MotifSearchPlugin.COLUMN_GENE_ID,
                // MotifSearchPlugin.COLUMN_PROJECT_ID,
                MotifSearchPlugin.COLUMN_MATCH_COUNT,
                MotifSearchPlugin.COLUMN_LOCATIONS,
                MotifSearchPlugin.COLUMN_SEQUENCE };

        WsfRequest request = new WsfRequest();
        request.setParams(params);
        request.setOrderedColumns(columns);
        request.setContext(new HashMap<String, String>());

        MotifSearchPlugin search = new MotifSearchPlugin();
        // invoke the plugin and get result back
        WsfResponse wsfResult = search.execute(request);

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
