package org.apidb.apicomplexa.wsfplugin.spanlogic;

import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.*;

import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.gusdb.wdk.controller.CConstants;
import org.gusdb.wdk.model.Question;
import org.gusdb.wdk.model.UnitTestHelper;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.WdkUserException;
import org.gusdb.wdk.model.jspwrap.WdkModelBean;
import org.gusdb.wdk.model.query.ProcessQueryInstance;
import org.gusdb.wdk.model.user.Step;
import org.gusdb.wdk.model.user.User;
import org.gusdb.wsf.plugin.WsfRequest;
import org.gusdb.wsf.plugin.WsfResponse;
import org.gusdb.wsf.plugin.WsfServiceException;
import org.gusdb.wsf.util.Formatter;
import org.json.JSONException;
import org.junit.Assert;
import org.junit.Test;

public class SpanCompositionTest {

    private static Logger logger = Logger.getLogger(SpanCompositionTest.class);

    private WdkModel wdkModel;
    private User user;
    private WsfRequest request;
    private SpanCompositionPlugin plugin;

    private String[] pureOverlapGenes = { "PFC0965w", "PFD0540c", "PFI1425w", "PFC0271c" };
    private String[] pureOverlapIsolates = { "G37988", "G37943", "G44484", "G37806" };

    private String[] containGenes = {"PFI0180w", "PFI0400c", "PFI0235w", "PFI0265c"};
    private String[] containIsolates = {"G37837", "G44516", "G44518", "G44510"};

    // none exist.
    private String[] containedGenes = {};
    private String[] containedIsolates = {};

    private String[] nearByGenes = {"PFI0875w", "PF13_0305", "PFI0135c", "PFI0960w", "PFI0415c"};
    private String[] nearByIsolates = {"G37836", "G37829", "G44522", "G44494", "G44512"};

    private String[] diffSeqGenes = {"PVX_118415", "PKH_145590", "PKH_146330", "PF14_0534"};
    private String[] diffSeqIsolates = {"G41070", "G41068", "G41073", "G44620"};

    public SpanCompositionTest() throws Exception {
        wdkModel = UnitTestHelper.getModel();
        user = UnitTestHelper.getRegisteredUser();
        plugin = createPlugin();
        request = createRequest();
    }

    @Test
    public void testGenesOverlapZeroOffset() throws Exception {
        logger.info("====== TEST genes overlap no offset ======");
        Map<String, String> params = request.getParams();
        params.put(PARAM_OPERATION, PARAM_VALUE_OVERLAP);
        request.setParams(params);

        // invoke the plugin and get result back
        WsfResponse response = plugin.execute(request);

        logger.info("Result Message: " + response.getMessage());
        logger.info("Result Signal: " + response.getSignal());
        
        Set<String> ids = new HashSet<String>();
        String[][] results = response.getResult();
        for (String[] record : results) {
            String source_id = record[1];
            ids.add(source_id);
        }
        
        // print results
        logger.info(Formatter.printArray(results));
   
        // it should include all pure-overlap, contains, and contained by genes
        for(String id : pureOverlapGenes) {
            Assert.assertTrue(ids.contains(id));
        }
        for(String id : containGenes) {
            Assert.assertTrue(ids.contains(id));
        }
        for(String id : containedGenes) {
            Assert.assertTrue(ids.contains(id));
        }
        
        // it won't include adjacent genes, nor genes from difference sequences
        for(String id : nearByGenes) {
            Assert.assertFalse(ids.contains(id));
        }
        for(String id : diffSeqGenes) {
            Assert.assertFalse(ids.contains(id));
        }
    }

    @Test
    public void testIsolatesOverlapZeroOffset() throws Exception {
        logger.info("====== TEST isolates overlap no offset ======");
        Map<String, String> params = request.getParams();
        params.put(PARAM_OPERATION, PARAM_VALUE_OVERLAP);
        params.put(PARAM_OUTPUT, PARAM_VALUE_OUTPUT_B);
        request.setParams(params);

        // invoke the plugin and get result back
        WsfResponse response = plugin.execute(request);

        logger.info("Result Message: " + response.getMessage());
        logger.info("Result Signal: " + response.getSignal());
        
        Set<String> ids = new HashSet<String>();
        String[][] results = response.getResult();
        for (String[] record : results) {
            String source_id = record[1];
            ids.add(source_id);
        }
        
        // print results
        logger.info(Formatter.printArray(results));
        
        // it should include all pure-overlap, contains, and contained by isolates
        for(String id : pureOverlapIsolates) {
            Assert.assertTrue(ids.contains(id));
        }
        for(String id : containIsolates) {
            Assert.assertTrue(ids.contains(id));
        }
        for(String id : containedIsolates) {
            Assert.assertTrue(ids.contains(id));
        }
        
        // it won't include adjacent isolates, nor isolates from difference sequences
        for(String id : nearByIsolates) {
            Assert.assertFalse(ids.contains(id));
        }
        for(String id : diffSeqIsolates) {
            Assert.assertFalse(ids.contains(id));
        }
    }

    @Test
    public void testGeneContainsIsolate() throws Exception {
        logger.info("====== TEST gene contains isolate ======");
        Map<String, String> params = request.getParams();
        params.put(PARAM_OPERATION, PARAM_VALUE_A_CONTAIN_B);
        request.setParams(params);

        // invoke the plugin and get result back
        WsfResponse response = plugin.execute(request);

        logger.info("Result Message: " + response.getMessage());
        logger.info("Result Signal: " + response.getSignal());
        
        Set<String> ids = new HashSet<String>();
        String[][] results = response.getResult();
        for (String[] record : results) {
            String source_id = record[1];
            ids.add(source_id);
        }
        
        // print results
        logger.info(Formatter.printArray(results));
   
        // it should include contains
        for(String id : containGenes) {
            Assert.assertTrue(ids.contains(id));
        }
        
        // it won't include any other genes
        for(String id : pureOverlapGenes) {
            Assert.assertFalse(ids.contains(id));
        }
        for(String id : containedGenes) {
            Assert.assertFalse(ids.contains(id));
        }
        for(String id : nearByGenes) {
            Assert.assertFalse(ids.contains(id));
        }
        for(String id : diffSeqGenes) {
            Assert.assertFalse(ids.contains(id));
        }
    }

    @Test
    public void testIsolateContainedByGene() throws Exception {
        logger.info("====== TEST isolate contains gene ======");
        Map<String, String> params = request.getParams();
        params.put(PARAM_OPERATION, PARAM_VALUE_A_CONTAIN_B);
        params.put(PARAM_OUTPUT, PARAM_VALUE_OUTPUT_B);
        request.setParams(params);

        // invoke the plugin and get result back
        WsfResponse response = plugin.execute(request);

        logger.info("Result Message: " + response.getMessage());
        logger.info("Result Signal: " + response.getSignal());
        
        Set<String> ids = new HashSet<String>();
        String[][] results = response.getResult();
        for (String[] record : results) {
            String source_id = record[1];
            ids.add(source_id);
        }
        
        // print results
        logger.info(Formatter.printArray(results));
        
        // it should include all pure-overlap, contains, and contained by isolates
        for(String id : containIsolates) {
            Assert.assertTrue(ids.contains(id));
        }
        
        // it won't include adjacent isolates, nor isolates from difference sequences
        for(String id : pureOverlapIsolates) {
            Assert.assertFalse(ids.contains(id));
        }
        for(String id : containedIsolates) {
            Assert.assertFalse(ids.contains(id));
        }
        for(String id : nearByIsolates) {
            Assert.assertFalse(ids.contains(id));
        }
        for(String id : diffSeqIsolates) {
            Assert.assertFalse(ids.contains(id));
        }
    }

    private SpanCompositionPlugin createPlugin() throws WsfServiceException {
        SpanCompositionPlugin plugin = new SpanCompositionPlugin();

        Map<String, Object> context = new HashMap<String, Object>();
        context.put(CConstants.WDK_MODEL_KEY, new WdkModelBean(wdkModel));
        plugin.initialize(context);

        return plugin;
    }

    private WsfRequest createRequest() throws Exception {
        Step geneStep = createGeneStep();
        Step isolateStep = createIsolateStep();

        // prepare parameters
        Map<String, String> params = new HashMap<String, String>();

        params.put(PARAM_OPERATION, PARAM_VALUE_OVERLAP);

        String spanA = Integer.toString(geneStep.getDisplayId());
        params.put(PARAM_SPAN + "a", spanA);
        params.put(PARAM_SPAN_BEGIN + "a", PARAM_VALUE_START);
        params.put(PARAM_SPAN_BEGIN_DIRECTION + "a", PARAM_VALUE_DOWNSTREAM);
        params.put(PARAM_SPAN_BEGIN_OFFSET + "a", "0");
        params.put(PARAM_SPAN_END + "a", PARAM_VALUE_STOP);
        params.put(PARAM_SPAN_END_DIRECTION + "a", PARAM_VALUE_DOWNSTREAM);
        params.put(PARAM_SPAN_END_OFFSET + "a", "0");

        String spanB = Integer.toString(isolateStep.getDisplayId());
        params.put(SpanCompositionPlugin.PARAM_SPAN + "b", spanB);
        params.put(PARAM_SPAN_BEGIN + "b", PARAM_VALUE_START);
        params.put(PARAM_SPAN_BEGIN_DIRECTION + "b", PARAM_VALUE_DOWNSTREAM);
        params.put(PARAM_SPAN_BEGIN_OFFSET + "b", "0");
        params.put(PARAM_SPAN_END + "b", PARAM_VALUE_STOP);
        params.put(PARAM_SPAN_END_DIRECTION + "b", PARAM_VALUE_DOWNSTREAM);
        params.put(PARAM_SPAN_END_OFFSET + "b", "0");

        params.put(PARAM_OUTPUT, PARAM_VALUE_OUTPUT_A);

        // prepare columns
        String[] columns = { COLUMN_PROJECT_ID, COLUMN_SOURCE_ID,
                COLUMN_WDK_WEIGHT };

        // prepare the request context
        Map<String, String> context = new HashMap<String, String>();
        context.put(ProcessQueryInstance.CTX_USER, user.getSignature());

        WsfRequest request = new WsfRequest();
        request.setParams(params);
        request.setOrderedColumns(columns);
        request.setContext(context);

        return request;
    }

    private Step createGeneStep() throws WdkUserException, WdkModelException,
            NoSuchAlgorithmException, SQLException, JSONException {
        Question question = wdkModel.getQuestion("GeneQuestions.GeneByLocusTag");
        Map<String, String> params = new HashMap<String, String>();

        StringBuilder builder = new StringBuilder();
        for (String gene : pureOverlapGenes) {
            builder.append(gene + " ");
        }
        for (String gene : containGenes) {
            builder.append(gene + " ");
        }
        for (String gene : containedGenes) {
            builder.append(gene + " ");
        }
        for (String gene : nearByGenes) {
            builder.append(gene + " ");
        }
        for (String gene : diffSeqGenes) {
            builder.append(gene + " ");
        }

        params.put("ds_gene_ids", builder.toString().trim());

        return user.createStep(question, params, (String) null, false, true, 0);
    }

    private Step createIsolateStep() throws WdkUserException,
            WdkModelException, NoSuchAlgorithmException, SQLException,
            JSONException {
        Question question = wdkModel.getQuestion("IsolateQuestions.IsolateByIsolateId");
        Map<String, String> params = new HashMap<String, String>();
 
        StringBuilder builder = new StringBuilder();
        for (String isolate : pureOverlapIsolates) {
            builder.append(isolate + " ");
        }
        for (String isolate : containIsolates) {
            builder.append(isolate + " ");
        }
        for (String isolate : containedIsolates) {
            builder.append(isolate + " ");
        }
        for (String isolate : nearByIsolates) {
            builder.append(isolate + " ");
        }
        for (String isolate : diffSeqIsolates) {
            builder.append(isolate + " ");
        }

        params.put("isolate_id",  builder.toString().trim());

        return user.createStep(question, params, (String) null, false, true, 0);
    }
}
