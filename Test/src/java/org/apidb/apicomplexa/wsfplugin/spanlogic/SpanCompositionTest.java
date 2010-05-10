package org.apidb.apicomplexa.wsfplugin.spanlogic;

import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.COLUMN_PROJECT_ID;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.COLUMN_SOURCE_ID;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.COLUMN_WDK_WEIGHT;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_OPERATION;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_OUTPUT;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_SPAN;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_SPAN_BEGIN;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_SPAN_BEGIN_DIRECTION;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_SPAN_BEGIN_OFFSET;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_SPAN_END;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_SPAN_END_DIRECTION;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_SPAN_END_OFFSET;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_VALUE_DOWNSTREAM;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_VALUE_OUTPUT_A;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_VALUE_OVERLAP;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_VALUE_START;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_VALUE_STOP;

import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

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
import org.gusdb.wsf.util.Formatter;
import org.json.JSONException;
import org.junit.Test;

public class SpanCompositionTest {

    private static Logger logger = Logger.getLogger(SpanCompositionTest.class);

    private WdkModel wdkModel;
    private User user;
    private WsfRequest request;
    private SpanCompositionPlugin plugin;

    public SpanCompositionTest() throws Exception {
        wdkModel = UnitTestHelper.getModel();
        user = UnitTestHelper.getRegisteredUser();
        plugin = createPlugin();
        request = createRequest();
    }

    @Test
    public void testOverlapZeroOffset() throws Exception {
        logger.info("====== TEST overlap no offset ======");
        Map<String, String> params = request.getParams();
        params.put(PARAM_OPERATION, PARAM_VALUE_OVERLAP);

        // invoke the plugin and get result back
        WsfResponse wsfResult = plugin.execute(request);

        logger.info("Result Message: " + wsfResult.getMessage());
        logger.info("Result Signal: " + wsfResult.getSignal());

        // print results
        logger.info(Formatter.printArray(wsfResult.getResult()));
    }

    private SpanCompositionPlugin createPlugin() {
        SpanCompositionPlugin plugin = new SpanCompositionPlugin();

        Map<String, Object> context = new HashMap<String, Object>();
        context.put(CConstants.WDK_MODEL_KEY, new WdkModelBean(wdkModel));
        plugin.setContext(context);

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
        params.put("ds_gene_ids", "PF10_0346 PFC0365w PFC0710w-a PFC0770c PFD0125c PFA0120c");

        return user.createStep(question, params, (String) null, false, true, 0);
    }

    private Step createIsolateStep() throws WdkUserException,
            WdkModelException, NoSuchAlgorithmException, SQLException,
            JSONException {
        Question question = wdkModel.getQuestion("IsolateQuestions.IsolateByIsolateId");
        Map<String, String> params = new HashMap<String, String>();
        params.put("isolate_id", "G44639 G37983 G37950 G41031 G37936 G42735");

        return user.createStep(question, params, (String) null, false, true, 0);
    }
}
