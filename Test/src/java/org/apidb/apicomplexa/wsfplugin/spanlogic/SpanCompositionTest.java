package org.apidb.apicomplexa.wsfplugin.spanlogic;

import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.COLUMN_PROJECT_ID;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.COLUMN_SOURCE_ID;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.COLUMN_WDK_WEIGHT;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_BEGIN_DIRECTION_PREFIX;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_BEGIN_OFFSET_PREFIX;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_BEGIN_PREFIX;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_END_DIRECTION_PREFIX;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_END_OFFSET_PREFIX;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_END_PREFIX;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_OPERATION;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_OUTPUT;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_SPAN_PREFIX;
import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.PARAM_STRAND;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.gusdb.wdk.controller.CConstants;
import org.gusdb.wdk.model.UnitTestHelper;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.jspwrap.WdkModelBean;
import org.gusdb.wdk.model.question.Question;
import org.gusdb.wdk.model.user.Step;
import org.gusdb.wdk.model.user.User;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.WsfResponse;
import org.gusdb.wsf.plugin.WsfServiceException;
import org.gusdb.wsf.util.Formatter;
import org.junit.Assert;
import org.junit.Test;

public class SpanCompositionTest {

    private static Logger logger = Logger.getLogger(SpanCompositionTest.class);

    private WdkModel wdkModel;
    private User user;
    private SpanCompositionPlugin plugin;
    private Map<String, String> steps;

    public SpanCompositionTest() throws Exception {
        wdkModel = UnitTestHelper.getModel();
        user = UnitTestHelper.getRegisteredUser();
        plugin = createPlugin();
        steps = new HashMap<String, String>();
    }

    @Test
    public void testSpanLogic() throws URISyntaxException, IOException,
            WsfServiceException, WdkModelException {
        List<SpanCompositionTestCase> testCases = loadTestCases("span-composition.test");

        for (SpanCompositionTestCase testCase : testCases) {
            logger.warn("++++++++++++++++++++ Test #" + testCase.id + ": " + testCase.description);
            PluginRequest request = createRequest(testCase);
            WsfResponse response = plugin.execute(request);
            String[][] results = response.getResult();
            validateResults(testCase, results);
        }
        boolean success = true;
        int count = 0;
        for (SpanCompositionTestCase testCase : testCases) {
            if (!testCase.success) {
                logger.warn("Test #" + testCase.id + " FAILED: " + testCase.description);
                logger.warn("============= EXPECT: "
                        + Formatter.printArray(testCase.expectedOutput));
                logger.warn("============= ACTUAL: "
                        + Formatter.printArray(testCase.actualOutput));
                success = false;
                count++;
            }
        }
        logger.info("Failed: " + count);
        Assert.assertTrue(success);
    }

    private List<SpanCompositionTestCase> loadTestCases(String resourceName)
            throws URISyntaxException, IOException, WsfServiceException {
        List<SpanCompositionTestCase> testCases = new ArrayList<SpanCompositionTestCase>();
        URL url = SpanCompositionTest.class.getResource(resourceName);
        if (url != null) {
            File file = new File(url.toURI());
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            int id = 1;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.length() == 0) continue;
                SpanCompositionTestCase testCase = new SpanCompositionTestCase(line);
                testCase.id = id++;
                testCases.add(testCase);
            }
            reader.close();
        }
        return testCases;
    }

    private PluginRequest createRequest(SpanCompositionTestCase testCase)
            throws WdkModelException {
        // prepare parameters
        Map<String, String> params = new HashMap<String, String>();

        params.put(PARAM_OPERATION, testCase.operator);

        params.put(PARAM_SPAN_PREFIX + "a", createGeneStep(testCase.inputA));
        params.put(PARAM_BEGIN_PREFIX + "a", testCase.beginA);
        params.put(PARAM_BEGIN_DIRECTION_PREFIX + "a", testCase.beginDirectionA);
        params.put(PARAM_BEGIN_OFFSET_PREFIX + "a", testCase.beginOffsetA);
        params.put(PARAM_END_PREFIX + "a", testCase.endA);
        params.put(PARAM_END_DIRECTION_PREFIX + "a", testCase.endDirectionA);
        params.put(PARAM_END_OFFSET_PREFIX + "a", testCase.endOffsetA);

        params.put(PARAM_SPAN_PREFIX + "b", createGeneStep(testCase.inputB));
        params.put(PARAM_BEGIN_PREFIX + "b", testCase.beginB);
        params.put(PARAM_BEGIN_DIRECTION_PREFIX + "b", testCase.beginDirectionB);
        params.put(PARAM_BEGIN_OFFSET_PREFIX + "b", testCase.beginOffsetB);
        params.put(PARAM_END_PREFIX + "b", testCase.endB);
        params.put(PARAM_END_DIRECTION_PREFIX + "b", testCase.endDirectionB);
        params.put(PARAM_END_OFFSET_PREFIX + "b", testCase.endOffsetB);

        params.put(PARAM_STRAND, testCase.strand);
        params.put(PARAM_OUTPUT, testCase.outputFrom);

        // prepare columns
        String[] columns = { COLUMN_PROJECT_ID, COLUMN_SOURCE_ID,
                COLUMN_WDK_WEIGHT };

        // prepare the request context
        Map<String, String> context = new HashMap<String, String>();
        context.put(Utilities.QUERY_CTX_USER, user.getSignature());

        PluginRequest request = new PluginRequest();
        request.setParams(params);
        request.setOrderedColumns(columns);
        request.setContext(context);

        return request;
    }

    private String createGeneStep(String[] input) throws WdkModelException {
        String key = Formatter.printArray(input).intern();
        if (steps.containsKey(key)) return steps.get(key);

        // IsolateQuestions.IsolateByIsolateId
        Question question = wdkModel.getQuestion("GeneQuestions.GeneByLocusTag");
        Map<String, String> params = new HashMap<String, String>();

        StringBuilder builder = new StringBuilder();
        for (String gene : input) {
            builder.append(gene + " ");
        }

        // isolate_id
        params.put("ds_gene_ids", builder.toString().trim());

        Step step = user.createStep(question, params, (String) null, false,
                true, 0);
        String sql = step.getAnswerValue().getIdSql();
        steps.put(key, sql);
        return sql;
    }

    private void validateResults(SpanCompositionTestCase testCase,
            String[][] actualResults) {
        Set<String> expected = new HashSet<String>();
        for (String value : testCase.expectedOutput) {
            expected.add(value);
        }

        Set<String> actual = new HashSet<String>();
        for (int i = 0; i < actualResults.length; i++) {
            String value = actualResults[i][1];
            actual.add(value);
        }
        testCase.actualOutput = new String[actual.size()];
        actual.toArray(testCase.actualOutput);

        for (String value : expected) {
            if (!actual.contains(value)) {
                testCase.success = false;
                return;
            }
        }
        for (String value : actual) {
            if (!expected.contains(value)) {
                testCase.success = false;
                return;
            }
        }
        testCase.success = true;
    }

    private SpanCompositionPlugin createPlugin() throws WsfServiceException {
        SpanCompositionPlugin plugin = new SpanCompositionPlugin();

        Map<String, Object> context = new HashMap<String, Object>();
        context.put(CConstants.WDK_MODEL_KEY, new WdkModelBean(wdkModel));
        plugin.initialize(context);

        return plugin;
    }
}
