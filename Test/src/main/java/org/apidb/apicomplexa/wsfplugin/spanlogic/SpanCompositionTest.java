package org.apidb.apicomplexa.wsfplugin.spanlogic;

import org.apache.log4j.Logger;
import org.gusdb.fgputil.FormatUtil;
import org.gusdb.wdk.controller.CConstants;
import org.gusdb.wdk.model.UnitTestHelper;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.jspwrap.WdkModelBean;
import org.gusdb.wdk.model.user.User;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import static org.apidb.apicomplexa.wsfplugin.spanlogic.SpanCompositionPlugin.*;

public class SpanCompositionTest {

  private static final Logger logger = Logger.getLogger(SpanCompositionTest.class);

  private final WdkModel wdkModel;
  private final User user;
  private final SpanCompositionPlugin plugin;
  private final Map<String, String> steps;

  public SpanCompositionTest() {
    wdkModel = UnitTestHelper.getModel();
    user = UnitTestHelper.getRegisteredUser();
    plugin = createPlugin();
    steps = new HashMap<>();
  }

  @Test
  public void testSpanLogic() throws Exception {
    List<SpanCompositionTestCase> testCases = loadTestCases("span-composition.test");

    for (SpanCompositionTestCase testCase : testCases) {
      logger.warn("++++++++++++++++++++ Test #" + testCase.id + ": "
          + testCase.description);
      PluginRequest request = createRequest(testCase);
      PluginResponse response = getResponse();
      plugin.execute(request, response);

      // print results
      String[][] results = response.getPage(0);
      validateResults(testCase, results);
    }
    boolean success = true;
    int count = 0;
    for (SpanCompositionTestCase testCase : testCases) {
      if (!testCase.success) {
        logger.warn("Test #" + testCase.id + " FAILED: " + testCase.description);
        logger.warn("============= EXPECT: "
            + FormatUtil.printArray(testCase.expectedOutput));
        logger.warn("============= ACTUAL: "
            + FormatUtil.printArray(testCase.actualOutput));
        success = false;
        count++;
      }
    }
    logger.info("Failed: " + count);
    Assertions.assertTrue(success);
  }

  private PluginResponse getResponse() throws IOException {
    File storageDir = File.createTempFile("temp/wsf", null);
    //noinspection ResultOfMethodCallIgnored
    storageDir.mkdirs();
    return new PluginResponse(storageDir, 0);
  }

  private List<SpanCompositionTestCase> loadTestCases(String resourceName)
      throws URISyntaxException, IOException, PluginUserException {
    var testCases = new ArrayList<SpanCompositionTestCase>();
    var url = SpanCompositionTest.class.getResource(resourceName);
    if (url != null) {
      var file = new File(url.toURI());
      var reader = new BufferedReader(new FileReader(file));
      String line;
      int id = 1;
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        if (line.length() == 0)
          continue;
        SpanCompositionTestCase testCase = new SpanCompositionTestCase(line);
        testCase.id = id++;
        testCases.add(testCase);
      }
      reader.close();
    }
    return testCases;
  }

  private PluginRequest createRequest(SpanCompositionTestCase testCase) {
    // prepare parameters
    var params = new HashMap<String, String>();

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
    var columns = { COLUMN_PROJECT_ID, COLUMN_SOURCE_ID, COLUMN_WDK_WEIGHT };

    // prepare the request context
    var context = new HashMap<String, String>();
    context.put(Utilities.QUERY_CTX_USER, user.getSignature());

    var request = new PluginRequest();
    request.setParams(params);
    request.setOrderedColumns(columns);
    request.setContext(context);

    return request;
  }

  private String createGeneStep(String[] input) {
    var key = FormatUtil.printArray(input).intern();
    if (steps.containsKey(key))
      return steps.get(key);

    // IsolateQuestions.IsolateByIsolateId
    var question = wdkModel.getQuestion("GeneQuestions.GeneByLocusTag");
    var params = new HashMap<String, String>();

    var builder = new StringBuilder();
    for (var gene : input) {
      builder.append(gene).append(" ");
    }

    // isolate_id
    params.put("ds_gene_ids", builder.toString().trim());

    var step = user.createStep(question, params, (String) null, false, true, 0);
    //noinspection ConstantConditions
    var sql = step.getAnswerValue().getIdSql();
    steps.put(key, sql);
    return sql;
  }

  private void validateResults(SpanCompositionTestCase testCase,
      String[][] actualResults) {
    var expected = new HashSet<String>();
    Collections.addAll(expected, testCase.expectedOutput);

    var actual = new HashSet<String>();
    for (var actualResult : actualResults) {
      var value = actualResult[1];
      actual.add(value);
    }
    testCase.actualOutput = new String[actual.size()];
    actual.toArray(testCase.actualOutput);

    for (var value : expected) {
      if (!actual.contains(value)) {
        testCase.success = false;
        return;
      }
    }
    for (var value : actual) {
      if (!expected.contains(value)) {
        testCase.success = false;
        return;
      }
    }
    testCase.success = true;
  }

  private SpanCompositionPlugin createPlugin() {
    SpanCompositionPlugin plugin = new SpanCompositionPlugin();

    var context = new HashMap<String, Object>();
    context.put(CConstants.WDK_MODEL_KEY, new WdkModelBean(wdkModel));
    plugin.initialize(context);

    return plugin;
  }
}
