package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apidb.apicomplexa.wsfplugin.MockProjectMapper;
import org.gusdb.fgputil.FormatUtil;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FindPolymorphismsSearchTest extends HsssTest {

  @SuppressWarnings("unused")
  private static final Logger logger = Logger.getLogger(FindPolymorphismsSearchTest.class.getName());

  public FindPolymorphismsSearchTest() throws Exception {
    super();
  }

  public FindPolymorphismsPlugin getPlugin() {
    return new FindPolymorphismsPlugin();
  }

  public void prepareParams(Map<String, String> params) {
    params.put(FindPolymorphismsPlugin.PARAM_ORGANISM, "Homo sapiens 123");
    params.put(FindPolymorphismsPlugin.PARAM_WEBSVCPATH, projectHome + "/ApiCommonWebService/HighSpeedSnpSearch/test/PROJECT_GOES_HERE");
    params.put(FindPolymorphismsPlugin.PARAM_READ_FREQ_PERCENT, "80");
    params.put(FindPolymorphismsPlugin.PARAM_STRAIN_LIST, "1,2,3,4");
    params.put(FindPolymorphismsPlugin.PARAM_MIN_PERCENT_KNOWNS, "20");
    params.put(FindPolymorphismsPlugin.PARAM_MIN_PERCENT_POLYMORPHISMS, "20");
  }

  public int getExpectedResultCount() { return 8; }

  @Test
  public void testSearch() throws Exception {
    FindPolymorphismsPlugin search = getPlugin();
    search.setOrganismNameForFiles("Hsapiens123");
    System.err.println("first" + properties);
    try {
      System.err.println("calling init");
      search.initialize(getContext());
      System.err.println("done calling init");

    } catch (NullPointerException ex) {
      // ignore missing wdk model
      //      throw ex;
    }
    search.setProjectMapper(new MockProjectMapper());

    // prepare parameters
    var params = new HashMap<String, String>();
    prepareParams(params);

    // invoke the plugin and get result back
    PluginRequest request = getRequest(params);
    PluginResponse response = getResponse();
    search.execute(request, response);

    // print results
    String[][] results = response.getPage(0);
    System.out.println(FormatUtil.printArray(results));

    Assertions.assertEquals(getExpectedResultCount(), results.length);
  }

  private PluginRequest getRequest(Map<String, String> params) {
    // prepare columns
    String[] columns = new String[] {
      FindPolymorphismsPlugin.COLUMN_SNP_SOURCE_ID,
      FindPolymorphismsPlugin.COLUMN_PROJECT_ID,
      FindPolymorphismsPlugin.COLUMN_PERCENT_OF_POLYMORPHISMS,
      FindPolymorphismsPlugin.COLUMN_PERCENT_OF_KNOWNS,
      FindPolymorphismsPlugin.COLUMN_PHENOTYPE };

    PluginRequest request = new PluginRequest();
    request.setParams(params);
    request.setOrderedColumns(columns);
    request.setContext(new HashMap<>());

    return request;
  }

}

