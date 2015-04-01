package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apidb.apicomplexa.wsfplugin.MockProjectMapper;
import org.gusdb.fgputil.FormatUtil;
import org.gusdb.wsf.common.WsfRequest;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.junit.Assert;
import org.junit.Test;

public class FindMajorAllelesSearchTest extends HsssTest {

  @SuppressWarnings("unused")
  private static final Logger logger = Logger.getLogger(FindMajorAllelesSearchTest.class.getName());

  public FindMajorAllelesSearchTest() throws Exception {
    super();
  }

  @Test
  public void testSearch() throws Exception {
    FindMajorAllelesPlugin search = new FindMajorAllelesPlugin();
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
    Map<String, String> params = new HashMap<String, String>();
    params.put(FindMajorAllelesPlugin.PARAM_ORGANISM, "Homo sapiens 123");
    params.put(FindMajorAllelesPlugin.PARAM_WEBSVCPATH, projectHome + "/ApiCommonWebService/HighSpeedSnpSearch/test/PROJECT_GOES_HERE");
    params.put(FindMajorAllelesPlugin.PARAM_READ_FREQ_PERCENT_A, "80");
    params.put(FindMajorAllelesPlugin.PARAM_STRAIN_LIST_A, "1,2");
    params.put(FindMajorAllelesPlugin.PARAM_MIN_PERCENT_KNOWNS_A, "20");
    params.put(FindMajorAllelesPlugin.PARAM_MIN_PERCENT_MAJOR_ALLELES_A, "80"); 
    params.put(FindMajorAllelesPlugin.PARAM_READ_FREQ_PERCENT_B, "80");
    params.put(FindMajorAllelesPlugin.PARAM_STRAIN_LIST_B, "3,4");
    params.put(FindMajorAllelesPlugin.PARAM_MIN_PERCENT_KNOWNS_B, "20");
    params.put(FindMajorAllelesPlugin.PARAM_MIN_PERCENT_MAJOR_ALLELES_B, "80");

    // invoke the plugin and get result back
    PluginRequest request = getRequest(params);
    PluginResponse response = getResponse();
    search.execute(request, response);

    // print results
    String[][] results = response.getPage(0);
    System.out.println(FormatUtil.printArray(results));

    Assert.assertEquals(1, results.length);
  }

  private PluginRequest getRequest(Map<String, String> params) {
    // prepare columns
    String[] columns = new String[] { 
      FindMajorAllelesPlugin.COLUMN_SNP_SOURCE_ID,
      FindMajorAllelesPlugin.COLUMN_PROJECT_ID,
      FindMajorAllelesPlugin.COLUMN_MAJOR_ALLELE_A,
      FindMajorAllelesPlugin.COLUMN_MAJOR_ALLELE_PCT_A,
      FindMajorAllelesPlugin.COLUMN_TRIALLELIC_A,
      FindMajorAllelesPlugin.COLUMN_MAJOR_PRODUCT_A,
      FindMajorAllelesPlugin.COLUMN_MAJOR_PRODUCT_VARIABLE_A,
      FindMajorAllelesPlugin.COLUMN_MAJOR_ALLELE_B,
      FindMajorAllelesPlugin.COLUMN_MAJOR_ALLELE_PCT_B,
      FindMajorAllelesPlugin.COLUMN_TRIALLELIC_B,
      FindMajorAllelesPlugin.COLUMN_MAJOR_PRODUCT_B,
      FindMajorAllelesPlugin.COLUMN_MAJOR_PRODUCT_VARIABLE_B,
    };

    PluginRequest request = new PluginRequest();
    request.setParams(params);
    request.setOrderedColumns(columns);
    request.setContext(new HashMap<String, String>());

    return request;
  }
}
 
