package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.util.Map;

public class FindGenesWithSnpCharsTest extends FindPolymorphismsSearchTest {

  public FindGenesWithSnpCharsTest() throws Exception {
    super();
  }

  @Override
  public FindPolymorphismsPlugin getPlugin() {
    return new FindGenesWithSnpCharsPlugin();
  }
  
  @Override
  public void prepareParams(Map<String, String> params) {
    super.prepareParams(params);
    params.put(FindGenesWithSnpCharsPlugin.PARAM_SNP_CLASS, "unit test");
    params.put(FindGenesWithSnpCharsPlugin.PARAM_OCCURENCES_LOWER, "1");
    params.put(FindGenesWithSnpCharsPlugin.PARAM_OCCURENCES_UPPER, "1000");
    params.put(FindGenesWithSnpCharsPlugin.PARAM_DNDS_LOWER, ".1");
    params.put(FindGenesWithSnpCharsPlugin.PARAM_DNDS_UPPER, ".9");
    params.put(FindGenesWithSnpCharsPlugin.PARAM_DENSITY_LOWER, "1");
    params.put(FindGenesWithSnpCharsPlugin.PARAM_DENSITY_UPPER, "1000");
  }

  @Override
  public int getExpectedResultCount() { return 2; }

}
