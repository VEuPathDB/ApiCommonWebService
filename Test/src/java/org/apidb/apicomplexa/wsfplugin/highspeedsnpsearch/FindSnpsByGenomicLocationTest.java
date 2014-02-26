package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.util.Map;

public class FindSnpsByGenomicLocationTest extends FindPolymorphismsSearchTest {

  public FindSnpsByGenomicLocationTest() throws Exception {
    super();
  }

  public FindPolymorphismsPlugin getPlugin() {
    return new FindSnpsByGenomicLocationPlugin();
  }
  
  public void prepareParams(Map<String, String> params) {
    super.prepareParams(params);
    params.put(FindSnpsByGenomicLocationPlugin.PARAM_GENES_DATASET, "unit test");
  }

  public int getExpectedResultCount() { return 2; }

}
