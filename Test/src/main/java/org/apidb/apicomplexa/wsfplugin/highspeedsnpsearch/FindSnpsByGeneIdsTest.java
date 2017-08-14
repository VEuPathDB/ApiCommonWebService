package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.util.Map;

public class FindSnpsByGeneIdsTest extends FindPolymorphismsSearchTest {

  public FindSnpsByGeneIdsTest() throws Exception {
    super();
  }

  @Override
  public FindPolymorphismsPlugin getPlugin() {
    return new FindSnpsByGeneIdsPlugin();
  }
  
  @Override
  public void prepareParams(Map<String, String> params) {
    super.prepareParams(params);
    params.put(FindSnpsByGeneIdsPlugin.PARAM_GENES_DATASET, "unit test");
  }

  @Override
  public int getExpectedResultCount() { return 2; }

}
