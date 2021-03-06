package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.util.Map;

public class FindPolymorphismsWithSeqFilterSearchTest extends FindPolymorphismsSearchTest {

  public FindPolymorphismsWithSeqFilterSearchTest() throws Exception {
    super();
  }

  @Override
  public FindPolymorphismsPlugin getPlugin() {
    return new FindPolymorphismsWithSeqFilterPlugin();
  }
  
  @Override
  public void prepareParams(Map<String, String> params) {
    super.prepareParams(params);
    params.put(FindPolymorphismsWithSeqFilterPlugin.PARAM_CHROMOSOME, "");
    params.put(FindPolymorphismsWithSeqFilterPlugin.PARAM_SEQUENCE, "c100");
    params.put(FindPolymorphismsWithSeqFilterPlugin.PARAM_START_POINT, "21");
    params.put(FindPolymorphismsWithSeqFilterPlugin.PARAM_END_POINT, "25");
  }

  @Override
  public int getExpectedResultCount() { return 1; }

}
