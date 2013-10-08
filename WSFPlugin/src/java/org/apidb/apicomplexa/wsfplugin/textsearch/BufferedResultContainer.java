package org.apidb.apicomplexa.wsfplugin.textsearch;

import java.util.LinkedHashMap;
import java.util.Map;

import org.eupathdb.websvccommon.wsfplugin.textsearch.ResultContainer;
import org.eupathdb.websvccommon.wsfplugin.textsearch.SearchResult;

public class BufferedResultContainer implements ResultContainer {

  private final Map<String, SearchResult> results = new LinkedHashMap<>();
  
  @Override
  public void addResult(SearchResult result) {
    results.put(result.getSourceId(), result);
  }

  public Map<String, SearchResult> getResults() {
    return results;
  }

  @Override
  public boolean hasResult(String sourceId) {
    return results.containsKey(sourceId);
  }
}
