package org.apidb.apicomplexa.wsfplugin.textsearch;

import java.util.Map;

import org.eupathdb.websvccommon.wsfplugin.textsearch.ResponseResultContainer;
import org.eupathdb.websvccommon.wsfplugin.textsearch.ResultContainer;
import org.eupathdb.websvccommon.wsfplugin.textsearch.SearchResult;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.WsfServiceException;

public class MergeResultContainer extends ResponseResultContainer implements
    ResultContainer {

  private final Map<String, SearchResult> commentResults;

  public MergeResultContainer(PluginResponse response, String[] orderedColumns,
      Map<String, SearchResult> commentResults) {
    super(response, orderedColumns);
    this.commentResults = commentResults;
  }

  @Override
  public void addResult(SearchResult result) throws WsfServiceException {
    String sourceId = result.getSourceId();
    // merge the result if it also exists in comment results
    if (commentResults.containsKey(sourceId)) {
      result.combine(commentResults.get(sourceId));
      // remove from cached
      commentResults.remove(sourceId);
    }

    // then send the result for the super class to process
    super.addResult(result);
  }

  public void processRemainingResults() throws WsfServiceException {
    // process the unprocessed results
    for (String sourceId : commentResults.keySet()) {
      super.addResult(commentResults.get(sourceId));
    }
    commentResults.clear();
  }
}
