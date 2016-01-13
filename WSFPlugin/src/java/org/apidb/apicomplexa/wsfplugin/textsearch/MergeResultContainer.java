package org.apidb.apicomplexa.wsfplugin.textsearch;

import java.util.Map;
import org.apache.log4j.Logger;

import org.eupathdb.websvccommon.wsfplugin.textsearch.ResponseResultContainer;
import org.eupathdb.websvccommon.wsfplugin.textsearch.ResultContainer;
import org.eupathdb.websvccommon.wsfplugin.textsearch.SearchResult;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;

public class MergeResultContainer extends ResponseResultContainer implements
    ResultContainer {

  private final Map<String, SearchResult> commentResults;
  private static final Logger logger = Logger.getLogger(TranscriptSearchPlugin.class);

  public MergeResultContainer(PluginResponse response, String[] orderedColumns,
      Map<String, SearchResult> commentResults) {
    super(response, orderedColumns);
    this.commentResults = commentResults;
  }

  @Override
  public void addResult(SearchResult result) throws PluginModelException, PluginUserException {
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

  public void processRemainingResults() throws PluginModelException, PluginUserException  {
    // process the unprocessed results
    for (String sourceId : commentResults.keySet()) {
      super.addResult(commentResults.get(sourceId));
    }
    commentResults.clear();
  }
}
