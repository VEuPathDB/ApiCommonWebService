package org.apidb.apicomplexa.wsfplugin.textsearch;

import org.eupathdb.websvccommon.wsfplugin.textsearch.SearchResult;

public class TranscriptSearchResult extends SearchResult {
  
  private String geneSourceId;
  
  public TranscriptSearchResult(String geneSourceId, String transcriptSourceId, float maxScore, String fieldsMatched, String projectId) {
    super(transcriptSourceId, projectId, maxScore, fieldsMatched);
    this.geneSourceId = geneSourceId;
  }

  @Override
  public String getGeneSourceId() {
    return geneSourceId;
    }

  @Override
  public String getPrimaryId() {
    return geneSourceId;
  }



  @Override
  public void combine(SearchResult other) {
    super.combine(other);
    if (getSourceId() == null) setSourceId(other.getSourceId());
  }

  @Override
  public int compareTo(SearchResult other) {
    if (other.getMaxScore() > getMaxScore() || (other.getMaxScore() == getMaxScore() && geneSourceId.compareTo(other.getGeneSourceId()) < 0)) {
      return -1;
      // the next match condition is redundant with the else clause.
      // } else if(other.getMaxScore() < maxScore || (other.getMaxScore() == maxScore && sourceId.compareTo(other.getSourceId()) > 0)) {
      // return 1;
    } else {
      return 1;
    }
  }


}
