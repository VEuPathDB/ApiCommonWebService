package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.util.ArrayList;
import java.util.List;

class Match {

  public String sourceId;
  public String projectId;

  /**
   * locations contains (xxx-yyy), (xxx-yyyy), ... sequence contains sequences
   * from matches, separated by a space (so it wraps in summary page)
   */
  public String locations;
  public int matchCount = 0;
  public String sequence;
  public List<String> matchSequences = new ArrayList<>();
  public String sequenceId;

  private String getKey() {
    return sourceId + projectId;
  }

  @Override
  public int hashCode() {
    return getKey().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null
        && obj instanceof Match
        && getKey().equals(((Match)obj).getKey());
  }
}