package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.util.ArrayList;
import java.util.List;

public class PluginMatch {

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
        && obj instanceof PluginMatch
        && getKey().equals(((PluginMatch)obj).getKey());
  }

  public PluginMatch() {}

  public PluginMatch(int start,
                     int stop,
                     String projectId,
                     String sequenceId,
                     int matchCount,
                     String strand,
                     String beforeContext,
                     String afterContext,
                     String motif) {
    this.projectId = projectId;
    this.matchCount = matchCount;
    final boolean reversed = strand.equals("r");
    if (reversed) {
      // NOT include the reverse strand hits	
      matchCount=  matchCount -1;
      // this.locations = AbstractMotifPlugin.formatLocation(motif.length(), start+1, stop, reversed);
    }
    else {
      this.locations = AbstractMotifPlugin.formatLocation(motif.length(), start, stop-1, reversed);
      this.sequenceId = sequenceId;
      this.sourceId = sequenceId + ":" + this.locations + ":" + strand;

      // create matching context
      if (beforeContext != null && !beforeContext.isBlank()) {
        this.matchSequences.add(motif);
        this.matchCount = matchCount;
        this.sequence = new StringBuilder()
            .append("...")
            .append(beforeContext)
            .append("<span class=\"" + AbstractMotifPlugin.MOTIF_STYLE_CLASS + "\">")
            .append(motif)
            .append("</span>")
            .append(afterContext)
            .append("...")
            .toString();
      }
    }
  }

}
