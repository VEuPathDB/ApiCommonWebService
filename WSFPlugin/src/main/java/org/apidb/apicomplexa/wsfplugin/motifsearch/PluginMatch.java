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
      this.locations = AbstractMotifPlugin.formatLocation(motif.length(), start+1, stop, reversed);
    }
    else {
      this.locations = AbstractMotifPlugin.formatLocation(motif.length(), start, stop-1, reversed);
    }
    this.sequenceId = sequenceId;
    this.sourceId = sequenceId + ":" + this.locations + ":" + strand;

    // create matching context
    StringBuilder context = new StringBuilder();
    if (beforeContext == null || beforeContext.isBlank()) {
      context.append("...");
      context.append(beforeContext);
    }

    this.matchSequences.add(motif);
    context.append("<span class=\"" + AbstractMotifPlugin.MOTIF_STYLE_CLASS + "\">");
    context.append(motif);
    context.append("</span>");

    if (beforeContext == null || beforeContext.isBlank()) {
      context.append(afterContext);
      context.append("...");
    }

    this.sequence = context.toString();
  }
}