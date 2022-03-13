package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.gusdb.fgputil.functional.FunctionalInterfaces.ConsumerWithException;
import org.gusdb.fgputil.functional.FunctionalInterfaces.FunctionWithException;

public class AAMatchFinder extends HighMemoryMatchFinder {

  private static final Logger LOG = Logger.getLogger(AAMatchFinder.class);

  public AAMatchFinder(MotifConfig config) {
    super(config);
  }

  @Override
  protected void findMatchesInSequence(
      String defLine,
      Pattern searchPattern,
      String sequence,
      ConsumerWithException<Match> consumer,
      FunctionWithException<String, String> orgToProjectId) throws Exception {

    // parse the headline
    Matcher deflineMatcher = _config.getDeflinePattern().matcher(defLine);
    if (!deflineMatcher.find()) {
      LOG.warn("Invalid defline: " + defLine + " Against Pattern "
          + _config.getDeflinePattern().pattern());
      return;
    }

    // the gene source id has to be in group(1),
    // organism has to be in group(2),
    String sourceId = deflineMatcher.group(1);
    String organism = deflineMatcher.group(2).replace('_', ' ');

    // workaround: trim "-p1" suffix to turn protein ID into transcript ID
    // sourceId = sourceId.replace("-p1", "");

    Match match = new Match();
    match.sourceId = sourceId;
    match.projectId = orgToProjectId.apply(organism);

    StringBuffer sbLoc = new StringBuffer();
    StringBuffer sbSeq = new StringBuffer();
    int prev = 0;
    int contextLength = _config.getContextLength();

    Matcher matcher = searchPattern.matcher(sequence);
    boolean longLoc = false, longSeq = false;
    while (matcher.find()) {
      // add locations only while we have room.
      if (!longLoc) {
        String location = AbstractMotifPlugin.formatLocation(0, matcher.start(), matcher.end() - 1, false);
        location = "(" + location + ")";
        if (sbLoc.length() + location.length() >= 3997) {
          sbLoc.append("...");
          longLoc = true;
        } else {
          if (sbLoc.length() != 0) sbLoc.append(", ");
          sbLoc.append(location);
        }
      }

      // add sequences only while we have room.
      if (!longSeq) {
        StringBuilder seq = new StringBuilder();
        // obtain the context sequence
        if ((matcher.start() - prev) <= (contextLength * 2)) {
          // no need to trim
          seq.append(sequence.substring(prev, matcher.start()));
        } else { // need to trim some
          if (prev != 0)
            seq.append(sequence.substring(prev, prev + contextLength));
          seq.append("... ");
          seq.append(sequence.substring(matcher.start() - contextLength,
              matcher.start()));
        }
        String motif = sequence.substring(matcher.start(), matcher.end());
        match.matchSequences.add(motif);

        seq.append("<span class=\"" + AbstractMotifPlugin.MOTIF_STYLE_CLASS + "\">");
        seq.append(motif);
        seq.append("</span>");

        // determine if we have enough space for the new sequence
        if (sbSeq.length() + seq.length() >= 3997) {
          sbSeq.append("...");
          longSeq = true;
        } else {
          sbSeq.append(seq);
        }
      }

      prev = matcher.end();
      match.matchCount++;
    }

    if (match.matchCount == 0) return;

    // grab the last context
    if (!longSeq) {
      String remain = ((prev + contextLength) < sequence.length()) 
          ? sequence.substring(prev, prev + contextLength) + "..."
          : sequence.substring(prev);
      if (remain.length() + sbSeq.length() < 4000)
        sbSeq.append(remain);
    }

    match.locations = sbLoc.toString();
    match.sequence = sbSeq.toString();
    consumer.accept(match);
  }

}
