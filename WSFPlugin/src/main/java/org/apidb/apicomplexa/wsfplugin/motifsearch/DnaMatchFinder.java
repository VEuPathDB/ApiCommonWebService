package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.gusdb.fgputil.functional.FunctionalInterfaces.ConsumerWithException;
import org.gusdb.fgputil.functional.FunctionalInterfaces.FunctionWithException;

public class DnaMatchFinder extends HighMemoryMatchFinder {

  private static final Logger LOG = Logger.getLogger(DnaMatchFinder.class);

  public DnaMatchFinder(MotifConfig config) {
    super(config);
  }

  @Override
  protected void findMatchesInSequence(
      String defLine,
      Pattern searchPattern,
      String sequence,
      ConsumerWithException<PluginMatch> consumer,
      FunctionWithException<String, String> orgToProjectId) throws Exception {

    Matcher deflineMatcher = _config.getDeflinePattern().matcher(defLine);
    if (!deflineMatcher.find()) {
      LOG.warn("Invalid defline: " + defLine);
      return;
    }
    // the sequence id has to be in group(1),
    // strand info has to be in group(2)
    // organism has to be in group(3),
    String sequenceId = deflineMatcher.group(1).intern();
    String strand = deflineMatcher.group(2).intern();
    String organism = deflineMatcher.group(3).replace('_', ' ').intern();
    String projectId = orgToProjectId.apply(organism).intern();

    int length = sequence.length();
    strand = strand.equals("-") ? "r" : "f";
    boolean reversed = (strand.equals("r"));
    int contextLength = _config.getContextLength();

    Matcher matcher = searchPattern.matcher(sequence);
    while (matcher.find()) {
      int start = matcher.start();
      int stop = matcher.end();
      PluginMatch match = new PluginMatch();
      match.projectId = projectId;
      match.matchCount = 1;
      if (strand.equals("r")) {
        match.locations = AbstractMotifPlugin.formatLocation(length, start+1, stop, reversed);
      }
      else {
        match.locations = AbstractMotifPlugin.formatLocation(length, start, stop-1, reversed);
      }
      match.sequenceId = sequenceId;
      match.sourceId = sequenceId + ":" + match.locations + ":" + strand;

      // create matching context
      StringBuilder context = new StringBuilder();
      int begin = Math.max(0, start - contextLength);
      if (begin > 0)
        context.append("...");
      if (begin < start){
        context.append(sequence.substring(begin, start));
      }

      String motif = sequence.substring(matcher.start(), matcher.end());
      match.matchSequences.add(motif);

      context.append("<span class=\"" + AbstractMotifPlugin.MOTIF_STYLE_CLASS + "\">");
      context.append(motif);
      context.append("</span>");
      int end = Math.min(sequence.length(), stop + contextLength);
      if (end > stop)
        context.append(sequence.substring(stop, end));
      if (end < sequence.length())
        context.append("...");

      match.sequence = context.toString();
      consumer.accept(match);
    }
  }

}
