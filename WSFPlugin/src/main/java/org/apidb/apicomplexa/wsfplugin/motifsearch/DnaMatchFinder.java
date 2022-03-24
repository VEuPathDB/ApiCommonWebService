package org.apidb.apicomplexa.wsfplugin.motifsearch;

import org.apache.log4j.Logger;
import org.apidb.apicomplexa.wsfplugin.motifsearch.algorithm.BufferedDnaMotifFinder;
import org.apidb.apicomplexa.wsfplugin.motifsearch.algorithm.MotifMatch;
import org.gusdb.fgputil.functional.FunctionalInterfaces.ConsumerWithException;
import org.gusdb.fgputil.functional.FunctionalInterfaces.FunctionWithException;

import java.io.Reader;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DnaMatchFinder extends StreamingMatchFinder {
  private static final int BUFFER_SIZE = 65536;
  private static final int MAX_MATCH_LENGTH = 1024;

  private static final Logger LOG = Logger.getLogger(DnaMatchFinder.class);

  public DnaMatchFinder(MotifConfig config) {
    super(config);
  }

  @Override
  protected void findMatchesInSequence(
      String defLine,
      Pattern searchPattern,
      Reader sequence,
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
    String strand = deflineMatcher.group(2).intern().equals("-") ? "r" : "f";
    String organism = deflineMatcher.group(3).replace('_', ' ').intern();
    String projectId = orgToProjectId.apply(organism).intern();

    final List<MotifMatch> matches = BufferedDnaMotifFinder.match(sequence, searchPattern, _config.getContextLength(), BUFFER_SIZE, MAX_MATCH_LENGTH);
    final List<PluginMatch> pluginMatches = matches.stream()
            .map(match -> new PluginMatch(
                    match.getStartPos(),
                    match.getEndPos(),
                    projectId,
                    sequenceId,
                    1,
                    strand,
                    match.getLeadingContext(),
                    match.getTrailingContext(),
                    match.getMatch()))
            .collect(Collectors.toList());
    for (PluginMatch pluginMatch : pluginMatches) {
      consumer.accept(pluginMatch);
    }
  }
}
