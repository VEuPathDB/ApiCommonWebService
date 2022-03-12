package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;

/**
 * Superclass for Orf and Protein motif plugins
 *
 * geneID could be an ORF or a genomic sequence depending on who uses the plugin
 *
 * @author Jerric, modified by Cristina 2010 to add DNA motif
 * @created Jan 31, 2006
 */
abstract class AAMotifPlugin extends AbstractMotifPlugin {

  private static final Logger logger = Logger.getLogger(AAMotifPlugin.class);

  //protected static final String DEFAULT_REGEX = ">(?:\\w*\\|)*([^|\\s]+)\\s*\\|.*?\\s*organism=([^|\\s]+)";
  protected static final String DEFAULT_REGEX = ">.*transcript=([^|\\s]+).*organism=([^|\\s]+)";

  /**
   * This constructor is provided to support children extension for motif search
   * of other protein related types, such as ORF, etc.
   *
   * @param regexField
   * @param defaultRegex
   */
  protected AAMotifPlugin(String regexField, String defaultRegex) {
    super(regexField, defaultRegex);
  }

  @Override
  public String[] getColumns(PluginRequest request) {
    return new String[] { COLUMN_SOURCE_ID, COLUMN_PROJECT_ID,
                  COLUMN_LOCATIONS, COLUMN_MATCH_COUNT, COLUMN_SEQUENCE, COLUMN_MATCH_SEQUENCES };
  }

  @Override
  protected Map<Character, String> getSymbols() {
    Map<Character, String> symbols = new HashMap<Character, String>();
    symbols.put('0', "DE");
    symbols.put('1', "ST");
    symbols.put('2', "ILV");
    symbols.put('3', "FHWY");
    symbols.put('4', "KRH");
    symbols.put('5', "DEHKR");
    symbols.put('6', "AVILMFYW");
    symbols.put('7', "KRHDENQ");
    symbols.put('8', "CDEHKNQRST");
    symbols.put('9', "ACDGNPSTV");
    symbols.put('B', "AGS");
    symbols.put('Z', "ACDEGHKNQRST");
    symbols.put('X', "ACDEFGHIKLMNPQRSTVWY");

    return symbols;
  }

  @Override
  protected void addMatch(PluginResponse response, Match match,
      Map<String, Integer> orders) throws PluginModelException, PluginUserException  {
    String[] result = new String[orders.size()];
    result[orders.get(COLUMN_PROJECT_ID)] = match.projectId;
    result[orders.get(COLUMN_SOURCE_ID)] = match.sourceId;
    result[orders.get(COLUMN_LOCATIONS)] = match.locations;
    result[orders.get(COLUMN_MATCH_COUNT)] = Integer.toString(match.matchCount);
    result[orders.get(COLUMN_SEQUENCE)] = match.sequence;
    result[orders.get(COLUMN_MATCH_SEQUENCES)] = String.join(", ", match.matchSequences);
    response.addRow(result);
  }

  @Override
  protected void findMatches(PluginResponse response,
      Map<String, Integer> orders, String headline, Pattern searchPattern,
      String sequence) throws PluginModelException, PluginUserException {
    MotifConfig config = getConfig();

    // parse the headline
    Matcher deflineMatcher = config.getDeflinePattern().matcher(headline);
    if (!deflineMatcher.find()) {
      logger.warn("Invalid defline: " + headline + " Against Pattern "
          + config.getDeflinePattern().pattern());
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
    try {
      match.projectId = getProjectId(organism);
    } catch (WdkModelException ex) {
      throw new PluginModelException(ex);
    }

    StringBuffer sbLoc = new StringBuffer();
    StringBuffer sbSeq = new StringBuffer();
    int prev = 0;
    int contextLength = config.getContextLength();

    Matcher matcher = searchPattern.matcher(sequence);
    boolean longLoc = false, longSeq = false;
    while (matcher.find()) {
      // add locations only while we have room.
      if (!longLoc) {
        String location = getLocation(0, matcher.start(), matcher.end() - 1,
            false);
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

        seq.append("<span class=\"" + MOTIF_STYLE_CLASS + "\">");
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
    addMatch(response, match, orders);
  }
}
