/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.WdkUserException;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author Jerric, modified by Cristina 2010 to add DNA motif
 * @created Jan 31, 2006
 */

// geneID could be an ORF or a genomic sequence deending on who uses the plugin
public class ProteinMotifPlugin extends AbstractMotifPlugin {

  // let's store files in same directory
  public static final String FIELD_REGEX = "ProteinDeflineRegex";
  private static final String DEFAULT_REGEX = "^>[^|]+\\|\\s*([^|\\s]+)\\s*\\|.*?\\s*organism=([^|_\\s]+)";

  private static final Logger logger = Logger.getLogger(ProteinMotifPlugin.class);

  /**
   * @throws WsfServiceException
   * 
   */
  public ProteinMotifPlugin() throws WsfServiceException {
    super(FIELD_REGEX, DEFAULT_REGEX);
  }

  /**
   * This constructor is provided to support children extension for motif search
   * of other protein related types, such as ORF, etc.
   * 
   * @param regexField
   * @param defaultRegex
   * @throws WsfServiceException
   */
  public ProteinMotifPlugin(String regexField, String defaultRegex)
      throws WsfServiceException {
    super(regexField, defaultRegex);
  }

  @Override
  public void initialize(Map<String, Object> context)
      throws WsfServiceException {
    super.initialize(context);
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

    return symbols;
  }

  @Override
  protected void findMatches(Set<Match> matches, String headline,
      Pattern searchPattern, String sequence) throws WdkModelException,
      WdkUserException, SQLException {
    MotifConfig config = getConfig();

    // parse the headline
    Matcher deflineMatcher = config.getDeflinePattern().matcher(headline);
    if (!deflineMatcher.find()) {
      logger.warn("Invalid defline: " + headline);
      return;
    }
    // the gene source id has to be in group(1),
    // organsim has to be in group(2),
    String sourceId = deflineMatcher.group(1);
    String organism = deflineMatcher.group(2);
    String projectId = getProjectId(organism);

    Match match = new Match();
    match.sourceId = sourceId;
    match.projectId = projectId;
    StringBuffer sbLoc = new StringBuffer();
    StringBuffer sbSeq = new StringBuffer();
    int prev = 0;
    int contextLength = config.getContextLength();

    Matcher matcher = searchPattern.matcher(sequence);
    while (matcher.find()) {
      String location = getLocation(0, matcher.start(), matcher.end() - 1,
          false);

      // add locations
      if (sbLoc.length() != 0)
        sbLoc.append(", ");
      sbLoc.append('(' + location + ')');

      // obtain the context sequence
      if ((matcher.start() - prev) <= (contextLength * 2)) {
        // no need to trim
        sbSeq.append(sequence.substring(prev, matcher.start()));
      } else { // need to trim some
        if (prev != 0)
          sbSeq.append(sequence.substring(prev, prev + contextLength));
        sbSeq.append("... ");
        sbSeq.append(sequence.substring(matcher.start() - contextLength,
            matcher.start()));
      }
      sbSeq.append("<span style=\"" + MOTIF_STYLE_CLASS + "\">");
      sbSeq.append(sequence.substring(matcher.start(), matcher.end()));
      sbSeq.append("</span>");
      prev = matcher.end();
      match.matchCount++;
    }
    if (match.matchCount == 0)
      return;

    // grab the last context
    if ((prev + contextLength) < sequence.length()) {
      sbSeq.append(sequence.substring(prev, prev + contextLength));
      sbSeq.append("... ");
    } else {
      sbSeq.append(sequence.substring(prev));
    }
    match.locations = sbLoc.toString();
    match.sequence = sbSeq.toString();
    matches.add(match);
  }
}
