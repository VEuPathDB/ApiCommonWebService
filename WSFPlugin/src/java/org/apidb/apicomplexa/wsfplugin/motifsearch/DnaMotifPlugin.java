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
public class DnaMotifPlugin extends AbstractMotifPlugin {

  // The property file for dna motif search
  public static final String FIELD_REGEX = "DnaDeflineRegex";

  private static final String DEFAULT_REGEX = "^>[^|]+\\|\\s*([^|\\s]+)\\s*\\|\\s*strand=\\(([+\\-])\\)\\s*\\|\\s*organism=([^|_\\s]+)";

  private static final Logger logger = Logger.getLogger(DnaMotifPlugin.class);

  public DnaMotifPlugin() throws WsfServiceException {
    super(FIELD_REGEX, DEFAULT_REGEX);
  }

  @Override
  public void initialize(Map<String, Object> context)
      throws WsfServiceException {
    super.initialize(context);
  }

  @Override
  protected Map<Character, String> getSymbols() {
    Map<Character, String> symbols = new HashMap<Character, String>();
    symbols.put('R', "AG");
    symbols.put('Y', "CT");
    symbols.put('M', "AC");
    symbols.put('K', "GT");
    symbols.put('S', "CG");
    symbols.put('W', "AT");
    symbols.put('B', "CGT");
    symbols.put('D', "AGT");
    symbols.put('H', "ACT");
    symbols.put('V', "ACG");
    symbols.put('N', "ACGT");

    return symbols;
  }

  @Override
  protected void findMatches(Set<Match> matches, String headline,
      Pattern searchPattern, String sequence) throws WdkModelException,
      WdkUserException, SQLException {
    // parse the headline
    MotifConfig config = getConfig();
    Matcher deflineMatcher = config.getDeflinePattern().matcher(headline);
    if (!deflineMatcher.find()) {
      logger.warn("Invalid defline: " + headline);
      return;
    }
    // the sequence id has to be in group(1),
    // strand info has to be in group(2)
    // organsim has to be in group(3),
    String sequenceId = deflineMatcher.group(1).intern();
    String strand = deflineMatcher.group(2).intern();
    String organism = deflineMatcher.group(3).intern();
    String projectId = getProjectId(organism).intern();

    int length = sequence.length();
    strand = strand.equals("-") ? "r" : "f";
    boolean reversed = (strand.equals("r"));
    int contextLength = config.getContextLength();

    Matcher matcher = searchPattern.matcher(sequence);
    while (matcher.find()) {
      int start = matcher.start();
      int stop = matcher.end();
      Match match = new Match();
      match.projectId = projectId;
      match.matchCount = 1;
      match.locations = getLocation(length, start, stop, reversed);
      match.sourceId = sequenceId + ":" + match.locations + ":" + strand;

      // create matching context
      StringBuilder context = new StringBuilder();
      int begin = Math.max(0, start - contextLength);
      if (begin > 0)
        context.append("...");
      if (begin < start)
        context.append(sequence.substring(begin, start));
      context.append("<span class=\"" + MOTIF_STYLE_CLASS + "\">");
      context.append(sequence.substring(start, stop));
      context.append("</span>");
      int end = Math.min(sequence.length(), stop + contextLength);
      if (end > stop)
        context.append(sequence.substring(stop, end));
      if (end < sequence.length())
        context.append("...");

      match.sequence = context.toString();
      matches.add(match);
    }
  }
}
