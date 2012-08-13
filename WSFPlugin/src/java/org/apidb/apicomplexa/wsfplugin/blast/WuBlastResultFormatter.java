package org.apidb.apicomplexa.wsfplugin.blast;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.WdkUserException;
import org.gusdb.wsf.plugin.WsfServiceException;

public class WuBlastResultFormatter extends AbstractResultFormatter {

  private static enum Section {
    Header, Summary, Warning, Alignment, Footer
  }

  public static final String newline = System.getProperty("line.separator");

  private static final Logger logger = Logger.getLogger(NcbiBlastResultFormatter.class);

  @Override
  public String[][] formatResult(String[] orderedColumns, File outFile,
      String dbType, String recordClass, StringBuffer message)
      throws IOException, WdkModelException, WdkUserException, SQLException,
      WsfServiceException {
    BufferedReader in = new BufferedReader(new FileReader(outFile));
    Section section = Section.Header;
    StringBuilder header = new StringBuilder(), footer = new StringBuilder(), warning = new StringBuilder();

    // {sourceId, summaryLine}
    Map<String, String> summaries = new LinkedHashMap<>();
    // {sourceId, projectId}
    Map<String, String> projects = new LinkedHashMap<>();
    // {sourceId, score}
    Map<String, String> scores = new LinkedHashMap<>();
    // {sourceId, alignment}
    Map<String, String> alignments = new LinkedHashMap<>();

    try {
      StringBuilder defline = new StringBuilder();
      StringBuilder alignment = new StringBuilder();
      boolean inDefline = false;
      String line;
      // read in blast output
      while ((line = in.readLine()) != null) {
        if (section == Section.Header) {
          // 2 possible transitions: to summary or footer
          if (line.startsWith("Parameters:")) {
            section = Section.Footer;
            footer.append(line + newline);
          } else {
            header.append(line + newline);
            if (line.startsWith("Sequences producing High-scoring Segment Pairs")) {
              header.append(in.readLine() + newline); // skip the empty line.
              section = Section.Summary;
            }
          }
        } else if (section == Section.Footer) { // no more transitions here
          footer.append(line + newline);
        } else if (section == Section.Summary) {
          // 1 possible transition: to warning,
          if (line.length() == 0 || line.trim().equals("*** NONE ***")) {
            // reach the end of summary (empty line), or no hits, transit to
            // footer, along with current line. The first empty line has been
            // skipped while reading "Sequences..."
            section = Section.Warning;
            footer.append(line + newline);
          } else { // find a summary line
            parseSummaryLine(line, summaries, scores);
          }
        } else if (section == Section.Warning) {
          // 2 possible transition to alignment, or to footer if no hits.
          if (line.startsWith("Parameters:")) { // to footer
            section = Section.Footer;
            footer.append(line + newline);
          } else if (line.startsWith(">")) { // transit to first alignment
            section = Section.Alignment;
            inDefline = true;
            defline.append(line + newline);
          } else {
            warning.append(line + newline);
          }
        } else if (section == Section.Alignment) {
          // 2 possible transitions, from current alignment to next alignment,
          // or to footer;
          if (line.startsWith("Parameters:")) { // to footer
            // process the last alignment
            parseAlignment(defline.toString(), alignment.toString(),
                alignments, projects, recordClass);
            
            section = Section.Footer;
            footer.append(line + newline);
          } else if (line.startsWith(">")) { // to next alignment
            // process the previous alignment
            parseAlignment(defline.toString(), alignment.toString(),
                alignments, projects, recordClass);
            
            inDefline = true;
            defline = new StringBuilder();
            alignment = new StringBuilder();
            defline.append(line + newline);
          } else {
            // within an alignment, we can either be on defline or on body
            if (inDefline) {
              if (line.trim().startsWith("Length =")) { // end of defline
                inDefline = false;
                alignment.append(line + newline);
              } else
                defline.append(line + newline);
            } else {
              alignment.append(line + newline);
            }
          }
        }
      }
    } finally {
      in.close();
    }

    logger.debug("Total " + summaries.size() + " hits found.");

    // post process on summary line
    addLinks(summaries, projects, recordClass);
    // prepare the results
    return format(orderedColumns, header.toString(), footer.toString(),
        warning.toString(), summaries, scores, alignments, projects, message);
  }

  private void parseSummaryLine(String line, Map<String, String> summaries,
      Map<String, String> scores) throws WsfServiceException {
    // parse out source id, need to add a '>' in front of summary line to form a
    // proper defline.
    String defline = ">" + line;
    int[] sourceIdLoc = findSourceId(defline);
    String sourceId = defline.substring(sourceIdLoc[0], sourceIdLoc[1]);
    if (summaries.containsKey(sourceId))
      throw new WsfServiceException("Duplicate source id in blast summary: "
          + sourceId);

    summaries.put(sourceId, line);

    // parse out scores
    int[] scoreLoc = findScore(line);
    String score = line.substring(scoreLoc[0], scoreLoc[1]);
    scores.put(sourceId, score);
  }

  private void parseAlignment(String defline, String alignment,
      Map<String, String> aligments, Map<String, String> projects,
      String recordClass) throws WdkModelException, WdkUserException,
      SQLException, UnsupportedEncodingException, WsfServiceException {
    // flaten the defline to a single line.
    String line = defline.replaceAll("\\s+", " ");

    // get sourceId & organism from flatened defline
    int[] sourceIdLoc = findSourceId(line);
    String sourceId = line.substring(sourceIdLoc[0], sourceIdLoc[1]);
    int[] organismLoc = findOrganism(line);
    String organism = line.substring(organismLoc[0], organismLoc[1]);

    // get project from organism
    String projectId = projectMapper.getProjectByOrganism(organism);
    if (projects.containsKey(sourceId))
      throw new WsfServiceException("Duplicate source id in blast alignment: "
          + sourceId);
    projects.put(sourceId, projectId);

    // add link to defline
    defline = insertIdUrl(defline, recordClass, projectId);

    // construct alignment
    aligments.put(sourceId, defline + alignment);
  }

  private void addLinks(Map<String, String> summaries,
      Map<String, String> projects, String recordClass)
      throws UnsupportedEncodingException {
    String[] sourceIds = summaries.keySet().toArray(new String[0]);
    for (String sourceId : sourceIds) {
      String summary = summaries.get(sourceId);
      String projectId = projects.get(sourceId);
      summary = insertIdUrl(">" + summary, recordClass, projectId);
      summary = summary.substring(1);// remove the leading ">"
      summaries.put(sourceId, summary);
    }
  }

  private String[][] format(String[] orderedColumns, String header,
      String footer, String warning, Map<String, String> summaries,
      Map<String, String> scores, Map<String, String> alignments,
      Map<String, String> projects, StringBuffer message) {
    // check if there is any hit, if not, format message & return empty array.
    if (summaries.size() == 0) {
      message.append(header).append(warning).append(footer);
      return new String[0][orderedColumns.length];
    }

    // create a map of <column/position>
    Map<String, Integer> columns = new HashMap<String, Integer>(
        orderedColumns.length);
    for (int i = 0; i < orderedColumns.length; i++) {
      columns.put(orderedColumns[i], i);
    }

    String[][] results = new String[summaries.size()][orderedColumns.length];
    int i = 0;
    for (String sourceId : summaries.keySet()) {
      // copy ID
      int idIndex = columns.get(WuBlastPlugin.COLUMN_ID);
      results[i][idIndex] = sourceId;

      // copy counter
      int counterIndex = columns.get(WuBlastPlugin.COLUMN_COUNTER);
      results[i][counterIndex] = Integer.toString(i + 1);

      // copy PROJECT_ID
      int projectIdIndex = columns.get(WuBlastPlugin.COLUMN_PROJECT_ID);
      results[i][projectIdIndex] = projects.get(sourceId);

      // copy alignment
      int alignmentIndex = columns.get(WuBlastPlugin.COLUMN_ALIGNMENT);
      results[i][alignmentIndex] = alignments.get(sourceId);
      // logger.info("\nWB prepareResult(): copied block\n");

      // copy summary row
      int summaryIndex = columns.get(WuBlastPlugin.COLUMN_SUMMARY);
      results[i][summaryIndex] = summaries.get(sourceId);
      
      i++;
    }

    // copy the header and footer
    results[0][columns.get(WuBlastPlugin.COLUMN_HEADER)] = header.toString();
    results[summaries.size() - 1][columns.get(WuBlastPlugin.COLUMN_FOOTER)] = footer.toString();
    // logger.info("\nWB prepareResult(): copied header and footer\n");

    // copy warning to the first alignment, if there is any warning
    if (warning.length() > 0) {
      int alignmentIndex = columns.get(WuBlastPlugin.COLUMN_ALIGNMENT);
      results[0][alignmentIndex] = warning + results[0][alignmentIndex];
    }

    return results;
  }
}
