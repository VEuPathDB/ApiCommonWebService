package org.apidb.apicomplexa.wsfplugin.blast;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.WdkUserException;

public class NcbiBlastResultFormatter extends AbstractResultFormatter {

  public static final String newline = System.getProperty("line.separator");

  private static final Logger logger = Logger.getLogger(NcbiBlastResultFormatter.class);

  @Override
  public String[][] formatResult(String[] orderedColumns, File outFile,
      String dbType, String recordClass, StringBuffer message)
      throws IOException, WdkModelException, WdkUserException, SQLException {
    // create a map of <column/position>
    Map<String, Integer> columns = new HashMap<String, Integer>(
        orderedColumns.length);
    for (int i = 0; i < orderedColumns.length; i++) {
      columns.put(orderedColumns[i], i);
    }
    // map of <source_id, [organism,tabular_row]>
    Map<String, String[]> rows = new HashMap<String, String[]>();
    List<String[]> blocks = new ArrayList<String[]>();
    StringBuilder header = new StringBuilder();
    StringBuilder footer = new StringBuilder();

    // open output file, and read it
    String line;
    BufferedReader reader = new BufferedReader(new FileReader(outFile));
    try {
      boolean hasHits = false;
      // read the header section
      while ((line = reader.readLine()) != null) {
        header.append(line).append(newline);
        if (line.startsWith("Sequence")) { // found summary section
          hasHits = true;
          break;
        } else if (line.indexOf("No hits found") >= 0) {
          // no hit in the result
          break;
        }
      }

      // check status
      if (!hasHits) { // can be either no hits, or an error output
        // if not an error output, read the rest of the output
        if (line != null) {
          while ((line = reader.readLine()) != null) {
            header.append(line).append(newline);
          }
        }
        // write the full output into message, and return empty results
        message.append(header.toString());
        return new String[0][columns.size()];
      }

      // read tabular part, which starts after the second empty line
      line = reader.readLine(); // skip an empty line
      while ((line = reader.readLine()) != null) {
        if (line.trim().length() == 0)
          break;
        // get source id
        int[] sourceIdPos = findSourceId(line);
        String sourceId = line.substring(sourceIdPos[0], sourceIdPos[1]);

        // get organism
        int[] organismPos = findOrganism(line);
        String organism = line.substring(organismPos[0], organismPos[1]);
        logger.debug("Organism extracted from defline is: " + organism);
        // insert the url linking to
        line = insertIdUrl(line, recordClass);
        if (line != null)
          rows.put(sourceId, new String[] { organism, line });
      }

      // extract alignment blocks
      StringBuilder block = new StringBuilder();
      String[] alignment = null;
      while ((line = reader.readLine()) != null) {
        // reach the footer part
        if (line.trim().startsWith("Database")) {
          // output the last block, if have
          if (alignment != null) {
            alignment[columns.get(NcbiBlastPlugin.COLUMN_ALIGNMENT)] = block.toString();
            blocks.add(alignment);
          }
          break;
        }

        // reach a new start of alignment block
        if (line.length() > 0 && line.charAt(0) == '>') {
          // output the previous block, if have
          if (alignment != null) {
            alignment[columns.get(NcbiBlastPlugin.COLUMN_ALIGNMENT)] = block.toString();
            blocks.add(alignment);
          }
          // create a new alignment and block
          alignment = new String[orderedColumns.length];
          block = new StringBuilder();

          block.append(line + newline);

          // to handle two-line definitions in ORFs
          String secondLine = reader.readLine();
          if (secondLine != null) {
            block.append(secondLine + newline);
            line = line.trim() + secondLine.trim();
          }

          // extract source id
          int[] sourceIdPos = findSourceId(line);
          String sourceId = line.substring(sourceIdPos[0], sourceIdPos[1]);

          // insert the organism url
          line = insertIdUrl(line, recordClass);
          if (line != null)
            alignment[columns.get(NcbiBlastPlugin.COLUMN_ID)] = sourceId;
        } else {
          // add this line to the block
          block.append(line + newline);
        }
      }

      // get the rest as the footer part
      footer.append(line + newline);
      while ((line = reader.readLine()) != null) {
        footer.append(line + newline);
      }
    } finally {
      reader.close();
    }

    // now reconstruct the result
    int size = Math.max(1, blocks.size());
    String[][] results = new String[size][orderedColumns.length];
    for (int i = 0; i < blocks.size(); i++) {
      String[] alignment = blocks.get(i);
      // copy ID
      int idIndex = columns.get(NcbiBlastPlugin.COLUMN_ID);
      results[i][idIndex] = alignment[idIndex];
      // copy block
      int blockIndex = columns.get(NcbiBlastPlugin.COLUMN_ALIGNMENT);
      results[i][blockIndex] = alignment[blockIndex];
      // copy tabular row
      int rowIndex = columns.get(NcbiBlastPlugin.COLUMN_SUMMARY);
      for (String id : rows.keySet()) {
        if (alignment[idIndex].startsWith(id)) {
          String[] parts = rows.get(id);
          String organism = parts[0];
          String tabularRow = parts[1];
          results[i][rowIndex] = tabularRow;
          int projectIdIndex = columns.get(NcbiBlastPlugin.COLUMN_PROJECT_ID);
          String projectId = projectMapper.getProjectByOrganism(organism);
          results[i][projectIdIndex] = projectId;
          break;
        }
      }
    }
    // copy the header and footer
    results[0][columns.get(NcbiBlastPlugin.COLUMN_HEADER)] = header.toString();
    results[size - 1][columns.get(NcbiBlastPlugin.COLUMN_FOOTER)] = footer.toString();
    return results;
  }

}
