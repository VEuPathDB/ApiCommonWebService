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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.WdkUserException;

public class WuBlastResultFormatter extends AbstractResultFormatter {

  public static final String newline = System.getProperty("line.separator");

  private static final Logger logger = Logger.getLogger(NcbiBlastResultFormatter.class);

  @Override
  public String[][] formatResult(String[] orderedColumns, File outFile,
      String dbType, String recordClass, StringBuffer message)
      throws IOException, WdkModelException, WdkUserException, SQLException {
    // so database name is built correctly for the Translated cases
    if (dbType.contains("Translated")) {
      if (dbType.contains("Transcripts"))
        dbType = "Transcripts";
      else if (dbType.contains("Genomics"))
        dbType = "Genomics";
      else if (dbType.contains("EST"))
        dbType = "EST";
    }

    // create a map of <column/position>
    Map<String, Integer> columns = new HashMap<String, Integer>(
        orderedColumns.length);
    for (int i = 0; i < orderedColumns.length; i++) {
      columns.put(orderedColumns[i], i);
    }

    // Why is rows a Map instead of a simple String[]?
    Map<String, String> rows = new HashMap<String, String>();
    // blocks will contain the alignments
    List<String[]> blocks = new ArrayList<String[]>();
    StringBuffer header = new StringBuffer();
    StringBuffer footer = new StringBuffer();
    StringBuffer warnings = new StringBuffer();

    // open output file, and read it
    // Header; if there is a WARNING before the line "Sequences", it will be
    // added to the header
    String line;
    BufferedReader in = new BufferedReader(new FileReader(outFile));
    try {
      do {
        line = in.readLine();
        logger.debug("\nWB prepareResult(): HEADER: " + line + "\n");
        if (line == null) {
          message.append(header.toString());
          return new String[0][columns.size()];
        }
        // throw new IOException("Invalid BLAST output format");

        // if this is a WARNING complaining about the numbers in some fasta
        // files, do not append this line and the two following
        if (!(line.contains("invalid") && line.contains("code")))
          header.append(line + newline);
        else {
          line = in.readLine();
          if (line != null)
            line = in.readLine();
          if (line == null) {
            message.append(header.toString());
            return new String[0][columns.size()];
          }
        }

        // logger.debug("\nWB prepareResult(): HEADER: " + line + "\n");
      } while ((!line.startsWith("Sequence")) && (!line.startsWith("FATAL")));

      // show stderr
      if (line.startsWith("FATAL")) {
        // in BlastPlugin we append the output to the message, even when it is
        // not empty.
        // The output, when FATAL, contains the important info, so we do not
        // need to keep adding lines to the message.
        // line = in.readLine();
        // header.append(line + newline);
        message.append(header.toString());
        // logger.debug("\nWB prepareResult(): message is: *******************\n"
        // + message + "\n");
        return new String[0][columns.size()];
      }

      // Tabular Rows; which starts after the ONE empty line
      line = in.readLine(); // skip an empty line

      // wublast truncates deflines in tabular lines, that is why in ORF fasta
      // files the source ids are sometimes truncated -- now it is the
      // organism...
      // we introduce these variables to link score to correct alignment
      // block, without id (with a counter instead)
      Integer counter = 0;
      String counterstring;
      String rowline; // to rewrite the tabular row line with a link

      // Loop on Tabular Rows
      while ((line = in.readLine()) != null) {
        logger.debug("\nWB prepareResult() Unless no hits, this should be a tabular row line: "
            + line + "\n");

        if (line.trim().length() == 0) {
          logger.debug("\nWB prepareResult(): ***********Line length 0!!, END OF tabular rows\n");
          logger.info("\n\n ********** NUMBER OF HITS: " + rows.size() + "\n\n");
          break;
        }

        // insert bookmark: link score to alignment block name=#counter
        // blastplugin does this at the end of execute() --insertBookmark()
        // line = insertLinkToBlock(line,counter);

        // insert link to record page, in source_id, when block is read,
        // so we do not have the problem of truncated deflines
        // line = insertIdUrl(line, dbType);

        counterstring = counter.toString();
        rows.put(counterstring, line);
        counter++;

      }// end while

      // END OF READING TABULAR ROWS -- start reading alignments

      // We need to deal with a possible WARNING between tabular rows and
      // alignments: move it to header
      line = in.readLine(); // skip an empty line
      if (line != null) {

        // logger.info("\nWB prepareResult() This line is supposed to be empty
        // or could have a WARNING or keyword NONE: " + line+"\n");
        header.append(newline + line + newline);

        if (line.indexOf("NONE") >= 0) {
          message.append(header.toString());
          return new String[0][columns.size()];
        }

        if (line.trim().startsWith("WARNING")) {
          line = in.readLine(); // get next line
          // logger.info("\nWB prepareResult() This line is continuation of
          // warning line: " + line+"\n");
          header.append(line + newline);
          line = in.readLine(); // get next line
          // logger.info("\nWB prepareResult() This line is supposed to be
          // empty: " + line+"\n");
          header.append(line + newline);
          line = in.readLine(); // get next line
          // logger.info("\nWB prepareResult() This line is supposed to be
          // empty: " + line+"\n");
        }
      }

      // Extract alignment blocks
      String hit_organism, hit_projectId = "", hit_sourceId = "";

      // alignment will have the following []:
      // COLUMN_ID,COLUMN_PROJECT_ID,COLUMN_ROW,COLUMN_BLOCK,COLUMN_HEADER,COLUMN_FOOTER
      String[] alignment = null;
      // block will be copied into alignment[COLUMN_BLOCK]
      StringBuffer block = null;
      // miniblock is used to concatenate the lines that make up the full
      // defline in each hit
      StringBuffer miniblock = null;

      counter = 0;

      String myLink; // for gbrowse link

      while ((line = in.readLine()) != null) {
        // found a warning before parameters
        if (line.trim().startsWith("WARNING")) {

          logger.debug("\nWB prepareResult() Found WARNING: " + line + "\n");
          warnings.append(line + newline);
          line = in.readLine(); // get next line
          warnings.append(line + newline);
          line = in.readLine(); // get next line
          warnings.append(line + newline);
          line = in.readLine(); // get next line
          warnings.append(line + newline);

          if (line == null)
            break;
        }

        // reach the footer part
        if (line.trim().startsWith("Parameters")) {
          logger.info("\nWB prepareResult(): FOOTER (found line Parameters): "
              + line + "\n");
          // output the last block, if have
          if (alignment != null) {
            alignment[columns.get(WuBlastPlugin.COLUMN_BLOCK)] = block.toString();

            // add gbrowse link here START
            if (dbType.equals("Genomics")) {
              String alignLine = block.toString();
              Integer indx = alignLine.indexOf("Score");
              String subline = alignLine.substring(indx);
              Integer indx1;

              Pattern pattern;
              Matcher matcher;
              // instead of positioning the Gbrowse link before "Strand=....."
              // we set it before "Positives = ....", so it works for tblastn
              // and tblastx
              String[] x = Pattern.compile("Positives =").split(subline);
              for (int i = 1; i < x.length; i++) {
                String hspStart = "";
                String hspEnd = "";

                // discard lines between HSPs
                x[i] = x[i].replaceAll("\\s*Minus Strand HSPs(.*)\\z", "");
                x[i] = x[i].replaceAll("\\s*Plus Strand HSPs(.*)\\z", "");

                // get start of HSP
                pattern = Pattern.compile("Sbjct:\\s*\\d+");
                matcher = pattern.matcher(x[i].trim());
                if (matcher.find()) {
                  hspStart = matcher.group();
                  hspStart = hspStart.split("Sbjct:\\s*")[1];
                }
                // get end of HSP
                x[i] = x[i].replaceAll("\\s*Identities(.*)\\z", "");
                x[i] = x[i].replaceAll("\\s*Score(.*)\\z", "");
                x[i] = x[i].replaceAll("\\s*Minus Strand HSPs:(.*)\\z", "");
                x[i] = x[i].replaceAll("\\s*Plus Strand HSPs:(.*)\\z", "");
                pattern = Pattern.compile("\\d+\\z");
                matcher = pattern.matcher(x[i].trim());
                if (matcher.find()) {
                  hspEnd = matcher.group();
                }
                // add gbrowse link just before the 'Strand = Minus / Plus' in
                // HSP hit
                if (hspStart.length() > 0 || hspEnd.length() > 0) {
                  myLink = insertGbrowseLink(hit_sourceId, hspStart, hspEnd,
                      hit_projectId);
                  indx1 = block.indexOf(x[i]);
                  block.insert(indx1 - 11, myLink); // 11 is the length of
                                                    // "Positives =" pattern
                  alignment[columns.get(WuBlastPlugin.COLUMN_BLOCK)] = block.toString();
                } else {
                  logger.info("prepareResult() hspStart/hspEnd not found in "
                      + x[i] + "\n");
                }
              }
            }
            // add gbrowse link here END

            blocks.add(alignment);
          }
          break;
        }

        // reach a new start of alignment block
        if (line.length() > 0 && line.charAt(0) == '>') {
          logger.debug("\n\n\n-----------------\nWB prepareResult() This should be a new block: "
              + line + "\n");

          // output the previous block, if have
          if (alignment != null) {
            alignment[columns.get(WuBlastPlugin.COLUMN_BLOCK)] = block.toString();

            // add gbrowse link here START
            if (dbType.equals("Genomics")) {
              String alignLine = block.toString();
              Integer indx = alignLine.indexOf("Score");
              String subline = alignLine.substring(indx);
              Integer indx1;

              Pattern pattern;
              Matcher matcher;

              String[] x = Pattern.compile("Positives =").split(subline);
              for (int i = 1; i < x.length; i++) {
                String hspStart = "";
                String hspEnd = "";

                // discard lines between HSPs
                x[i] = x[i].replaceAll("\\s*Minus Strand HSPs(.*)\\z", "");
                x[i] = x[i].replaceAll("\\s*Plus Strand HSPs(.*)\\z", "");

                // get start of HSP
                pattern = Pattern.compile("Sbjct:\\s*\\d+");
                matcher = pattern.matcher(x[i].trim());
                if (matcher.find()) {
                  hspStart = matcher.group();
                  hspStart = hspStart.split("Sbjct:\\s*")[1];
                }
                // get end of HSP
                x[i] = x[i].replaceAll("\\s*Identities(.*)\\z", "");
                x[i] = x[i].replaceAll("\\s*Score(.*)\\z", "");
                x[i] = x[i].replaceAll("\\s*Minus Strand HSPs:(.*)\\z", "");
                x[i] = x[i].replaceAll("\\s*Plus Strand HSPs:(.*)\\z", "");
                pattern = Pattern.compile("\\d+\\z");
                matcher = pattern.matcher(x[i].trim());
                if (matcher.find()) {
                  hspEnd = matcher.group();
                }
                // add gbrowse link just before the 'Strand = Minus / Plus' in
                // HSP hit
                if (hspStart.length() > 0 && hspEnd.length() > 0) {
                  myLink = insertGbrowseLink(hit_sourceId, hspStart, hspEnd,
                      hit_projectId);
                  indx1 = block.indexOf(x[i]);
                  block.insert(indx1 - 11, myLink); // 11 is length of
                                                    // "Positives =" pattern
                  alignment[columns.get(WuBlastPlugin.COLUMN_BLOCK)] = block.toString();
                } else {
                  logger.info("prepareResult() hspStart/hspEnd not found in "
                      + x[i] + "\n");
                }
              }
            }
            // add gbrowse link here END

            blocks.add(alignment);
          }
          // create a new alignment and block
          alignment = new String[orderedColumns.length];
          block = new StringBuffer();

          // for deflines where source id is TOO long so
          // the keyword "organism" in blast report appears in the second line :
          // concatenate lines of the defline so we can find "organism"
          // with the regex provided in config file
          miniblock = new StringBuffer();
          while (!(line.trim().startsWith("Length ="))) {
            // logger.debug("\nWB prepareResult() concatenating defline: " +
            // line + "\n");
            miniblock.append(line.trim() + " ");
            line = in.readLine(); // get next line
            if (line == null)
              break;
          }
          miniblock.append("\n" + line);
          line = miniblock.toString();

          // --------------- Making the link to record page
          // get source id
          int[] sourceIdPos = findSourceId(line);
          hit_sourceId = line.substring(sourceIdPos[0], sourceIdPos[1]);

          // get organism
          int[] organismPos = findOrganism(line);
          hit_organism = line.substring(organismPos[0], organismPos[1]);
          // logger.debug("\nWB prepareResult() Organism extracted from defline is: "
          // + hit_organism);

          hit_projectId = projectMapper.getProjectByOrganism(hit_organism);
          // logger.debug("\nWB prepareResult() projectId : " +
          // hit_projectId+"\n\n");
          alignment[columns.get(WuBlastPlugin.COLUMN_PROJECT_ID)] = hit_projectId;

          // logger.info("\n\n\n\n\nWB prepareResult(): to insert URL in alignments: line is: "
          // + line + "\n");
          // Insert link to gene page, in source_id
          // ncbi plugin does not do this
          line = insertIdUrl(line, hit_organism);

          // --------------
          // insert link in tabular row now that we know the organism
          counterstring = counter.toString();
          rowline = rows.get(counterstring);
          // logger.info("\nWB prepareResult(): alignments: to insert URL in TABROW: "
          // + rowline + "\n");
          if (!(dbType.contains("ORF") && hit_organism.contains("Crypto")))
            rowline = insertIdUrl(rowline, hit_organism);
          if (rowline != null)
            rows.put(counterstring, rowline);
          counter++;
          // --------------

          alignment[columns.get(WuBlastPlugin.COLUMN_ID)] = hit_sourceId;

        }
        // add the line to the block
        if (line != null)
          block.append(line + newline);

      }// end while

      // get the rest as the footer part
      if (warnings.length() != 0)
        footer.append(warnings.toString());
      footer.append(line + newline);
      while ((line = in.readLine()) != null) {
        if (line.contains("Database") || line.contains("Title")) {
          String[] singleDbs = line.split(";");
          for (int i = 0; i < singleDbs.length; i++) {
            footer.append(singleDbs[i] + newline);
          }
        } else
          footer.append(line + newline);
      }
    } finally {
      in.close();
    }

    logger.debug("\n\n\n--------------------------------------------\n\nWB prepareResult(): Information stored in Stringbuffers and tables, now copy info into results[][]\n\n");

    // now reconstruct the result
    int size = Math.max(1, blocks.size());
    // logger.info("\n---------WB prepareResult(): size is of Results is: "
    // + size + "\n");

    String[][] results = new String[size][orderedColumns.length];
    for (int i = 0; i < blocks.size(); i++) {
      int counter = i;
      String counterstring = Integer.toString(counter);
      String[] alignment = blocks.get(i);

      // copy ID
      int idIndex = columns.get(WuBlastPlugin.COLUMN_ID);
      results[i][idIndex] = alignment[idIndex];

      // copy counter
      int counterIndex = columns.get(WuBlastPlugin.COLUMN_COUNTER);
      results[i][counterIndex] = counterstring;

      // copy PROJECT_ID
      int projectIdIndex = columns.get(WuBlastPlugin.COLUMN_PROJECT_ID);
      results[i][projectIdIndex] = alignment[projectIdIndex];
      // logger.info("\n---------WB prepareResult(): copied
      // Identifier\n");

      // copy block
      int blockIndex = columns.get(WuBlastPlugin.COLUMN_BLOCK);
      results[i][blockIndex] = alignment[blockIndex];
      // logger.info("\nWB prepareResult(): copied block\n");

      // copy tabular row
      int rowIndex = columns.get(WuBlastPlugin.COLUMN_ROW);
      for (String id : rows.keySet()) {
        // if (alignment[idIndex].startsWith(id)) {
        if (id.equalsIgnoreCase(counterstring)) {
          results[i][rowIndex] = rows.get(id);
          // logger.info("\nWB prepareResult(): copied tabular
          // row\n");
          break;
        }
      }
    }
    // copy the header and footer
    results[0][columns.get(WuBlastPlugin.COLUMN_HEADER)] = header.toString();
    results[size - 1][columns.get(WuBlastPlugin.COLUMN_FOOTER)] = footer.toString();
    // logger.info("\nWB prepareResult(): copied header and footer\n");

    return results;
  }

}
