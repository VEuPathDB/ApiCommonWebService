package org.apidb.apicomplexa.wsfplugin.wublast;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apidb.apicomplexa.wsfplugin.BlastPlugin;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * 
 */

/**
 * @author Jerric
 * @created Nov 2, 2005
 */
public class WuBlastPlugin extends BlastPlugin {

    private static final String PROPERTY_FILE = "wuBlast-config.xml";

    /**
     * @throws WsfServiceException
     * @throws IOException
     * @throws InvalidPropertiesFormatException
     * 
     */
    public WuBlastPlugin() throws WsfServiceException {
        super(PROPERTY_FILE);
    }

    protected String[] prepareParameters(Map<String, String> params,
            File seqFile, File outFile, String dbType) throws IOException,
            WsfServiceException {
        // now prepare the commandline
        Vector<String> cmds = new Vector<String>();

        String qType = params.get(PARAM_QUERY_TYPE);
        params.remove(PARAM_QUERY_TYPE);
        String dbOrgs = params.get(PARAM_DATABASE_ORGANISM);
        params.remove(PARAM_DATABASE_ORGANISM);

        String blastApp = getBlastProgram(qType, dbType);
        cmds.add(appPath + "/" + blastApp);
        String blastDbs = getBlastDatabase(dbType, dbOrgs);
        cmds.add(blastDbs);
        cmds.add(seqFile.getAbsolutePath());
        cmds.add("O=" + outFile.getAbsolutePath());

        for (String param : params.keySet()) {
            cmds.add(param);
            cmds.add(params.get(param));
        }
        logger.debug(blastDbs + " inferred from (" + dbType + ", '" + dbOrgs
                + "')");
        logger.debug(blastApp + " inferred from (" + qType + ", " + dbType
                + ")");

        String[] cmdArray = new String[cmds.size()];
        cmds.toArray(cmdArray);
        return cmdArray;
    }

    protected String getBlastDatabase(String dbType, String dbOrgs) {
        // decide the sequence type
        String seqType = "/n/";
        if (dbType.equals("Proteins")) {
            seqType = "/p/";
        }

        // the dborgs is a multipick value, containing several organisms,
        // separated by a comma
        String[] organisms = dbOrgs.split(",");
        StringBuffer sb = new StringBuffer();
        sb.append(dataPath + seqType + organisms[0] + dbType + "/"
                + organisms[0] + dbType);
        for (int i = 1; i < organisms.length; i++) {
            sb.append(" " + dataPath + seqType + organisms[i] + dbType + "/"
                    + organisms[i] + dbType);
        }
        return sb.toString();
    }

    protected String[][] prepareResult(String[] orderedColumns, File outFile,
            String dbType) throws IOException {
        // create a map of <column/position>
        Map<String, Integer> columns = new HashMap<String, Integer>(
                orderedColumns.length);
        for (int i = 0; i < orderedColumns.length; i++) {
            columns.put(orderedColumns[i], i);
        }

        // open output file, and read it
        String line;
        BufferedReader in = new BufferedReader(new FileReader(outFile));
        StringBuffer header = new StringBuffer();
        do {
            line = in.readLine();
            if (line == null)
                throw new IOException("Invalid BLAST output format");
            header.append(line + newline);
        } while (!line.startsWith("Sequence"));

        // read tabular part, which starts after the ONE empty line
        line = in.readLine(); // skip an empty line
        logger.debug("\nWB prepareResult: Line skipped: " + line + "\n");

        Map<String, String> rows = new HashMap<String, String>();
        while ((line = in.readLine()) != null) {
            logger.debug("\nWB prepareResult: Unless no hits, this should be a"
                    + " tabular row line: " + line + "\n");
            if (line.trim().length() == 0) {
                logger.debug("\nWB prepareResult: Line length 0!!, we finished"
                        + " with tabular rows: " + line + "\n");
                break;
            }

            // TODO: before adding the line to rows, insert:
            // - link to gene page, in source_id, and
            // get source id
            int[] sourceIdPos = findField(line, sourceIdRegex);
            String sourceId = line.substring(sourceIdPos[0], sourceIdPos[1]);
            line = insertIdUrl(line, dbType);
            rows.put(sourceId, line);

        }// end while

        // we need to deal with WARNINGs
        line = in.readLine(); // skip an empty line
        logger
                .debug("\nWB prepareResult: This line is supposed to be empty or could have a WARNING or NONE: "
                        + line + "\n");

        if (line.indexOf("NONE") >= 0) return new String[0][columns.size()];

        if (line.trim().startsWith("WARNING")) {
            line = in.readLine(); // skip
            logger
                    .debug("\nWB prepareResult: This line is continuation of warning line: "
                            + line + "\n");
            line = in.readLine(); // skip
            logger
                    .debug("\nWB prepareResult: This line is supposed to be empty: "
                            + line + "\n");
            line = in.readLine(); // skip
            logger
                    .debug("\nWB prepareResult: This line is supposed to be empty: "
                            + line + "\n");
        }

        // extract alignment blocks
        List<String[]> blocks = new ArrayList<String[]>();
        StringBuffer block = null;
        String[] alignment = null;
        while ((line = in.readLine()) != null) {
            // found a warning before parameters
            if (line.trim().startsWith("WARNING")) {
                logger.debug("\nWB prepareResult: Found WARNING, skip: " + line
                        + "\n");
                line = in.readLine(); // skip
                line = in.readLine(); // skip
                line = in.readLine(); // skip
            }

            // reach the footer part
            if (line.trim().startsWith("Parameters")) {
                logger.debug("\nWB prepareResult: Found Parameters: " + line
                        + "\n");
                // output the last block, if have
                if (alignment != null) {
                    alignment[columns.get(COLUMN_BLOCK)] = block.toString();
                    blocks.add(alignment);
                }
                break;
            }

            // reach a new start of alignment block
            if (line.length() > 0 && line.charAt(0) == '>') {
                logger.debug("\nWB prepareResult: This should be a new block: "
                        + line + "\n");

                // output the previous block, if have
                if (alignment != null) {
                    alignment[columns.get(COLUMN_BLOCK)] = block.toString();
                    blocks.add(alignment);
                }
                // create a new alignment and block
                alignment = new String[orderedColumns.length];
                block = new StringBuffer();

                // extract source id
                int[] sourceIdPos = findField(line, sourceIdRegex);
                String sourceId = line
                        .substring(sourceIdPos[0], sourceIdPos[1]);

                // insert the organism url
                line = insertIdUrl(line, dbType);
                alignment[columns.get(COLUMN_ID)] = sourceId;

                // get the project id
                int[] organismPos = findField(line, organismRegex);
                String organism = line
                        .substring(organismPos[0], organismPos[1]);
                logger.info("\n         organism : " + organism + "\n");
                String projectId = getProjectId(organism);
                logger.info("\n         projectId : " + projectId + "\n\n");
                alignment[columns.get(COLUMN_PROJECT_ID)] = projectId;
            }
            // add the line to the block
            block.append(line + newline);

        }// end while

        // get the rest as the footer part
        StringBuffer footer = new StringBuffer();
        footer.append(line + newline);
        while ((line = in.readLine()) != null) {
            footer.append(line + newline);
        }

        // now reconstruct the result
        int size = Math.max(1, blocks.size());
        String[][] results = new String[size][orderedColumns.length];
        for (int i = 0; i < blocks.size(); i++) {
            alignment = blocks.get(i);
            // copy ID
            int idIndex = columns.get(COLUMN_ID);
            results[i][idIndex] = alignment[idIndex];
            // copy PROJECT_ID
            int projectIdIndex = columns.get(COLUMN_PROJECT_ID);
            results[i][projectIdIndex] = alignment[projectIdIndex];
            // copy block
            int blockIndex = columns.get(COLUMN_BLOCK);
            results[i][blockIndex] = alignment[blockIndex];

            // copy tabular row
            int rowIndex = columns.get(COLUMN_ROW);
            for (String id : rows.keySet()) {
                if (alignment[idIndex].startsWith(id)) {
                    results[i][rowIndex] = rows.get(id);
                    break;
                }
            }
        }
        // copy the header and footer
        results[0][columns.get(COLUMN_HEADER)] = header.toString();
        results[size - 1][columns.get(COLUMN_FOOTER)] = footer.toString();
        return results;
    }
}
