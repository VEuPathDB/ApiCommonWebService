package org.apidb.apicomplexa.wsfplugin.ncbiblast;

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
public class NcbiBlastPlugin extends BlastPlugin {

    private static final String PROPERTY_FILE = "ncbiBlast-config.xml";

    /**
     * @throws WsfServiceException
     * @throws IOException
     * @throws InvalidPropertiesFormatException
     * 
     */
    public NcbiBlastPlugin() throws WsfServiceException {
        super(PROPERTY_FILE);
    }

    protected String[] prepareParameters(Map<String, String> params,
            File seqFile, File outFile, String dbType) throws IOException,
            WsfServiceException {
        // now prepare the commandline
        Vector<String> cmds = new Vector<String>();
        cmds.add(appPath + "/blastall");

        String qType = params.get(PARAM_QUERY_TYPE);
        params.remove(PARAM_QUERY_TYPE);
        String dbOrgs = params.get(PARAM_DATABASE_ORGANISM);
        params.remove(PARAM_DATABASE_ORGANISM);

        String blastApp = getBlastProgram(qType, dbType);
        String blastDbs = getBlastDatabase(dbType, dbOrgs);
        cmds.add("-p");
        cmds.add(blastApp);
        cmds.add("-d");
        cmds.add(blastDbs);
        cmds.add("-i");
        cmds.add(seqFile.getAbsolutePath());
        cmds.add("-o");
        cmds.add(outFile.getAbsolutePath());

        for (String param : params.keySet()) {

	    if(param.equals("-filter")) {
		cmds.add("-F");
		if (params.get(param).equals("yes"))
		    cmds.add("T");
		else
		    cmds.add("F");
	    }

            if (!param.equals("-p") && !param.equals("-d")
                    && !param.equals("-i") && !param.equals("-o") && !param.equals("-filter")) {
		cmds.add(param);
                cmds.add(params.get(param));
            }
        }
        logger.debug(blastDbs + " inferred from (" + dbType + ", '" + dbOrgs
                + "')");
        logger.debug(blastApp + " inferred from (" + qType + ", " + dbType
                + ")");

        String[] cmdArray = new String[cmds.size()];
        cmds.toArray(cmdArray);
        return cmdArray;
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

            // check if no hit in the result
            if (line.indexOf("No hits found") >= 0) {
                // no hits found, then read everything into message
                 while ((line = in.readLine()) != null) {
                     header.append(line + newline);
                 }
                this.message = header.toString();
                return new String[0][columns.size()];
            }
        } while (!line.startsWith("Sequence"));

        // read tabular part, which starts after the second empty line
        line = in.readLine(); // skip an empty line
        // map of <source_id, [organism,tabular_row]>
        Map<String, String[]> rows = new HashMap<String, String[]>();
        while ((line = in.readLine()) != null) {
            if (line.trim().length() == 0) break;
            // get source id
            int[] sourceIdPos = findField(line, sourceIdRegex);
            String sourceId = line.substring(sourceIdPos[0], sourceIdPos[1]);

            // get organism
            int[] organismPos = findField(line, organismRegex);
            String organism = line.substring(organismPos[0], organismPos[1]);
            logger.debug("Organism extracted from defline is: " + organism);
            // insert the url linking to
            line = insertIdUrl(line, dbType);
            rows.put(sourceId, new String[] { organism, line });
        }

        // extract alignment blocks
        List<String[]> blocks = new ArrayList<String[]>();
        StringBuffer block = null;
        String[] alignment = null;
        while ((line = in.readLine()) != null) {
            // reach the footer part
            if (line.trim().startsWith("Database")) {
                // output the last block, if have
                if (alignment != null) {
                    alignment[columns.get(COLUMN_BLOCK)] = block.toString();
                    blocks.add(alignment);
                }
                break;
            }

            // reach a new start of alignment block
            if (line.length() > 0 && line.charAt(0) == '>') {
                // output the previous block, if have
                if (alignment != null) {
                    alignment[columns.get(COLUMN_BLOCK)] = block.toString();
                    blocks.add(alignment);
                }
                // create a new alignment and block
                alignment = new String[orderedColumns.length];
                block = new StringBuffer();

		// to handle two-line definitions in ORFs
		String secondLine = in.readLine();
		block.append(line + newline);
		block.append(secondLine + newline);

		line = line.trim() + secondLine.trim();

                // extract source id
                int[] sourceIdPos = findField(line, sourceIdRegex);
                String sourceId = line
                        .substring(sourceIdPos[0], sourceIdPos[1]);

                // insert the organism url
                line = insertIdUrl(line, dbType);
                alignment[columns.get(COLUMN_ID)] = sourceId;
            } else {
		// add this line to the block
		block.append(line + newline);
	    }
        }

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
            // copy block
            int blockIndex = columns.get(COLUMN_BLOCK);
            results[i][blockIndex] = alignment[blockIndex];
            // copy tabular row
            int rowIndex = columns.get(COLUMN_ROW);
            for (String id : rows.keySet()) {
                if (alignment[idIndex].startsWith(id)) {
                    String[] parts = rows.get(id);
                    String organism = parts[0];
                    String tabularRow = parts[1];
                    results[i][rowIndex] = tabularRow;
                    if (useProjectId) {
                        int projectIdIndex = columns.get(COLUMN_PROJECT_ID);
                        String projectId = getProjectId(organism);
                        results[i][projectIdIndex] = projectId;
                    }
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
