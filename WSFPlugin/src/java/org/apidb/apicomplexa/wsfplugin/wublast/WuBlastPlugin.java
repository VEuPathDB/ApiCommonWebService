package org.apidb.apicomplexa.wsfplugin.wublast;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Map;

import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * 
 */

/**
 * @author Jerric
 * @created Nov 2, 2005
 */
public class WuBlastPlugin extends WsfPlugin {

    private static final String PROPERTY_FILE = "wuBlast-config.xml";

    // column definitions
    public static final String COLUMN_ID = "Identifier";
    public static final String COLUMN_HEADER = "Header";
    public static final String COLUMN_FOOTER = "Footer";
    public static final String COLUMN_ROW = "TabularRow";
    public static final String COLUMN_BLOCK = "Alignment";

    // required parameter definitions
    public static final String PARAM_APPLICATION = "Application";
    public static final String PARAM_SEQUENCE = "Sequence";
    public static final String PARAM_DATABASE = "Database";

    // field definitions in the config file
    private static final String FIELD_APP_PATH = "AppPath";
    private static final String FIELD_DATA_PATH = "DataPath";
    private static final String FIELD_TIMEOUT = "Timeout";

    private static final String TEMP_FILE_PREFIX = "wuBlastPlugin";

    private static String appPath;
    private static String dataPath;
    private static long timeout;

    /**
     * @throws WsfServiceException
     * @throws IOException
     * @throws InvalidPropertiesFormatException
     * 
     */
    public WuBlastPlugin() throws WsfServiceException {
        super(PROPERTY_FILE);
        // load properties
        appPath = getProperty(FIELD_APP_PATH);
        dataPath = getProperty(FIELD_DATA_PATH);
        if (appPath == null || dataPath == null)
            throw new WsfServiceException(
                    "The required fields in property file are missing: "
                            + FIELD_APP_PATH + ", " + FIELD_DATA_PATH);
        String max = getProperty(FIELD_TIMEOUT);
        if (max == null) timeout = 60; // by default, set timeout as 60 seconds
        else timeout = Integer.parseInt(max);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getRequiredParameters()
     */
    @Override
    protected String[] getRequiredParameterNames() {
        return new String[]{ PARAM_APPLICATION, PARAM_SEQUENCE, PARAM_DATABASE };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getColumns()
     */
    @Override
    protected String[] getColumns() {
        return new String[]{ COLUMN_ID, COLUMN_HEADER, COLUMN_FOOTER,
                COLUMN_ROW, COLUMN_BLOCK };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#validateParameters(java.util.Map)
     */
    @Override
    protected void validateParameters(Map<String, String> params)
            throws WsfServiceException {
    // do nothing in this plugin
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
     */
    @Override
    protected String[][] execute(Map<String, String> params,
            String[] orderedColumns) throws WsfServiceException {
        logger.info("Invoking WuBlastPlugin...");

        try {
            // create temporary files for input sequence and output report
            File seqFile = File.createTempFile(TEMP_FILE_PREFIX, "in");
            File outFile = File.createTempFile(TEMP_FILE_PREFIX, "out");

            // prepare the arguments
            String command = prepareParameters(params, seqFile, outFile);
            logger.debug("Command prepared: " + command);

            // invoke the command
            String output = invokeCommand(command, timeout);
            if (exitValue != 0)
                throw new WsfServiceException("The invocation is failed: "
                        + output);

            // if the invocation succeeds, prepare the result; otherwise,
            // prepare results for failure scenario
            logger.debug("Preparing the result");
            String[][] result = prepareResult(orderedColumns, outFile);
            return result;
        } catch (IOException ex) {
            logger.error(ex);
            throw new WsfServiceException(ex);
        }
    }

    private String prepareParameters(Map<String, String> params, File seqFile,
            File outFile) throws IOException {
        // get sequence
        String seq = params.get(PARAM_SEQUENCE);

        // output sequence in fasta format, with sequence wrapped for every 60
        // characters
        PrintWriter out = new PrintWriter(new FileWriter(seqFile));
        out.println(">Seq1");
        int pos = 0;
        while (pos < seq.length()) {
            int end = Math.min(pos + 60, seq.length());
            out.println(seq.substring(pos, end));
            pos = end;
        }
        out.flush();
        out.close();

        // now prepare the commandline
        StringBuffer sb = new StringBuffer();
        sb.append(appPath + "/" + params.get(PARAM_APPLICATION));
        sb.append(" " + dataPath + "/" + params.get(PARAM_DATABASE));
        sb.append(" " + seqFile.getAbsolutePath());

        for (String param : params.keySet()) {
            if (!param.equals(PARAM_APPLICATION)
                    && !param.equals(PARAM_DATABASE)
                    && !param.equals(PARAM_SEQUENCE)) {
                sb.append(" -" + param + " " + params.get(param));
            }
        }
        sb.append(" O=" + outFile.getAbsolutePath());
        return sb.toString();
    }

    private String[][] prepareResult(String[] orderedColumns, File outFile)
            throws IOException {
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

        // read tabular part, which starts after the second empty line
        line = in.readLine(); // skip an empty line
        Map<String, String> rows = new HashMap<String, String>();
        while ((line = in.readLine()) != null) {
            if (line.trim().length() == 0) break;
            rows.put(extractID(line), line);
        }

        line = in.readLine(); // skip an empty line
        // extract alignment blocks
        List<String[]> blocks = new ArrayList<String[]>();
        StringBuffer block = null;
        String[] alignment = null;
        while ((line = in.readLine()) != null) {
            // reach the footer part
            if (line.trim().startsWith("Parameters")) {
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

                // obtain the ID of it, which is the rest of this line
                alignment[columns.get(COLUMN_ID)] = line.substring(1).trim();
            }
            // add this line to the block
            block.append(line + newline);
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

    private String extractID(String row) {
        // remove last two pieces
        String[] pieces = tokenize(row);
        /*
         * StringBuffer sb = new StringBuffer(); for (int i = 0; i <
         * pieces.length - 2; i++) { sb.append(pieces[i] + " "); } return
         * sb.toString().trim();
         */
        String ID = pieces[0];
        return ID;
    }
}
