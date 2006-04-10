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
    public static final String COLUMN_ID = "Id";
    public static final String COLUMN_HEADER = "Header";
    public static final String COLUMN_FOOTER = "Footer";
    public static final String COLUMN_ROW = "TabularRow";
    public static final String COLUMN_BLOCK = "Alignment";
    public static final String COLUMN_PROJECT = "Project";

    // required parameter definitions
    public static final String PARAM_SEQUENCE = "BlastQuerySequence";
    public static final String PARAM_QUERY_TYPE = "BlastQueryType";
    public static final String PARAM_ORGANISM = "BlastDatabaseOrganism";
    public static final String PARAM_DATATYPE = "BlastDatabaseTypeGene";

    // field definitions in the config file
    private static final String FIELD_APP_PATH = "AppPath";
    private static final String FIELD_DATA_PATH = "DataPath";
    private static final String FIELD_TIMEOUT = "Timeout";

    private static final String TEMP_FILE_PREFIX = "wuBlastPlugin";

    private static String appPath;
    private static String dataPath;
    private static long timeout;
    private static String dtype;
    private static boolean useProjectId;
    private static String projectId;
    private static String application;

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
        //Remove when we have new parameters passing through;
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
        return new String[] { PARAM_QUERY_TYPE, PARAM_SEQUENCE, PARAM_ORGANISM, PARAM_DATATYPE };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getColumns()
     */
    @Override
    protected String[] getColumns() {
           return new String[] { COLUMN_PROJECT, COLUMN_ID, COLUMN_HEADER, COLUMN_FOOTER,
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
        dtype = params.get(PARAM_DATATYPE);
        String org = params.get(PARAM_ORGANISM);
        projectId = getProjectId(org);


        String seqType = "p";
        if (dtype.equals("genomic")) {
               seqType = "n";
        }       
        if (dtype.equals("CDS")) {
               //seqType = "t";
               seqType = "n";
        }       

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

        //Parse-out any blast options
        StringBuffer bv = new StringBuffer();

        String blastVariables = bv.toString();
       
        // now prepare the commandline
        StringBuffer sb = new StringBuffer();
                                                                                                    

        String qType = params.get(PARAM_QUERY_TYPE);
        String blastApp = getBlastProgram(qType, dtype);
        sb.append(appPath + blastApp);
        sb.append(" " + dataPath + seqType + "/" + params.get(PARAM_ORGANISM) + params.get(PARAM_DATATYPE) + "/" + params.get(PARAM_ORGANISM) + params.get(PARAM_DATATYPE));
        sb.append(" " + seqFile.getAbsolutePath());

        for (String param : params.keySet()) {
            if (!param.equals(PARAM_QUERY_TYPE)
                    && !param.equals(PARAM_ORGANISM)
                    && !param.equals(PARAM_DATATYPE)
                    && !param.equals(PARAM_SEQUENCE)) {
                sb.append(" " + param + "=" + params.get(param));
            }
        }
        sb.append(" O=" + outFile.getAbsolutePath() + blastVariables);
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
        line = in.readLine(); // skip an empty line
        Map<String, String> rows = new HashMap<String, String>();
        while ((line = in.readLine()) != null) {
          // check if no hit in the result
            if (line.indexOf("NONE") >= 0) {
                // Commented by Jerric
                // return an empty array if there's no hit found
                
//                // no hits found, next are footer
//                StringBuffer footer = new StringBuffer();
//                while ((line = in.readLine()) != null) {
//                    footer.append(line + newline);
//                }
//                String[][] result = new String[1][columns.size()];
//                result[0][columns.get(COLUMN_PROJECT)] = projectId;
//                result[0][columns.get(COLUMN_ID)] = "";
//                result[0][columns.get(COLUMN_ROW)] = "";
//                result[0][columns.get(COLUMN_BLOCK)] = "";
//                result[0][columns.get(COLUMN_HEADER)] = header.toString();
//                result[0][columns.get(COLUMN_FOOTER)] = footer.toString();
//                return result;
                return new String[0][columns.size()];
            }
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
                //alignment[columns.get(COLUMN_ID)] = extractID(line);
                String rawId = extractID(line);
                alignment[columns.get(COLUMN_ID)] = getSourceUrl(projectId, rawId);


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
        results[0][columns.get(COLUMN_PROJECT)] = projectId;
        results[0][columns.get(COLUMN_HEADER)] = header.toString();
        results[size - 1][columns.get(COLUMN_FOOTER)] = footer.toString();
        return results;
    }

    private String getBlastProgram(String qType, String dbType) {
              // throws WsfServiceException{
        String bp = null;
        if ("dna".equalsIgnoreCase(qType)) {
            if ("CDS".equals(dbType) || "genomic".equals(dbType)
                    || "dna".equals(dbType)) {
                bp = "blastn";
            } else if (dbType.toLowerCase().indexOf("translated") >= 0) {
                bp = "tblastx";
            } else if ("proteins".equals(dbType)) {
                bp = "blastx";
            }
        } else if ("protein".equalsIgnoreCase(qType)) {
            if ("CDS".equals(dbType) || "genomic".equals(dbType)
                    || dbType.toLowerCase().indexOf("translated") >= 0) {
                bp = "tblastn";
            } else if ("proteins".equals(dbType)) {
                bp = "blastp";
            }
        }
                                                                                                                             
        //if (bp == null) {
         //  throw new WsfServiceException("invalid blast query or database types ("
           //     + qType + ", " + dbType + ")");
        //}

       return bp;
    }


    private String getProjectId(String org) {

        String projectId = "apiDb";

        if (org.startsWith("C")) {
           projectId = "cryptodb";
        }
        else if (org.startsWith("P")) {
           projectId = "plasmodb";
        }
        else if (org.startsWith("T")) {
           projectId = "toxodb";
        }
        return projectId;
    }

    private String extractID(String row) {
        String[] pieces = tokenize(row);

        String srcid = pieces[2];
        if (dtype == "genomic") {
           srcid = pieces[1];
        }
        return srcid;
    }

    private String getSourceUrl(String projectId, String sourceId) {
        String sourceUrl = sourceId + " - (no link)";   

     if ("cryptodb".equals(projectId)) {
         sourceUrl = ("<a href=http://dev1.cryptodb.org/cryptodb/showRecord.do?name=Class.recordset.Clas.Something&projectId=" + projectId + "&primary_key=" + sourceId + ">" + sourceId + "</a>");
       }
       else if ("plasmodb".equals(projectId)) {
           sourceUrl = ("<a href=http://v5-0.plasmodb.org/plasmo-release5-0/showRecord.do?name=Class.recordset.Clas.Something&projectId=" + projectId + "&primary_key=" + sourceId + ">" + sourceId + "</a>");
       }
       else if ("toxodb".equals(projectId)) {
           sourceUrl = ("<a href=http://v4-0.toxodb.org/toxo-release4-0/showRecord.do?name=Class.recordset.Clas.Something&projectId=" + projectId + "&primary_key=" + sourceId + ">" + sourceId + "</a>");
       }

    return sourceUrl;
      
    }

}
