package org.apidb.apicomplexa.wsfplugin.ncbiblast;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * 
 */

/**
 * @author Jerric
 * @created Nov 2, 2005
 */
public class NcbiBlastPlugin extends WsfPlugin {

    private static final String PROPERTY_FILE = "ncbiBlast-config.xml";

    // column definitions
    public static final String COLUMN_ID = "Identifier";
    public static final String COLUMN_HEADER = "Header";
    public static final String COLUMN_FOOTER = "Footer";
    public static final String COLUMN_ROW = "TabularRow";
    public static final String COLUMN_BLOCK = "Alignment";
    public static final String COLUMN_PROJECT_ID = "ProjectId";

    // required parameter definitions
    public static final String PARAM_QUERY_TYPE = "BlastQueryType";
    public static final String PARAM_DATABASE_TYPE = "BlastDatabaseType";
    public static final String PARAM_DATABASE_ORGANISM = "BlastDatabaseOrganism";
    public static final String PARAM_SEQUENCE = "BlastQuerySequence";

    // field definitions in the config file
    private static final String FIELD_APP_PATH = "AppPath";
    private static final String FIELD_DATA_PATH = "DataPath";
    private static final String FIELD_TIMEOUT = "Timeout";
    private static final String FIELD_USE_PROJECT_ID = "UseProjectId";

    private static final String FIELD_SOURCE_ID_REGEX = "SourceIdRegex_";
    private static final String FIELD_ORGANISM_REGEX = "OrganismRegex_";

    private static final String URL_MAP_PREFIX = "UrlMap_";
    private static final String FIELD_URL_MAP_OTHER = URL_MAP_PREFIX + "Others_";
    private static final String PROJECT_MAP_PREFIX = "ProjectMap_";
    private static final String FIELD_PROJECT_MAP_OTHER = PROJECT_MAP_PREFIX
            + "Others_";

    private static final String TEMP_FILE_PREFIX = "ncbiBlastPlugin";

    private static Set<String> validBlastDBs = new LinkedHashSet<String>();
    static {
        validBlastDBs.add("Pfalciparum_CDS");
        validBlastDBs.add("Pfalciparum_proteins");
        validBlastDBs.add("Pfalciparum_genomic");
        validBlastDBs.add("Pvivax_CDS");
        validBlastDBs.add("Pvivax_proteins");
        validBlastDBs.add("Pvivax_genomic");
        validBlastDBs.add("Pyoelii_CDS");
        validBlastDBs.add("Pyoelii_proteins");
        validBlastDBs.add("Pyoelii_genomic");
        validBlastDBs.add("Plasmodium_CDS");
        validBlastDBs.add("Plasmodium_proteins");
        validBlastDBs.add("Plasmodium_genomic");
        validBlastDBs.add("test_dna");
    }

    private String appPath;
    private String dataPath;
    private long timeout;
    private boolean useProjectId;
    private String sourceIdRegex;
    private String organismRegex;

    private String urlMapOthers;
    private String projectMapOthers;

    /**
     * @throws WsfServiceException
     * @throws IOException
     * @throws InvalidPropertiesFormatException
     * 
     */
    public NcbiBlastPlugin() throws WsfServiceException {
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

        String useProject = getProperty(FIELD_USE_PROJECT_ID);
        useProjectId = (useProject != null && useProject.equalsIgnoreCase("yes"));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getRequiredParameters()
     */
    @Override
    protected String[] getRequiredParameterNames() {
        return new String[]{ PARAM_QUERY_TYPE, PARAM_DATABASE_ORGANISM,
                PARAM_SEQUENCE };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getColumns()
     */
    @Override
    protected String[] getColumns() {
        if (useProjectId) return new String[]{ COLUMN_PROJECT_ID, COLUMN_ID,
                COLUMN_HEADER, COLUMN_FOOTER, COLUMN_ROW, COLUMN_BLOCK };
        else return new String[]{ COLUMN_ID, COLUMN_HEADER, COLUMN_FOOTER,
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
        boolean dbTypePresent = false;
        for (String param : params.keySet()) {
            logger.debug("Param - name=" + param + ", value="
                    + params.get(param));
            if (param.startsWith(PARAM_DATABASE_TYPE)) {
                dbTypePresent = true;
                break;
            }
        }
        if (!dbTypePresent)
            throw new WsfServiceException(
                    "The required database type parameter is not presented.");
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
     */
    @Override
    protected String[][] execute(Map<String, String> params,
            String[] orderedColumns) throws WsfServiceException {
        logger.info("Invoking NcbiBlastPlugin...");

        try {
            // create temporary files for input sequence and output report
            File seqFile = File.createTempFile(TEMP_FILE_PREFIX, "in");
            File outFile = File.createTempFile(TEMP_FILE_PREFIX, "out");

            // get database type parameter
            String dbType = null;
            String dbTypeName = null;
            for (String param : params.keySet()) {
                if (param.startsWith(PARAM_DATABASE_TYPE)) {
                    dbTypeName = param;
                    dbType = params.get(param);
                    break;
                }
            }
            params.remove(dbTypeName);
            
            // get the proper regular expression

            sourceIdRegex = getProperty(FIELD_SOURCE_ID_REGEX+dbType);
            organismRegex = getProperty(FIELD_ORGANISM_REGEX+dbType);

            urlMapOthers = getProperty(FIELD_URL_MAP_OTHER+dbType);
            projectMapOthers = getProperty(FIELD_PROJECT_MAP_OTHER + dbType);
            

            // prepare the arguments
            String command = prepareParameters(params, seqFile, outFile, dbType);
            logger.debug("Command prepared: " + command);

            // invoke the command
            String output = invokeCommand(command, timeout);
            if (exitValue != 0)
                throw new WsfServiceException("The invocation is failed: "
                        + output);

            // if the invocation succeeds, prepare the result; otherwise,
            // prepare results for failure scenario
            logger.debug("Preparing the result");
            String[][] result = prepareResult(orderedColumns, outFile, dbType);
            logger.debug(printArray(result));
            return result;
        } catch (IOException ex) {
            logger.error(ex);
            throw new WsfServiceException(ex);
        }
    }

    private String prepareParameters(Map<String, String> params, File seqFile,
            File outFile, String dbType) throws IOException,
            WsfServiceException {
        // get sequence
        String seq = params.get(PARAM_SEQUENCE);
        params.remove(PARAM_SEQUENCE);

        // write the sequence into the temporary fasta file, with sequence
        // wrapped for every 60 characters
        PrintWriter out = new PrintWriter(new FileWriter(seqFile));
        if (!seq.startsWith(">")) out.println(">MySeq1");
        int pos = 0;
        while (pos < seq.length()) {
            int end = Math.min(pos + 60, seq.length());
            out.println(seq.substring(pos, end));
            pos = end;
        }
        out.flush();
        out.close();

        // now prepare the commandline
        StringBuffer sb = new StringBuffer(appPath + "/blastall");

        String qType = params.get(PARAM_QUERY_TYPE);
        params.remove(PARAM_QUERY_TYPE);
        String dbOrg = params.get(PARAM_DATABASE_ORGANISM);
        params.remove(PARAM_DATABASE_ORGANISM);

        String blastApp = getBlastProgram(qType, dbType);
        String blastDbFile = dataPath + "/" + getBlastDatabase(dbType, dbOrg);
        sb.append(" -p " + blastApp);
        sb.append(" -d " + blastDbFile);
        sb.append(" -i " + seqFile.getAbsolutePath());
        sb.append(" -o " + outFile.getAbsolutePath());

        for (String param : params.keySet()) {
            if (!param.equals("-p") && !param.equals("-d")
                    && !param.equals("-i") && !param.equals("-o"))
                sb.append(" " + param + " " + params.get(param));
        }
        logger.debug(blastDbFile + " inferred from (" + dbType + ", " + dbOrg
                + ")");
        logger.debug(blastApp + " inferred from (" + qType + ", " + dbType
                + ")");
        return sb.toString();
    }

    private String getBlastProgram(String qType, String dbType)
            throws WsfServiceException {
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

        if (bp != null) {
            return bp;
        }

        throw new WsfServiceException("invalid blast query or database types ("
                + qType + ", " + dbType + ")");
    }

    private String getBlastDatabase(String dbType, String dbOrg)
            throws WsfServiceException {
        int x = dbType.toLowerCase().indexOf("translated");
        if (x >= 0) {
            dbType = dbType.substring(0, x) + dbType.substring(x + 10);
            dbType = dbType.replaceAll(" ", "");
        }

        if (dbOrg.toLowerCase().matches("^p\\.?\\s?f.*$")) {
            dbOrg = "Pfalciparum";
        } else if (dbOrg.toLowerCase().matches("^p\\.?\\s?v.*$")) {
            dbOrg = "Pvivax";
        } else if (dbOrg.toLowerCase().matches("^p\\.?\\s?y.*$")) {
            dbOrg = "Pyoelii";
        } else if ("any".equals(dbOrg)) {
            dbOrg = "Plasmodium";
        }
        String blastDb = dbOrg + "_" + dbType;

        logger.debug("blastDb=" + blastDb);

        if (!validBlastDBs.contains(blastDb)) {
            throw new WsfServiceException(
                    "invalid blast database type or organism (" + dbType + ", "
                            + dbOrg + ")");
        }
        return blastDb;
    }

    private String[][] prepareResult(String[] orderedColumns, File outFile,
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
                // no hits found, next are footer
                StringBuffer footer = new StringBuffer();
                while ((line = in.readLine()) != null) {
                    footer.append(line + newline);
                }
                String[][] result = new String[1][columns.size()];
                result[0][columns.get(COLUMN_ID)] = "";
                result[0][columns.get(COLUMN_ROW)] = "";
                result[0][columns.get(COLUMN_BLOCK)] = "";
                result[0][columns.get(COLUMN_HEADER)] = header.toString();
                result[0][columns.get(COLUMN_FOOTER)] = footer.toString();

                if (useProjectId)
                    result[0][columns.get(COLUMN_PROJECT_ID)] = "";
                return result;
            }
        } while (!line.startsWith("Sequence"));

        // read tabular part, which starts after the second empty line
        line = in.readLine(); // skip an empty line
        // map of <source_id, [organism,tabular_row]>
        Map<String, String[]> rows = new HashMap<String, String[]>();
        while ((line = in.readLine()) != null) {
            if (line.trim().length() == 0) break;
            // extract source id
            String sourceId = extractField(line, sourceIdRegex);
            String organism = extractField(line, organismRegex);
            logger.info("Organism extracted from defline is: " + organism);
            // insert the organism url
            line = insertUrl(line, dbType);
            rows.put(sourceId, new String[]{ organism, line });
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

                // extract source id
                String sourceId = extractField(line, sourceIdRegex);
                // insert the organism url
                line = insertUrl(line, dbType);
                alignment[columns.get(COLUMN_ID)] = sourceId;
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
            // copy project id
            // HACK
            // results[i][projectIdIndex] = projectId;
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

    private String extractField(String defline, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(defline);
        if (matcher.find()) {
            // the match is located at group 1
            return matcher.group(1);
        } else return null;
    }

    private String insertUrl(String defline, String dbType) {
        // extract organism from the defline
        String sourceId = extractField(defline, sourceIdRegex);
        String organism = extractField(defline, organismRegex);

        // get the url mapping for this organsim
        String mapkey = URL_MAP_PREFIX + organism + "_" + dbType;
        String mapurl = getProperty(mapkey);
        logger.info("mapkey=" + mapkey + ", mapurl=" + mapurl);
        if (mapurl == null) mapurl = urlMapOthers; // use default url
        mapurl = mapurl.trim().replaceAll("\\$\\$source_id\\$\\$", sourceId);

        // replace the url into the defline
        Pattern pattern = Pattern.compile(sourceIdRegex);
        Matcher matcher = pattern.matcher(defline);
        if (matcher.find()) {
            // the organism is located at group 1
            int start = matcher.start(1);
            int end = matcher.end(1);

            // insert a link tag into the data
            StringBuffer sb = new StringBuffer(defline.substring(0, start));
            sb.append("<a href=\"");
            sb.append(mapurl);
            sb.append("\">");
            sb.append(sourceId);
            sb.append("</a>");
            sb.append(defline.substring(end));
            return sb.toString();
        } else return defline;
    }

    private String getProjectId(String organism) {
        String mapKey = PROJECT_MAP_PREFIX + organism;
        String projectId = getProperty(mapKey);
        if (projectId == null) projectId = projectMapOthers;
        return projectId;
    }
}
