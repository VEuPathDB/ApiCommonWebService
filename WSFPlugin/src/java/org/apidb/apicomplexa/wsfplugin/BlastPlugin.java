/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gusdb.wsf.plugin.IWsfPlugin;
import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author xingao
 * 
 */
public abstract class BlastPlugin extends WsfPlugin implements IWsfPlugin {

    // column definitions
    public static final String COLUMN_ID = "Identifier";
    public static final String COLUMN_HEADER = "Header";
    public static final String COLUMN_FOOTER = "Footer";
    public static final String COLUMN_ROW = "TabularRow";
    public static final String COLUMN_BLOCK = "Alignment";
    public static final String COLUMN_PROJECT_ID = "ProjectId";

    // required parameter definitions
    public static final String PARAM_ALGORITHM = "BlastAlgorithm";
    //    public static final String PARAM_QUERY_TYPE = "BlastQueryType";
    public static final String PARAM_DATABASE_TYPE = "BlastDatabaseType";
    public static final String PARAM_DATABASE_ORGANISM = "BlastDatabaseOrganism";
    public static final String PARAM_SEQUENCE = "BlastQuerySequence";

    // field definitions in the config file
    private static final String FIELD_APP_PATH = "AppPath";
    private static final String FIELD_DATA_PATH = "DataPath";
    private static final String FIELD_TEMP_PATH = "TempPath";
    private static final String FIELD_TIMEOUT = "Timeout";
    private static final String FIELD_USE_PROJECT_ID = "UseProjectId";
    private static final String FIELD_FILE_PATH_PATTERN = "FilePathPattern";

    private static final String FIELD_SOURCE_ID_REGEX_PREFIX = "SourceIdRegex_";
    private static final String FIELD_ORGANISM_REGEX_PREFIX = "OrganismRegex_";
    private static final String FIELD_SCORE_REGEX = "ScoreRegex";

    private static final String URL_MAP_PREFIX = "UrlMap_";
    private static final String FIELD_URL_MAP_OTHER = URL_MAP_PREFIX
            + "Others_";
    private static final String PROJECT_MAP_PREFIX = "ProjectMap_";
    private static final String FIELD_PROJECT_MAP_OTHER = PROJECT_MAP_PREFIX
            + "Others_";

    protected String appPath;
    protected String dataPath;
    protected String filePathPattern;

    private String tempPath;
    private long timeout;
    protected boolean useProjectId;
    protected String sourceIdRegex;
    protected String organismRegex;
    private String scoreRegex;

    private String urlMapOthers;

    protected String projectMapOthers;

    /**
     * @param propertyFile
     * @throws WsfServiceException
     */
    public BlastPlugin(String propertyFile) throws WsfServiceException {
        super(propertyFile);

        // load properties
        appPath = getProperty(FIELD_APP_PATH);
        dataPath = getProperty(FIELD_DATA_PATH);
        tempPath = getProperty(FIELD_TEMP_PATH);
        filePathPattern = getProperty(FIELD_FILE_PATH_PATTERN);
        if (appPath == null || dataPath == null || tempPath == null)
            throw new WsfServiceException(
                    "The required fields in property file are missing: "
                            + FIELD_APP_PATH + ", " + FIELD_DATA_PATH + ", "
                            + FIELD_TEMP_PATH);

        String max = getProperty(FIELD_TIMEOUT);
        if (max == null) timeout = 60; // by default, set timeout as 60 seconds
        else timeout = Integer.parseInt(max);

        String useProject = getProperty(FIELD_USE_PROJECT_ID);
        useProjectId = (useProject != null && useProject.equalsIgnoreCase("yes"));

        // get the regex for parsing scores from the tabular row
        scoreRegex = getProperty(FIELD_SCORE_REGEX);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#getRequiredParameterNames()
     */
    @Override
    protected String[] getRequiredParameterNames() {
        return new String[] { PARAM_ALGORITHM, PARAM_DATABASE_ORGANISM,
                PARAM_SEQUENCE };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#getColumns()
     */
    @Override
    protected String[] getColumns() {
        if (useProjectId) return new String[] { COLUMN_PROJECT_ID, COLUMN_ID,
                COLUMN_HEADER, COLUMN_FOOTER, COLUMN_ROW, COLUMN_BLOCK };
        else return new String[] { COLUMN_ID, COLUMN_HEADER, COLUMN_FOOTER,
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
     * @see org.gusdb.wsf.plugin.WsfPlugin#execute(java.util.Map,
     *      java.lang.String[])
     */
    @Override
    protected String[][] execute(String invokeKey, Map<String, String> params,
            String[] orderedColumns) throws WsfServiceException {
        // get plugin name
        String pluginName = getClass().getSimpleName();
        logger.info("Invoking " + pluginName + "...");

        File seqFile = null;
        File outFile = null;
        try {
            // create temporary files for input sequence and output report
            File dir = new File(tempPath);
            seqFile = File.createTempFile(pluginName + "_", ".in", dir);
            outFile = File.createTempFile(pluginName + "_", ".out", dir);

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
            sourceIdRegex = getProperty(FIELD_SOURCE_ID_REGEX_PREFIX + dbType);
            organismRegex = getProperty(FIELD_ORGANISM_REGEX_PREFIX + dbType);

            if (sourceIdRegex == null)
                throw new WsfServiceException("The regular expression for "
                        + FIELD_SOURCE_ID_REGEX_PREFIX + dbType
                        + " is missing.");
            if (organismRegex == null)
                throw new WsfServiceException("The regular expression for "
                        + FIELD_ORGANISM_REGEX_PREFIX + dbType + " is missing.");

            urlMapOthers = getProperty(FIELD_URL_MAP_OTHER + dbType);
            projectMapOthers = getProperty(FIELD_PROJECT_MAP_OTHER + dbType);

            // get sequence
            String seq = params.get(PARAM_SEQUENCE);
            params.remove(PARAM_SEQUENCE);

            // write the sequence into the temporary fasta file,
            // do not reformat the sequence - easy to introduce problem
            PrintWriter out = new PrintWriter(new FileWriter(seqFile));
            if (!seq.startsWith(">")) out.println(">MySeq1");
            out.println(seq);
            out.flush();
            out.close();

            // prepare the arguments
            String[] command = prepareParameters(params, seqFile, outFile,
                    dbType);
            logger.info("Command prepared: " + printArray(command));

            // invoke the command
            String output = invokeCommand(command, timeout);

            if (exitValue != 0)
                throw new WsfServiceException("The invocation is failed: "
                        + output);

            // if the invocation succeeds, prepare the result; otherwise,
            // prepare results for failure scenario
            logger.info("\nPreparing the result");
            String[][] result = prepareResult(orderedColumns, outFile, dbType);
            logger.info("\nResult prepared");

            // insert a bookmark into the tabular row, linking to alignment
            insertBookmark(result, orderedColumns);
            logger.debug(printArray(result));
            return result;
        } catch (IOException ex) {
            logger.error(ex);
            throw new WsfServiceException(ex);
        } finally {
            if (seqFile != null) seqFile.delete();
            if (outFile != null) outFile.delete();
        }
    }

    protected String getBlastProgram(String qType, String dbType)
            throws WsfServiceException {
        String bp = null;
        if ("dna".equalsIgnoreCase(qType)) {
            if ("transcripts".equalsIgnoreCase(dbType)
                    || "genomic".equalsIgnoreCase(dbType)
                    || "DNA".equalsIgnoreCase(dbType)
                    || "ESTs".equalsIgnoreCase(dbType)) {
                bp = "blastn";
            } else if (dbType.toLowerCase().indexOf("translated") >= 0) {
                bp = "tblastx";
            } else if ("proteins".equalsIgnoreCase(dbType)
                    || "orfs".equalsIgnoreCase(dbType)) {
                bp = "blastx";
            }
        } else if ("protein".equalsIgnoreCase(qType)) {
            if ("Transcripts".equalsIgnoreCase(dbType)
                    || "transcripts".equalsIgnoreCase(dbType)
                    || "ESTs".equalsIgnoreCase(dbType)
                    || "Genomic".equalsIgnoreCase(dbType)
                    || dbType.toLowerCase().indexOf("translated") >= 0
                    || "ESTs".equalsIgnoreCase(dbType)) {
                bp = "tblastn";
            } else if ("proteins".equalsIgnoreCase(dbType)
                    || "orfs".equalsIgnoreCase(dbType)) {
                bp = "blastp";
            }
        }

        if (bp != null) return bp;
        else throw new WsfServiceException(
                "invalid blast query or database types (" + qType + ", "
                        + dbType + ")");
    }

    protected String getBlastDatabase(String dbType, String dbOrgs) {
        // the dborgs is a multipick value, containing several organisms,
        // separated by a comma
        String[] organisms = dbOrgs.split(",");
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < organisms.length; i++) {
            // construct file path pattern
            String path = filePathPattern.replaceAll("\\$\\$Organism\\$\\$", Matcher.quoteReplacement(organisms[i]).trim()  );
            path = path.replaceAll("\\$\\$DbType\\$\\$", dbType);
            sb.append(dataPath + "/" + path + " ");
        }
        // sb.append("\"");
        return sb.toString().trim();
    }

    protected int[] findField(String defline, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(defline);
        if (matcher.find()) {
            // the match is located at group 1
            return new int[] { matcher.start(1), matcher.end(1) };
        } else {
            logger.warn("Couldn't find pattern \"" + regex + "\" in defline \""
                    + defline + "\"");
            return null;
        }
    }

    protected String insertIdUrl(String defline, String dbType) {
        // extract organism from the defline
	logger.debug("\ninsertIdUrl() line is: " + defline + "\nand dbType is " + dbType + "\n");

        int[] orgPos = findField(defline, organismRegex);
        String organism = defline.substring(orgPos[0], orgPos[1]);
        int[] srcPos = findField(defline, sourceIdRegex);
        String sourceId = defline.substring(srcPos[0], srcPos[1]);
	logger.debug("\ninsertIdUrl() organism is: " + organism + "\nand sourceId is " + sourceId + "\n");

	String projectId= getProjectId(organism);
	logger.debug("\ninsertIdUrl() project is: " + projectId + "\n");
        // get the url mapping for this organsim
        String mapkey = URL_MAP_PREFIX + organism + "_" + dbType;
        String mapurl = getProperty(mapkey);
        logger.debug("mapkey=" + mapkey + ", mapurl=" + mapurl);

        if (mapurl == null) mapurl = urlMapOthers; // use default url
        mapurl = mapurl.trim().replaceAll("\\$\\$source_id\\$\\$", Matcher.quoteReplacement(sourceId));
        mapurl = mapurl.trim().replaceAll("\\$\\$project_id\\$\\$", Matcher.quoteReplacement(projectId));

        // insert a link tag into the data
        StringBuffer sb = new StringBuffer(defline.substring(0, srcPos[0]));
        sb.append("<a href=\"");
        sb.append(mapurl);
        sb.append("\">");
        sb.append(sourceId);
        sb.append("</a>");
        sb.append(defline.substring(srcPos[1]));
        return sb.toString();
    }

    protected void insertBookmark(String[][] result, String[] orderedColumns) {
        // get the position of source_id, tabular row and alignment block
        int srcPos = 0;
        int rowPos = 0;
        int blockPos = 0;
        for (int i = 0; i < orderedColumns.length; i++) {
            if (orderedColumns[i].equalsIgnoreCase(COLUMN_ID)) srcPos = i;
            if (orderedColumns[i].equalsIgnoreCase(COLUMN_ROW)) rowPos = i;
            if (orderedColumns[i].equalsIgnoreCase(COLUMN_BLOCK)) blockPos = i;
        }
        // iterate on each record, and insert the bookmark
        for (int rowId = 0; rowId < result.length; rowId++) {
            String sourceId = result[rowId][srcPos];
            if (sourceId == null || sourceId.length() == 0) continue;

            // insert the link to the score field
            String tabRow = result[rowId][rowPos].trim();
            int[] scorePos = findField(tabRow, scoreRegex);
            StringBuffer sbRow = new StringBuffer();
            sbRow.append(tabRow.substring(0, scorePos[0]));
            sbRow.append("<a href=\"#" + sourceId + "\">");
            sbRow.append(tabRow.substring(scorePos[0], scorePos[1]));
            sbRow.append("</a>");
            sbRow.append(tabRow.substring(scorePos[1]));
            result[rowId][rowPos] = sbRow.toString();

            // insert the bookmark/anchor to the alignment block
            String block = result[rowId][blockPos];
            StringBuffer sbBlock = new StringBuffer(">");
            sbBlock.append("<a name=\"" + sourceId + "\" ");
            sbBlock.append(" id=\"" + sourceId + "\"></a>");
            sbBlock.append(block.substring(1));
            result[rowId][blockPos] = sbBlock.toString();
        }
    }

    protected String getProjectId(String organism) {
        String mapKey = PROJECT_MAP_PREFIX + organism;
        String projectId = getProperty(mapKey);
        if (projectId == null) projectId = projectMapOthers;
        return projectId;
    }

    protected abstract String[] prepareParameters(Map<String, String> params,
            File seqFile, File outFile, String dbType) throws IOException,
            WsfServiceException;

    protected abstract String[][] prepareResult(String[] orderedColumns,
            File outFile, String dbType) throws IOException;

}
