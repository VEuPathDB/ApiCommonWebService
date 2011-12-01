/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.WsfRequest;
import org.gusdb.wsf.plugin.WsfResponse;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author Jerric, modified by Cristina 2010 to add DNA motif
 * @created Jan 31, 2006
 */

// geneID could be an ORF or a genomic sequence deending on who uses the plugin
public abstract class MotifSearchPlugin extends AbstractPlugin {

    protected class Match {

        public String sourceId;
        public String projectId;
        public String locations;
        public int matchCount = 0;
        public String sequence;

        public String getKey() {
            return sourceId + projectId;
        }

        @Override
        public int hashCode() {
            return getKey().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj != null && obj instanceof Match) {
                Match match = (Match) obj;
                return getKey().equals(match.getKey());
            } else
                return false;
        }
    }

    // required parameter definition
    public static final String PROPERTY_FILE = "motifSearch-config.xml";

    public static final String PARAM_DATASET = "motif_organism";
    public static final String PARAM_EXPRESSION = "motif_expression";

    // column definitions for returnd results
    public static final String COLUMN_SOURCE_ID = "SourceID";
    public static final String COLUMN_PROJECT_ID = "ProjectId";
    public static final String COLUMN_LOCATIONS = "Locations";
    public static final String COLUMN_MATCH_COUNT = "MatchCount";
    public static final String COLUMN_SEQUENCE = "Sequence";

    // field definition
    private static final String FIELD_DATA_DIR = "DataDir";
    private static final String FIELD_USE_PROJECT_ID = "UseProjectId";
    private static final String FIELD_PROJECT_MAP_PREFIX = "ProjectMap_";
    private static final String FIELD_PROJECT_MAP_OTHER = FIELD_PROJECT_MAP_PREFIX
            + "Others";

    private static final String DEFAULT_COLOR_CODE = "red";
    private static final int DEFAULT_CONTEXT_LENGTH = 20;
    private static final int MAX_MATCH = 50000;

    protected Pattern deflinePattern;

    private File dataDir;
    private boolean useProjectId;

    private String projectMapOthers;

    /**
     * @throws WsfServiceException
     * 
     */
    protected MotifSearchPlugin() throws WsfServiceException {
        super(PROPERTY_FILE);
    }

    protected abstract String getDeflineField();

    protected abstract Map<Character, String> getSymbols();

    protected abstract void findMatches(Set<Match> matches, String headline,
            Pattern searchPattern, String sequence, String colorCode,
            int contextLength);

    // load properties

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.AbstractPlugin#initialize(java.util.Map)
     */
    @Override
    public void initialize(Map<String, Object> context)
            throws WsfServiceException {
        super.initialize(context);

        String dir = getProperty(FIELD_DATA_DIR);
        if (dir == null)
            throw new WsfServiceException("The required field in property "
                    + "file is missing: " + FIELD_DATA_DIR);
        dataDir = new File(dir);

        String useProject = getProperty(FIELD_USE_PROJECT_ID);
        useProjectId = (useProject != null && useProject
                .equalsIgnoreCase("yes"));

        projectMapOthers = getProperty(FIELD_PROJECT_MAP_OTHER);

        // get defline
        String deflineKey = getDeflineField();
        String deflineRegex = getProperty(deflineKey);
        if (deflineRegex == null)
            throw new WsfServiceException(deflineKey
                    + " is not defined in the config file.");
        this.deflinePattern = Pattern.compile(deflineRegex);

        logger.info("dataDir: " + dataDir.getAbsolutePath());
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getRequiredParameters()
     */
    public String[] getRequiredParameterNames() {
        return new String[] { PARAM_EXPRESSION, PARAM_DATASET };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getColumns()
     */
    public String[] getColumns() {
        if (useProjectId)
            return new String[] { COLUMN_SOURCE_ID, COLUMN_PROJECT_ID,
                    COLUMN_LOCATIONS, COLUMN_MATCH_COUNT, COLUMN_SEQUENCE };
        else
            return new String[] { COLUMN_SOURCE_ID, COLUMN_LOCATIONS,
                    COLUMN_MATCH_COUNT, COLUMN_SEQUENCE };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#validateParameters(java.util.Map)
     */
    public void validateParameters(WsfRequest request)
            throws WsfServiceException {
        // do nothing in this plugin
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
     */
    public WsfResponse execute(WsfRequest request) throws WsfServiceException {
        logger.info("Invoking MotifSearchPlugin...");

        Map<String, String> params = request.getParams();

        // get required parameters
        String datasetIDs = params.get(PARAM_DATASET);
        String expression = params.get(PARAM_EXPRESSION);

        logger.debug("dataDir: " + dataDir.getName());
        logger.debug("datasetIDs: " + datasetIDs);
        logger.debug("expression: " + expression);

        // get optional parameters
        String colorCode = DEFAULT_COLOR_CODE;
        if (params.containsKey("ColorCode"))
            colorCode = params.get("ColorCode");
        int contextLength = DEFAULT_CONTEXT_LENGTH;
        if (params.containsKey("ContextLength"))
            contextLength = Integer.parseInt(params.get("ContextLength"));
        if (contextLength <= 0)
            contextLength = DEFAULT_CONTEXT_LENGTH;

        // translate the expression
        Pattern searchPattern = translateExpression(expression);

        // open the flatfile database assigned by the user
        try {
            Set<Match> allMatches = new HashSet<Match>();
            String[] dsIds = datasetIDs.split(",");

            // scan on each dataset, and add matched motifs in the result
            for (String dsId : dsIds) {
                logger.debug("execute(): dsId: " + dsId);
		//parent organisms in a treeParam, we only need the leave nodes
		if (dsId.contains("-1")) {
		    logger.debug("organism value: (" + dsId + ") not included, we only care for leave nodes\n");
		    continue;
		}

                Set<Match> matches = findMatches(dsId.trim(), searchPattern,
                        colorCode, contextLength);
                allMatches.addAll(matches);
                matches = null;
                System.gc();
            }

            // locations contains (xxx-yyy), (xxx-yyyy), ...
            // sequence contains sequences from matches, separated by a space
            // (so it wraps in summary page)

            String[][] result = prepareResult(allMatches,
                    request.getOrderedColumns());

            WsfResponse wsfResponse = new WsfResponse();
            wsfResponse.setResult(result);
            return wsfResponse;
        } catch (IOException ex) {
            throw new WsfServiceException(ex);
        }
    }

    /**
     * @param fileID
     * @return
     * @throws IOException
     */
    private File openDataFile(String datasetID) throws IOException {
        logger.info("openDataFile(): dataDir: " + dataDir.getAbsolutePath()
                + ", datasetID: " + datasetID + "\n");

        File dataFile = new File(dataDir, datasetID);
        if (!dataFile.exists())
            throw new IOException("The dataset \"" + dataFile.toString()
                    + "\" cannot be found.");
        else
            return dataFile;
    }

    private Pattern translateExpression(String expression) {
        Map<Character, String> codes = getSymbols();

        boolean inSquareBraces = false, inCurlyBraces = false;
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < expression.length(); i++) {
            char ch = Character.toUpperCase(expression.charAt(i));
            boolean skipChar = false;
            if (ch == '{')
                inCurlyBraces = true;
            else if (ch == '}')
                inCurlyBraces = false;
            else if (ch == '[')
                inSquareBraces = true;
            else if (ch == ']')
                inSquareBraces = false;
            else if (!inCurlyBraces && codes.containsKey(ch)) {
                // the char is not in any curly braces, and is a known code;
                // replace the char with the actual string.
                String replace = codes.get(ch);
                if (!inSquareBraces)
                    replace = "[" + replace + "]";
                builder.append(replace);
                skipChar = true;
            }
            if (!skipChar)
                builder.append(ch);
        }
        logger.debug("translated expression: " + builder);

        int option = Pattern.CASE_INSENSITIVE;
        return Pattern.compile(builder.toString(), option);
    }

    private Set<Match> findMatches(String datasetID, Pattern searchPattern,
            String colorCode, int contextLength) throws IOException,
            WsfServiceException {
        File datasetFile = openDataFile(datasetID);
        BufferedReader in = new BufferedReader(new FileReader(datasetFile));
        Set<Match> matches = new HashSet<Match>();

        // read header of the first sequence
        String headline = null, line;
        StringBuilder sequence = new StringBuilder();
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.length() == 0)
                continue;

            if (line.charAt(0) == '>') {
                // starting of a new sequence, process the previous sequence if
                // have any;
                if (sequence.length() > 0) {
                    findMatches(matches, headline, searchPattern,
                            sequence.toString(), colorCode, contextLength);
                    // stop the process if too many hits are found, to avoid 
                    // out-of-memory error.
                    if (matches.size() > MAX_MATCH)
                        throw new WsfServiceException("The number of matches "
                           + "exceeds the system limit, please refine your "
                           + "search pattern to make it more specific."); 
                    // clear the sequence buffer to be ready for the next one
                    sequence = new StringBuilder();
                }
                headline = line;
            } else {
                sequence.append(line);
            }
        }
        // process the last sequence, if it hasn't been processed
        if (headline!= null && sequence.length() > 0) {
            findMatches(matches, headline, searchPattern,
                    sequence.toString(), colorCode, contextLength);
        }
        return matches;
    }

    private String[][] prepareResult(Set<Match> matches, String[] cols) {
        String[][] result = new String[matches.size()][cols.length];
        // create a column order map
        Map<String, Integer> orders = new HashMap<String, Integer>();
        for (int i = 0; i < cols.length; i++)
            orders.put(cols[i], i);

        int i = 0;
        for (Match match : matches) {
            result[i][orders.get(COLUMN_SOURCE_ID)] = match.sourceId;
            result[i][orders.get(COLUMN_LOCATIONS)] = match.locations;
            result[i][orders.get(COLUMN_MATCH_COUNT)] = Integer
                    .toString(match.matchCount);
            result[i][orders.get(COLUMN_SEQUENCE)] = match.sequence;

            // put project id into result, if required
            if (useProjectId)
                result[i][orders.get(COLUMN_PROJECT_ID)] = match.projectId;
            i++;
        }
        logger.info("hits found: " + result.length + "\n");
        // logger.debug("result " + resultToString(result) + "\n");
        return result;
    }

    protected String getProjectId(String organism) {
        String mapKey = FIELD_PROJECT_MAP_PREFIX + organism;
        String projectId = getProperty(mapKey);
        if (projectId == null)
            projectId = projectMapOthers;
        return projectId;
    }

    @Override
    protected String[] defineContextKeys() {
        return null;
    }

    protected String getLocation(int length, int start, int stop, boolean reversed) {
        if (reversed) {
            int newStart = length - stop;
            stop = length - start;
            start = newStart;
        }
        String location = Integer.toString(start);
        if (start != stop) location += "-" + stop;
        return location;
    }

}
