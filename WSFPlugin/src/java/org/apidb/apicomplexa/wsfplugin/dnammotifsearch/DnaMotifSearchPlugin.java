/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.dnamotifsearch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfResult;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author Jerric, modified by Cristina
 * @created Apr 30, 2010
 */
public class DnaMotifSearchPlugin extends WsfPlugin {

    private class Match {

        public String dynSpanID;
        public String projectId;
        public String locations;
        public int matchCount = 0;
        public String sequence;

        @Override
        public int hashCode() {
            return (dynSpanID + projectId).hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj != null && obj instanceof Match) {
                Match match = (Match) obj;
                return (dynSpanID + projectId).equals(match.dynSpanID
                        + match.projectId);
            } else return false;
        }
    }

    private static final String PROPERTY_FILE = "dnaMotifSearch-config.xml";

   // class string definition                                                                                                                             
    public static final String CLASS_ACIDIC = "[de]";
    public static final String CLASS_ALCOHOL = "[st]";
    public static final String CLASS_ALIPHATIC = "[ilv]";
    public static final String CLASS_AROMATIC = "[fhwy]";
    public static final String CLASS_BASIC = "[krh]";
    public static final String CLASS_CHARGED = "[dehkr]";
    public static final String CLASS_HYDROPHOBIC = "[avilmfyw]";
    public static final String CLASS_HYDROPHILIC = "[krhdenq]";
    public static final String CLASS_POLAR = "[cdehknqrst]";
    public static final String CLASS_SMALL = "[acdgnpstv]";
    public static final String CLASS_TINY = "[ags]";
    public static final String CLASS_TURNLIKE = "[acdeghknqrst]";

    // required parameter definition
    public static final String PARAM_DATASET = "motif_organism";
    public static final String PARAM_EXPRESSION = "motif_expression";

    // column definitions for returnd results
    public static final String COLUMN_DYNSPAN_ID = "DynSpanID";
    public static final String COLUMN_PROJECT_ID = "ProjectId";
    public static final String COLUMN_LOCATIONS = "Locations";
    public static final String COLUMN_MATCH_COUNT = "MatchCount";
    public static final String COLUMN_SEQUENCE = "Sequence";

    // field definition
    private static final String FIELD_DATA_DIR = "DataDir";
    private static final String FIELD_USE_PROJECT_ID = "UseProjectId";
    private static final String FIELD_SOURCEID_REGEX = "SourceIdRegex";
    private static final String FIELD_ORGANISM_REGEX = "OrganismRegex";
    private static final String FIELD_PROJECT_MAP_PREFIX = "ProjectMap_";
    private static final String FIELD_PROJECT_MAP_OTHER = FIELD_PROJECT_MAP_PREFIX
            + "Others";

    private File dataDir;
    private boolean useProjectId;

    private String sourceIdRegex;
    private String organismRegex;

    private String projectMapOthers;

    /**
     * @throws WsfServiceException
     * 
     */
    public DnaMotifSearchPlugin() throws WsfServiceException {
        super(PROPERTY_FILE);
        // load properties

        String dir = getProperty(FIELD_DATA_DIR);
        if (dir == null)
            throw new WsfServiceException(
                    "The required field in property file is missing: "
                            + FIELD_DATA_DIR);
        dataDir = new File(dir);
        logger.info("constructor(): dataDir: " + dataDir.getAbsolutePath()
                + "\n");

        String useProject = getProperty(FIELD_USE_PROJECT_ID);
        useProjectId = (useProject != null && useProject.equalsIgnoreCase("yes"));

        sourceIdRegex = getProperty(FIELD_SOURCEID_REGEX);
        organismRegex = getProperty(FIELD_ORGANISM_REGEX);

        projectMapOthers = getProperty(FIELD_PROJECT_MAP_OTHER);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getRequiredParameters()
     */
    @Override
    protected String[] getRequiredParameterNames() {
        return new String[] { PARAM_EXPRESSION }; // , PARAM_DATASET };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getColumns()
     */
    @Override
    protected String[] getColumns() {
        if (useProjectId) return new String[] { COLUMN_DYNSPAN_ID,
                COLUMN_PROJECT_ID, COLUMN_LOCATIONS, COLUMN_MATCH_COUNT,
                COLUMN_SEQUENCE };
        else return new String[] { COLUMN_DYNSPAN_ID, COLUMN_LOCATIONS,
                COLUMN_MATCH_COUNT, COLUMN_SEQUENCE };
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

        boolean datasetPresent = false;
        for (String param : params.keySet()) {
            logger.debug("Param - name=" + param + ", value="
                    + params.get(param));
            if (param.startsWith(PARAM_DATASET)) {
                datasetPresent = true;
                break;
            }
        }
        if (!datasetPresent)
            throw new WsfServiceException(
                    "The required dataset parameter is not presented.");

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
     */
    @Override
    protected WsfResult execute(String invokeKey, String userSignature, Map<String, String> params,
            String[] orderedColumns) throws WsfServiceException {
        logger.info("Invoking DnaMotifSearchPlugin...");

        // get parameters
        // String datasetIDs = params.get(PARAM_DATASET);
        String datasetIDs = null;
        for (String paramName : params.keySet()) {
            if (paramName.startsWith(PARAM_DATASET)) {
                datasetIDs = params.get(paramName);
                break;
            }
        }

        logger.info("execute(): datasetIDs: " + datasetIDs + "\n");
        logger.info("execute(): dataDir: " + dataDir.getName() + "\n");

        String expression = params.get(PARAM_EXPRESSION);

        // get optional parameters
        String colorCode = "Red";
        if (params.containsKey("ColorCode"))
            colorCode = params.get("ColorCode");
        int contextLength = 0;
        if (params.containsKey("ContextLength"))
            contextLength = Integer.parseInt(params.get("ContextLength"));
        if (contextLength <= 0) contextLength = 20;

        // translate the expression
        String regex = translateExpression(expression);

        // open the flatfile database assigned by the user
        try {
            Set<Match> matches = new HashSet<Match>();
            String[] dsIds = datasetIDs.split(",");

            // scan on each dataset, and add matched motifs in the result
            for (String dsId : dsIds) {
                logger.info("execute(): dsId: " + dsId
                        + " , input expression: " + expression
                        + " , expr translated to regex: " + regex + "\n");
                matches.addAll(findMatches(dsId.trim(), regex, colorCode,
                        contextLength));
            }

            // construct results
            String[][] result = prepareResult(matches, orderedColumns);
            WsfResult wsfResult = new WsfResult();
            wsfResult.setResult(result);
            return wsfResult;
        } catch (IOException ex) {
            throw new WsfServiceException(ex);
        }
    }
   
    /* 
     * @param fileID
     * @return
     * @throws IOException
     */
    private File openDataFile(String datasetID) throws IOException {
        logger.info("openDataFile(): dataDir: " + dataDir.getAbsolutePath()
                + ", datasetID: " + datasetID + "\n");

        File dataFile = new File(dataDir, datasetID);
        if (!dataFile.exists()) throw new IOException("The dataset \""
                + dataFile.toString() + "\" cannot be found.");
        else return dataFile;
    }

    private String translateExpression(String expression) {
        // remove spaces
        String regex = expression.replaceAll("\\s", "");

        // replace '(' to '{', ')' to '}', 'x' to '.', '<' to '(', '>' to ')'
        regex = regex.replace('(', '{');
        regex = regex.replace(')', '}');
        regex = regex.replace('x', '.');
        regex = regex.replace('<', '(');
        regex = regex.replace('>', ')');

        // remove '-'
        regex = regex.replaceAll("[\\-]", "");

        // replace numbers/number pairs by surrounding them with "{}"
        Pattern pattern = Pattern.compile("\\(\\d+(,\\d+)?\\)");
        Matcher matcher = pattern.matcher(regex);
        int prev = 0;
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            // get previous part
            sb.append(regex.substring(prev, matcher.start()));
            // replace '(' to '{'
            sb.append('{');
            sb.append(regex.subSequence(matcher.start() + 1, matcher.end() - 1));
            sb.append('}');
            prev = matcher.end();
        }
        // add last part into the buffer
        sb.append(regex.substring(prev));
        regex = sb.toString();

        // replace {string} into ignoring characters
        pattern = Pattern.compile("\\{(\\D+?)\\}");
        matcher = pattern.matcher(regex);
        prev = 0;
        sb.delete(0, sb.length());
        while (matcher.find()) {
            // get the previous part
            sb.append(regex.substring(prev, matcher.start()));
            // replace '{' with "[^"
            sb.append("[^");
            sb.append(regex.substring(matcher.start() + 1, matcher.end() - 1));
            sb.append(']');
            prev = matcher.end();
        }
        // add the last part
        sb.append(regex.substring(prev));
        regex = sb.toString();

        // Split the string by '{.*}'
        String splitStub = "SplitStub";
        regex = regex.replaceAll("\\{", splitStub + "{");
        regex = regex.replaceAll("\\}", "}" + splitStub);
        String[] parts = regex.split(splitStub);
        sb.delete(0, sb.length());
        for (String part : parts) {
            // check if it contains '{'
            if (part.indexOf('{') < 0) { // not containing '{.*}'
                // replace amino acids shortcuts
                part = part.replaceAll("0", CLASS_ACIDIC);
                part = part.replaceAll("1", CLASS_ALCOHOL);
                part = part.replaceAll("2", CLASS_ALIPHATIC);
                part = part.replaceAll("3", CLASS_AROMATIC);
                part = part.replaceAll("4", CLASS_BASIC);
                part = part.replaceAll("5", CLASS_CHARGED);
                part = part.replaceAll("6", CLASS_HYDROPHOBIC);
                part = part.replaceAll("7", CLASS_HYDROPHILIC);
                part = part.replaceAll("8", CLASS_POLAR);
                part = part.replaceAll("9", CLASS_SMALL);
                part = part.replaceAll("B", CLASS_TINY);
                part = part.replaceAll("Z", CLASS_TURNLIKE);
            }
            sb.append(part);
        }
        return sb.toString();
    }

    private Set<Match> findMatches(String datasetID, String regex,
            String colorCode, int contextLength) throws IOException,
            WsfServiceException {
        File datasetFile = openDataFile(datasetID);
        BufferedReader in = new BufferedReader(new FileReader(datasetFile));

        // check if the user use c-terminus
        if (regex.endsWith("$") && !regex.endsWith("\\**$"))
            regex = regex.substring(0, regex.length() - 1) + "\\**$";

        Set<Match> matches = new HashSet<Match>();
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

        // read header of the first sequence
        String line;
        line = in.readLine();
        while (true) {
            // get first dyn span id
            line = line.trim().replaceAll("\\s", " ");
            String dynSpanID = extractField(line, sourceIdRegex);

            // TEST
            logger.info("dynSpanID = " + dynSpanID);

            // get project id, if required
            String projectId = null;
            if (useProjectId) {
                String organism = extractField(line, organismRegex);

                // TEST
                 logger.debug("Organism from defline = " + organism);

                projectId = getProjectId(organism);

                // TEST
                 logger.debug("ProjectId = " + projectId);
            }

            StringBuffer seq = new StringBuffer();
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.length() == 0) continue;
                // check if we reach the header of the next sequence
                // if so, we just finishe reading the sequence, exit inner loop
                if (line.charAt(0) == '>') break;
                seq.append(line);
            }
            // check if we reach the end of the file
            if (line == null) break;

            // scan the sequence to find all matched locations
            Match match = findLocations(dynSpanID, projectId, pattern, seq.toString(),
                    colorCode, contextLength);
            if (match != null && !matches.contains(match)) {
                if (useProjectId) match.projectId = projectId;
                matches.add(match);
            }
        }
        return matches;
    }

    private Match findLocations(String dynSpanID, String projectId, Pattern pattern,
            String sequence, String colorCode, int contextLength)
            throws WsfServiceException {
        Match match = new Match();
        match.dynSpanID = dynSpanID;
        match.projectId = projectId;
        StringBuffer sbLoc = new StringBuffer();
        StringBuffer sbSeq = new StringBuffer();
        int prev = 0;

        Matcher matcher = pattern.matcher(sequence);
        while (matcher.find()) {
            // add locations
            if (sbLoc.length() != 0) sbLoc.append(", ");
            sbLoc.append('(');
            sbLoc.append(matcher.start());
            sbLoc.append("-");
            sbLoc.append(matcher.end() - 1);
            sbLoc.append(')');

            // obtain the context sequence
            if ((matcher.start() - prev) <= (contextLength * 2)) {
                // no need to trim
                sbSeq.append(sequence.substring(prev, matcher.start()));
            } else { // need to trim some
                if (prev != 0)
                    sbSeq.append(sequence.substring(prev, prev + contextLength));
                sbSeq.append("... ");
                sbSeq.append(sequence.substring(
                        matcher.start() - contextLength, matcher.start()));
            }
            sbSeq.append("<font color=\"" + colorCode + "\">");
            sbSeq.append(sequence.substring(matcher.start(), matcher.end()));
            sbSeq.append("</font>");
            prev = matcher.end();
            match.matchCount++;
        }
        if (match.matchCount == 0) return null;

        // grab the last context
        if ((prev + contextLength) < sequence.length()) {
            sbSeq.append(sequence.substring(prev, prev + contextLength));
            sbSeq.append("... ");
        } else {
            sbSeq.append(sequence.substring(prev));
        }
        match.locations = sbLoc.toString();
        match.sequence = sbSeq.toString();
        return match;
    }

    private String[][] prepareResult(Set<Match> matches, String[] cols) {
        String[][] result = new String[matches.size()][cols.length];
        // create an column order map
        Map<String, Integer> orders = new HashMap<String, Integer>();
        for (int i = 0; i < cols.length; i++)
            orders.put(cols[i], i);

        int i = 0;
        for (Match match : matches) {
            result[i][orders.get(COLUMN_DYNSPAN_ID)] = match.dynSpanID;
            result[i][orders.get(COLUMN_LOCATIONS)] = match.locations;
            result[i][orders.get(COLUMN_MATCH_COUNT)] = Integer.toString(match.matchCount);
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

    private String extractField(String defline, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(defline);
        if (matcher.find()) {
            // the match is located at group 1
            return matcher.group(1);
        } else return null;
    }

    protected String getProjectId(String organism) {
        String mapKey = FIELD_PROJECT_MAP_PREFIX + organism;
        String projectId = getProperty(mapKey);
        if (projectId == null) projectId = projectMapOthers;
        return projectId;
    }
}
