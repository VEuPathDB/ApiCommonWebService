/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.keywordsearch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author John I
 * @created Aug 23, 2006
 */
public class KeywordSearchPlugin extends WsfPlugin {

    private class Match {

        public String geneID;
        public String locations;
        public int matchCount = 0;
        public String sequence;
    }

    private static final String PROPERTY_FILE = "keywordSearch-config.xml";

    // required parameter definition
    public static final String PARAM_DATASET = "Dataset";
    public static final String PARAM_EXPRESSION = "Expression";

    public static final String COLUMN_GENE_ID = "GeneID";
    public static final String COLUMN_LOCATIONS = "Locations";
    public static final String COLUMN_MATCH_COUNT = "MatchCount";
    public static final String COLUMN_SEQUENCE = "Sequence";

    // field definition
    private static final String FIELD_DATA_DIR = "DataDir";
    private static final String FIELD_SOURCEID_REGEX = "SourceIdRegex";
    private static final String FIELD_MAX_LENGTH = "MaxLength";

    private File dataDir;
    private String sourceIdRegex;
    private int maxLen;

    /**
     * @throws WsfServiceException
     * 
     */
    public KeywordSearchPlugin() throws WsfServiceException {
        super(PROPERTY_FILE);
        // load properties
        String dir = getProperty(FIELD_DATA_DIR);
        if (dir == null)
            throw new WsfServiceException(
                    "The required field in property file is missing: "
                            + FIELD_DATA_DIR);
        dataDir = new File(dir);
        logger.info("constructor(): dataDir: " + dataDir.getName() + "\n");

        sourceIdRegex = getProperty(FIELD_SOURCEID_REGEX);

        String maxLength = getProperty(FIELD_MAX_LENGTH);
        if (maxLength != null) maxLen = Integer.parseInt(maxLength);
        else maxLen = 4000; // default value
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
        return new String[] { COLUMN_GENE_ID };
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
    protected String[][] execute(Map<String, String> params,
            String[] orderedColumns) throws WsfServiceException {
        logger.info("Invoking KeywordSearchPlugin...");

        // get parameters
        // String datasetIDs = params.get(PARAM_DATASET);
        String datasetIDs = null;
        for (String paramName : params.keySet()) {
            if (paramName.startsWith(PARAM_DATASET)) {
                datasetIDs = params.get(paramName);
                break;
            }
        }

	// for debugging
	datasetIDs = "";

        String expression = params.get(PARAM_EXPRESSION);

        // translate the expression
        String regex = translateExpression(expression);

        // open the flatfile database assigned by the user 

	/*
	  try { */
            List<Match> matches = new ArrayList<Match>();
	    /*            String[] dsIds = datasetIDs.split(",");

            // scan on each dataset, and add matched keywords in the result
            for (String dsId : dsIds) {
                logger.info("execute(): dsId: " + dsId + "\n");

            }
	*/

            // construct results
            return prepareDummyResult(matches, orderedColumns);
	    /*        } catch (IOException ex) {
            throw new WsfServiceException(ex);
        }
	    */
    }

    /**
     * 
     * @param fileID
     * @return
     * @throws IOException
     */
    private File openDataFile(String datasetID) throws IOException {
        logger.info("openDataFile(): dataDir: " + dataDir.getName()
                + ", datasetID: " + datasetID + "\n");

        File dataFile = new File(dataDir, datasetID);
        if (!dataFile.exists()) throw new IOException("The dataset \""
                + datasetID + "\" cannot be found.");
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
            }
            sb.append(part);
        }
        return sb.toString();
    }

    private List<Match> findMatches(String datasetID, String regex,
            String colorCode, int contextLength) throws IOException,
            WsfServiceException {
        File datasetFile = openDataFile(datasetID);
        BufferedReader in = new BufferedReader(new FileReader(datasetFile));

        List<Match> matches = new ArrayList<Match>();
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

        // read header of the first sequence
        String line;
        line = in.readLine();
        while (true) {
            // get first gene id
            line = line.trim().replaceAll("\\s", " ");
            String geneID = extractField(line, sourceIdRegex);

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
            Match match = findLocations(geneID, pattern, seq.toString(),
                    colorCode, contextLength);
            if (match != null) matches.add(match);
        }
        return matches;
    }

    private Match findLocations(String geneID, Pattern pattern,
            String sequence, String colorCode, int contextLength)
            throws WsfServiceException {
        Match match = new Match();
        match.geneID = geneID;
        StringBuffer sbLoc = new StringBuffer();
        StringBuffer sbSeq = new StringBuffer();
        int prev = 0;

        Matcher matcher = pattern.matcher(sequence);
        while (matcher.find()) {
            // add locations
            if (sbLoc.length() != 0) sbLoc.append(",");
            sbLoc.append('(');
            sbLoc.append(matcher.start());
            sbLoc.append("-");
            sbLoc.append(matcher.end() - 1);
            sbLoc.append(')');

            // abtain the context sequence
            if ((matcher.start() - prev) <= (contextLength * 2)) {
                // no need to trim
                sbSeq.append(sequence.substring(prev, matcher.start()));
            } else { // need to trim some
                if (prev != 0)
                    sbSeq.append(sequence.substring(prev, prev + contextLength));
                sbSeq.append("...");
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
            sbSeq.append("...");
        } else {
            sbSeq.append(sequence.substring(prev));
        }
        // check if the match is too unspecific, that is, the length is bigger
        // than 400 characters (the maximum length of varchar in Oracle); if so,
        // throw an exception to state that
        if (sbSeq.length() > maxLen || sbLoc.length() > maxLen)
            throw new WsfServiceException("The expression hits too many "
                    + "locations than the system can handle. Please make it "
                    + "more specific.");
        match.locations = sbLoc.toString();
        match.sequence = sbSeq.toString();
        return match;
    }

    private String[][] prepareResult(List<Match> matches, String[] cols) {
        String[][] result = new String[matches.size()][cols.length];
        // create an column order map
        Map<String, Integer> orders = new HashMap<String, Integer>();
        for (int i = 0; i < cols.length; i++)
            orders.put(cols[i], i);

        for (int i = 0; i < matches.size(); i++) {
            Match match = matches.get(i);

            result[i][orders.get(COLUMN_GENE_ID)] = match.geneID;
            result[i][orders.get(COLUMN_LOCATIONS)] = match.locations;
            result[i][orders.get(COLUMN_MATCH_COUNT)] = Integer.toString(match.matchCount);
            result[i][orders.get(COLUMN_SEQUENCE)] = match.sequence;
        }
        return result;
    }

    private String[][] prepareDummyResult(List<Match> matches, String[] cols) {
        String[][] result = new String[1][cols.length];
        // create an column order map
        Map<String, Integer> orders = new HashMap<String, Integer>();
        for (int i = 0; i < cols.length; i++)
            orders.put(cols[i], i);


        result[0][orders.get(COLUMN_GENE_ID)] = "PF13_0021";

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
}
