/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.profile;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfResult;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author xingao
 * 
 */
public class ProfileSimilarityPlugin extends WsfPlugin {

    private static final String PROPERTY_FILE = "profileSimilarity-config.xml";

    // required parameter definition
    public static final String PARAM_GENE_ID = "ProfileGeneId";
    public static final String PARAM_DISTANCE_METHOD = "ProfileDistanceMethod";
    public static final String PARAM_NUM_RETURN = "ProfileNumToReturn";
    public static final String PARAM_PROFILE_SET = "ProfileProfileSet";
    public static final String PARAM_SEARCH_GOAL = "ProfileSearchGoal";
    //    public static final String PARAM_SCALE_DATA = "ProfileScaleData";
    public static final String PARAM_TIME_SHIFT = "ProfileTimeShift";
    //    public static final String PARAM_SHIFT_PLUS_MINUS = "ProfileShiftPlusMinus";

    // required result column definition
    public static final String COLUMN_GENE_ID = "GeneID";
    public static final String COLUMN_PROJECT_ID = "ProjectId";
    public static final String COLUMN_QUERY_GENE_ID = "QueryGeneId";
    public static final String COLUMN_DISTANCE = "Distance";
    public static final String COLUMN_SHIFT = "Shift";

    // field definition
    private static final String FIELD_PERL_EXECUTABLE = "perlExecutable";
    private static final String FIELD_PERL_SCRIPT = "perlScript";
    private static final String FIELD_NUM_TIME_POINTS = "numTimePoints";
    private static final String FIELD_SKIP_TIMES = "skipTimes";
    private static final String FIELD_DB_CONNECTION = "dbConnection";
    private static final String FIELD_DB_LOGIN = "dbLogin";
    private static final String FIELD_DB_PASSWORD = "dbPassword";
    private static final String FIELD_PROJECT_ID = "projectId";

    private String perlExec;
    private String perlScript;
    private String numTimePoints;
    private String skipTimes;
    private String dbConnection;
    private String dbLogin;
    private String dbPassword;
    private String projectId;

    /**
     * @throws WsfServiceException
     * 
     */
    public ProfileSimilarityPlugin() throws WsfServiceException {
        super(PROPERTY_FILE);

        // load properties
        perlExec = getProperty(FIELD_PERL_EXECUTABLE);
        perlScript = getProperty(FIELD_PERL_SCRIPT);
	numTimePoints = getProperty(FIELD_NUM_TIME_POINTS);
	skipTimes = getProperty(FIELD_SKIP_TIMES);
        dbConnection = getProperty(FIELD_DB_CONNECTION);
        dbLogin = getProperty(FIELD_DB_LOGIN);
        dbPassword = getProperty(FIELD_DB_PASSWORD);
        projectId = getProperty(FIELD_PROJECT_ID);

        if (perlExec == null)
            throw new WsfServiceException("The " + FIELD_PERL_EXECUTABLE
                    + " field is missing from the configuration file.");
        if (perlScript == null)
            throw new WsfServiceException("The " + FIELD_PERL_SCRIPT
                    + "field is missing from the configuration file");
        if (numTimePoints == null)
            throw new WsfServiceException("The " + FIELD_NUM_TIME_POINTS
                    + "field is missing from the configuration file");
        if (skipTimes == null)
            throw new WsfServiceException("The " + FIELD_SKIP_TIMES
                    + "field is missing from the configuration file");
        if (dbConnection == null)
            throw new WsfServiceException("The " + FIELD_DB_CONNECTION
                    + "field is missing from the configuration file");
        if (dbLogin == null)
            throw new WsfServiceException("The " + FIELD_DB_LOGIN
                    + "field is missing from the configuration file");
        if (dbPassword == null)
            throw new WsfServiceException("The " + FIELD_DB_PASSWORD
                    + "field is missing from the configuration file");
        if (projectId == null)
            throw new WsfServiceException("The " + FIELD_PROJECT_ID
                    + "field is missing from the configuration file");
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#getRequiredParameterNames()
     */
    @Override
    protected String[] getRequiredParameterNames() {
        return new String[] { PARAM_GENE_ID, PARAM_DISTANCE_METHOD,
			      PARAM_NUM_RETURN, PARAM_PROFILE_SET, PARAM_SEARCH_GOAL,
			      //PARAM_SCALE_DATA, PARAM_SHIFT_PLUS_MINUS, 
			      PARAM_TIME_SHIFT };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#getColumns()
     */
    @Override
    protected String[] getColumns() {
        return new String[] { COLUMN_GENE_ID, COLUMN_PROJECT_ID,
                COLUMN_DISTANCE, COLUMN_SHIFT, COLUMN_QUERY_GENE_ID };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#validateParameters(java.util.Map)
     */
    @Override
    protected void validateParameters(Map<String, String> params)
            throws WsfServiceException {
        // validate distance method
        String distanceMethod = params.get(PARAM_DISTANCE_METHOD);
        if (!distanceMethod.equalsIgnoreCase("pearson_correlation")
                && !distanceMethod.equalsIgnoreCase("euclidean_distance"))
            throw new WsfServiceException("Invalid distance method: "
                    + distanceMethod + ". Should be either "
                    + "\"pearson_correlation\" or \"euclidean_distance\"");
        params.put(PARAM_DISTANCE_METHOD, distanceMethod.toLowerCase());

        // validdate the size of result
        int numReturn = 0;
        try {
            numReturn = Integer.parseInt(params.get(PARAM_NUM_RETURN));
        } catch (NumberFormatException e) {
            throw new WsfServiceException("Invalid size of the result: "
                    + numReturn + ", a number is expected.");
        }
        // validate search goal
        String searchGoal = params.get(PARAM_SEARCH_GOAL);
        if (!searchGoal.equalsIgnoreCase("similar")
                && !searchGoal.equalsIgnoreCase("dissimilar"))
            throw new WsfServiceException("Invalid search goal: " + searchGoal
                    + ". Should be either \"similar\" or \"dissimilar\"");
        params.put(PARAM_SEARCH_GOAL, searchGoal.toLowerCase());


        // validate scale data - NOT IMPLEMENTED
        /**  String scaleData = params.get(PARAM_SCALE_DATA);
        if (scaleData.equalsIgnoreCase("true") || scaleData.equals("1")) {
            scaleData = "1";
        } else scaleData = "0";
	params.put(PARAM_SCALE_DATA, scaleData);  **/

        // validate time_shift
        int timeShift = 0;
        try {
            timeShift = Integer.parseInt(params.get(PARAM_TIME_SHIFT));
            if (timeShift < -24 || timeShift > 24)
                throw new WsfServiceException(
                        "The timeShift should be within the range of [-24 - 24]");
        } catch (NumberFormatException ex) {
            throw new WsfServiceException("Invalid time shift value: " + timeShift
                    + ", a number is expected.");
        }
	/*** 
        // validate time_shift_minus_plus
        int shiftPlusMinus = 0;
        try {
            shiftPlusMinus = Integer.parseInt(params.get(PARAM_SHIFT_PLUS_MINUS));
            if (shiftPlusMinus < 0 || shiftPlusMinus > 12)
                throw new WsfServiceException("The time shift_plus/minus should be within "
                        + "the range of [0 - 12]");
        } catch (NumberFormatException e) {
            throw new WsfServiceException("Invalid time shift_plus/minus value: " + shiftPlusMinus
                    + ", a number is expected.");
        }  
	***/
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#execute(java.util.Map,
     *      java.lang.String[])
     */
    @Override
    protected WsfResult execute(String invokeKey, Map<String, String> params,
            String[] orderedColumns) throws WsfServiceException {
        logger.info("Invoking ProfileSimilarity Plugin...");

        // prepare the command
        String[] cmds = prepareCommand(params);

        // measure the time used for invocation
        long start = System.currentTimeMillis();
        try {
            // invoke the command, and set default 10 min as timeout limit
            StringBuffer output = new StringBuffer();

            int signal = invokeCommand(cmds, output, 10 * 60);
            long end = System.currentTimeMillis();
            logger.info("Invocation takes: " + ((end - start) / 1000.0)
                    + " seconds");

            if (signal != 0)
                throw new WsfServiceException("The invocation is failed: "
                        + output);

            // prepare the result
            String queryGeneId = params.get(PARAM_GENE_ID);
            String[][] result = prepareResult(output.toString(),
                    orderedColumns, queryGeneId);

            WsfResult wsfResult = new WsfResult();
            wsfResult.setResult(result);
            wsfResult.setSignal(signal);
            return wsfResult;
        } catch (IOException ex) {
            long end = System.currentTimeMillis();
            logger.info("Invocation takes: " + ((end - start) / 1000.0)
                    + " seconds");

            throw new WsfServiceException(ex);
        }

    }

    private String[] prepareCommand(Map<String, String> params) {
        List<String> cmds = new ArrayList<String>();

        cmds.add(perlExec);
        cmds.add(perlScript);
        cmds.add(params.get(PARAM_DISTANCE_METHOD));
        cmds.add(params.get(PARAM_NUM_RETURN));
        cmds.add(params.get(PARAM_GENE_ID));
        cmds.add(params.get(PARAM_PROFILE_SET));
        cmds.add(params.get(PARAM_SEARCH_GOAL));
        //cmds.add(params.get(PARAM_SCALE_DATA));
        cmds.add(params.get(PARAM_TIME_SHIFT));
	//    cmds.add(params.get(PARAM_SHIFT_PLUS_MINUS));
        cmds.add(numTimePoints);
        cmds.add(skipTimes);
        cmds.add(dbConnection);
        cmds.add(dbLogin);
        cmds.add(dbPassword);

        String[] array = new String[cmds.size()];
        cmds.toArray(array);
        return array;
    }

    private String[][] prepareResult(String content, String[] orderedColumns,
            String queryGeneId) throws WsfServiceException, IOException {
        // create a map of <column/position>
        Map<String, Integer> columns = new HashMap<String, Integer>(
                orderedColumns.length);
        for (int i = 0; i < orderedColumns.length; i++) {
            columns.put(orderedColumns[i], i);
        }

        // check if the output contains error message
        if (content.indexOf("ERROR:") >= 0)
            throw new WsfServiceException(content);

        // read from the buffered stream
        List<String[]> results = new ArrayList<String[]>();
        BufferedReader in = new BufferedReader(new InputStreamReader(
                new ByteArrayInputStream(content.getBytes())));

        NumberFormat format = NumberFormat.getNumberInstance();
        format.setMaximumFractionDigits(4);
        format.setMinimumFractionDigits(4);

        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.length() == 0) continue;
            String[] parts = line.split("\t");

            if (parts.length != 3)
                throw new WsfServiceException("Invalid output format:\n"
                        + content);

            String geneId = parts[0].trim();
            double distance = Double.parseDouble(parts[1]);

            // do not skip the query gene, and include it in the result list
            // if (geneId.equalsIgnoreCase(queryGeneId)) continue;

            String[] row = new String[5];
            row[columns.get(COLUMN_GENE_ID)] = geneId;
            row[columns.get(COLUMN_PROJECT_ID)] = projectId;
            row[columns.get(COLUMN_DISTANCE)] = format.format(distance);
            row[columns.get(COLUMN_SHIFT)] = parts[2];
            row[columns.get(COLUMN_QUERY_GENE_ID)] = queryGeneId;
            results.add(row);
        }
        in.close();

        String[][] array = new String[results.size()][5];
        for (int i = 0; i < array.length; i++) {
            array[i] = results.get(i);
        }
        return array;
    }
}
