/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.orthomcl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import javax.sql.DataSource;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;
import org.gusdb.fgputil.Client.ClientUtil;
import org.gusdb.fgputil.db.runner.SQLRunner;

/**
 * @author markhick
 * 
 */
public class PhyleticPatternPlugin extends AbstractPlugin {

    private static final Logger logger = Logger.getLogger(PhyleticPatternPlugin.class);

    private static final String PROPERTY_FILE = "phyleticPattern-config.xml";

    // required parameter definition
    public static final String PARAM_ORGANISM = "organism";
    public static final String PARAM_PROFILE_PATTERN = "profile_pattern";

    // required result column definition
    public static final String COLUMN_SOURCE_ID = "source_id";
    public static final String COLUMN_GENE_ID = "GeneID";
    public static final String COLUMN_PROJECT_ID = "ProjectId";
    public static final String COLUMN_MATCHED_RESULT = "matched_result";
    public static final String COLUMN_ORTHOMCL_NAME = "orthomcl_name";

    // field definition
    private static final String FIELD_DB_CONNECTION = "dbConnection";
    private static final String FIELD_DB_LOGIN = "dbLogin";
    private static final String FIELD_DB_PASSWORD = "dbPassword";
    private static final String FIELD_PROJECT_ID = "projectId";
    private static final String FIELD_POST_URL = "postUrl";

    private String dbConnection;
    private String dbLogin;
    private String dbPassword;
    private String projectId;
    private String postUrl;

    public PhyleticPatternPlugin() {
        super(PROPERTY_FILE);
    }

    // load properties
    @Override
    public void initialize(PluginRequest request) throws PluginModelException {

        super.initialize(request);

        // load properties

        dbConnection = getProperty(FIELD_DB_CONNECTION);
        dbLogin = getProperty(FIELD_DB_LOGIN);
        dbPassword = getProperty(FIELD_DB_PASSWORD);
        projectId = getProperty(FIELD_PROJECT_ID);
        postUrl = getProperty(FIELD_POST_URL);

        if (dbConnection == null)
            throw new PluginModelException("The " + FIELD_DB_CONNECTION
                    + "field is missing from the configuration file");
        if (dbLogin == null)
            throw new PluginModelException("The " + FIELD_DB_LOGIN
                    + "field is missing from the configuration file");
        if (dbPassword == null)
            throw new PluginModelException("The " + FIELD_DB_PASSWORD
                    + "field is missing from the configuration file");
        if (projectId == null)
            throw new PluginModelException("The " + FIELD_PROJECT_ID
                    + "field is missing from the configuration file");
        if (postUrl == null)
            throw new PluginModelException("The " + FIELD_POST_URL
                    + "field is missing from the configuration file");
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#getRequiredParameterNames()
     */
    @Override
    public String[] getRequiredParameterNames() {
        return new String[] { PARAM_ORGANISM, PARAM_PROFILE_PATTERN };
    }

    @Override
    public String[] getColumns(PluginRequest request) {
        return new String[] { COLUMN_SOURCE_ID, COLUMN_GENE_ID, COLUMN_PROJECT_ID,
			      COLUMN_MATCHED_RESULT, COLUMN_ORTHOMCL_NAME };
    }

    @Override
    public void validateParameters(PluginRequest request)
            throws PluginUserException {

        Map<String, String> params = request.getParams();
        String organism = params.get(PARAM_ORGANISM);
        String profilePattern = params.get(PARAM_PROFILE_PATTERN);

        // how validate organism?  are there examples?
	// how validate profilePattern?  use ortho code or component site code?
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#execute(java.util.Map,
     * java.lang.String[])
     */
    @Override
    public int execute(PluginRequest request, PluginResponse response) throws PluginModelException, PluginUserException {
        logger.debug("Invoking PhyleticPatternPlugin...");

        Map<String, String> params = request.getParams();
        String profilePattern = params.get(PARAM_PROFILE_PATTERN);
        String organism = params.get(PARAM_ORGANISM);
	
	Set<String> setOfGroups = getSetOfGroupsFromOrthomcl(postUrl,profilePattern);

	DataSource appDs = wdkModel.getAppDb().getDataSource();
	String sql = "SELECT source_id, orthomcl_name FROM ApidbTuning.TranscriptAttributes WHERE taxon_id IN ("
	             + organism + ")";
	new SQLRunner(appDs, sql).executeQuery(rs -> {
		while (rs.next()) {
		    if (setOfGroups.contains(rs.getString(2)) {
			    addGene(response,rs.getString(1),request.getOrderedColumns());
			    // use something like prepareResult method below??
		    }
		}
	});










1
        // prepare the command

        String[] cmds = prepareCommand(params);

        // measure the time used for invocation
        long start = System.currentTimeMillis();
        try {
            // invoke the command, and set default 10 min as timeout limit
            StringBuffer output = new StringBuffer();

            int signal = invokeCommand(cmds, output, 10 * 60);
            long end = System.currentTimeMillis();
            logger.debug("Invocation takes: " + ((end - start) / 1000.0)
                    + " seconds");

            if (signal != 0)
                throw new PluginModelException("The invocation is failed: "
                        + output);

            // prepare the result
            String queryGeneId = params.get(PARAM_GENE_ID);
            prepareResult(response, output.toString(),
                    request.getOrderedColumns(), queryGeneId);

            return signal;
        } catch (IOException ex) {
            long end = System.currentTimeMillis();
            logger.debug("Invocation takes: " + ((end - start) / 1000.0)
                    + " seconds");

            throw new PluginModelException(ex);
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

        cmds.add(params.get(PARAM_SCALE_FACTOR));
        cmds.add(params.get(PARAM_MIN_POINTS));
        cmds.add(params.get(PARAM_MISSING_PTS_PERCENT));

        cmds.add(dbConnection);
        cmds.add(dbLogin);
        cmds.add(dbPassword);

        //cmds.add(params.get(PARAM_WEIGHTS_STRING));


        String[] array = new String[cmds.size()];
        cmds.toArray(array);
        return array;
    }

    private void prepareResult(PluginResponse response, String content, String[] orderedColumns,
            String queryGeneId) throws IOException, PluginModelException, PluginUserException {
        // create a map of <column/position>
        Map<String, Integer> columns = new HashMap<String, Integer>(
                orderedColumns.length);
        for (int i = 0; i < orderedColumns.length; i++) {
            columns.put(orderedColumns[i], i);
        }

        // check if the output contains error message
        if (content.indexOf("ERROR:") >= 0)
            throw new PluginModelException(content);

        // read from the buffered stream
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
                throw new PluginModelException("Invalid output format:\n"
                        + content);

            String geneId = parts[0].trim();
            double distance = Double.parseDouble(parts[1]);

            // do not skip the query gene, and include it in the result list
            // if (geneId.equalsIgnoreCase(queryGeneId)) continue;

            String[] row = new String[7];
            row[columns.get(COLUMN_GENE_ID)] = geneId;
            row[columns.get(COLUMN_PROJECT_ID)] = projectId;
            row[columns.get(COLUMN_MATCHED_RESULT)] = new String("Y");
            row[columns.get(COLUMN_SOURCE_ID)] = null;
            row[columns.get(COLUMN_DISTANCE)] = format.format(distance);
            row[columns.get(COLUMN_SHIFT)] = parts[2];
            row[columns.get(COLUMN_QUERY_GENE_ID)] = queryGeneId;
            response.addRow(row);
        }
        in.close();
    }

    private Set getSetOfGroupsFromOrthomcl(String postUrl,String profilePattern)
	    throws PluginModelException, PluginUserException {

	String bodyText = "{\"searchConfig\":{\"parameters\":{\"phyletic_expression\":\"" +
	              profilePattern +
	              "\"},\"wdkWeight\": 10},\"reportConfig\":{\"attributes\": [\"primary_key\"],\"tables\":[]}}}";
	Set<String> groupIds = new HashSet<String>();
	try (InputStream tabularStream = ClientUtil.makeAsyncPostRequest(
	    postUrl,                     // request URL
	    bodyText,                    // request body
	    MediaType.APPLICATION_JSON,  // request type
	    MediaType.WILDCARD)          // response type
	    .getInputStream()) {
		// process the stream
		BufferedReader reader = new BufferedReader(new InputStreamReader(tabularStream));
		String groupId = reader.readline();
		groupIds.add(groupId);
	    }
	)
	return groupIds;
    }
