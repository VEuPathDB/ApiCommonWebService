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
import org.gusdb.fgputil.runtime.InstanceManager;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;
import org.gusdb.fgputil.client.ClientUtil;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.db.runner.SQLRunnerException;

/**
 * @author markhick
 * 
 */
public class PhyleticPatternPlugin extends AbstractPlugin {

    private static final Logger logger = Logger.getLogger(PhyleticPatternPlugin.class);

    private static final String PROPERTY_FILE = "phyleticPattern-config.xml";

    // required parameter definition
    public static final String PARAM_ORGANISM = "organism";
    public static final String PARAM_PHYLETIC_PATTERN = "phyletic_pattern";

    // required result column definition
    public static final String COLUMN_SOURCE_ID = "source_id";
    public static final String COLUMN_GENE_SOURCE_ID = "gene_source_id";
    public static final String COLUMN_PROJECT_ID = "project_id";
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
        return new String[] { PARAM_ORGANISM, PARAM_PHYLETIC_PATTERN };
    }

    @Override
    public String[] getColumns(PluginRequest request) {
        return new String[] { COLUMN_SOURCE_ID, COLUMN_GENE_SOURCE_ID, COLUMN_PROJECT_ID,
			      COLUMN_MATCHED_RESULT, COLUMN_ORTHOMCL_NAME };
    }

    @Override
    public void validateParameters(PluginRequest request)
            throws PluginUserException {

        Map<String, String> params = request.getParams();
        String organism = params.get(PARAM_ORGANISM);
        String phyleticPattern = params.get(PARAM_PHYLETIC_PATTERN);

        // how validate organism?  are there examples?
	// how validate phyleticPattern?  use ortho code or component site code?
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
        String phyleticPattern = params.get(PARAM_PHYLETIC_PATTERN);
        String organism = params.get(PARAM_ORGANISM);

	Set<String> setOfGroups;
	setOfGroups = getSetOfGroupsFromOrthomcl(postUrl,phyleticPattern);
	
	WdkModel wdkModel = InstanceManager.getInstance(WdkModel.class, projectId);
	DataSource appDs = wdkModel.getAppDb().getDataSource();
	String sql = "SELECT gene_source_id, orthomcl_name FROM ApidbTuning.TranscriptAttributes WHERE taxon_id IN ("
	             + organism + ")";
	try {
	     new SQLRunner(appDs, sql).executeQuery(rs -> {
		while (rs.next()) {
		    if (setOfGroups.contains(rs.getString(2))) {
			try {
			    addGene(response,rs.getString(1),rs.getString(2),request.getOrderedColumns());
			} catch (PluginModelException|PluginUserException e) {
			    throw new SQLRunnerException(e);
			} 
		    }
		}
		return null;
	    });
	} catch (SQLRunnerException e) {
	    try {     // unwrap the exception thrown by sql runner and convert to pluginmodel or pluginuser exception
		throw e.getCause();
	    } catch (PluginModelException|PluginUserException e2) {
		throw e2;
	    } catch (Exception e2) {
		throw new PluginModelException("Unable to get gene_source_id and orthomcl_name from database",e2);
	    } catch (Throwable e2) {
		throw new PluginModelException("Unable to get gene_source_id and orthomcl_name from database",e2);
	    }
	}
	return 0;
    }
	    
    private void addGene(PluginResponse response, String geneId, String orthomclGroup, String[] orderedColumns)	
	throws PluginModelException, PluginUserException {
        // create a map of <column/position>
        Map<String, Integer> columns = new HashMap<String, Integer>(
                orderedColumns.length);
        for (int i = 0; i < orderedColumns.length; i++) {
            columns.put(orderedColumns[i], i);
        }

	String[] row = new String[7];
	row[columns.get(COLUMN_GENE_SOURCE_ID)] = geneId;
	row[columns.get(COLUMN_PROJECT_ID)] = projectId;
	row[columns.get(COLUMN_MATCHED_RESULT)] = new String("Y");
	row[columns.get(COLUMN_SOURCE_ID)] = null;
	row[columns.get(COLUMN_ORTHOMCL_NAME)] = orthomclGroup;
	response.addRow(row);
    }

    private Set getSetOfGroupsFromOrthomcl(String postUrl,String phyleticPattern)
	throws PluginModelException, PluginUserException {

	String bodyText = "{\"searchConfig\":{\"parameters\":{\"phyletic_expression\":\"" +
	    phyleticPattern +
	    "\"},\"wdkWeight\": 10},\"reportConfig\":{\"attributes\": [\"primary_key\"],\"includeHeader\":false,\"attachmentType\":\"plain\"}}";
	Set<String> groupIds = new HashSet<String>();

	try (
	    InputStream tabularStream = ClientUtil.makeAsyncPostRequest(
	         postUrl,                     // request URL
		 bodyText,                    // request body
		 MediaType.TEXT_PLAIN,        // request type
		 MediaType.WILDCARD)          // response type
		.getInputStream();
	    BufferedReader reader = new BufferedReader(new InputStreamReader(tabularStream))
	     ) {
		String groupId = reader.readLine().strip();
		while (groupId != null) {
		    groupIds.add(groupId);
		    groupId = reader.readLine().strip();
		}
		return groupIds;
	} catch (Exception e) {
	    throw new PluginModelException("Unable to get groups from orthomcl", e.getCause());
	}	
    }

}
