/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.textsearch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfResult;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author John I
 * @created Nov 16, 2008
 */
public class KeywordSearchPlugin extends WsfPlugin {

    private static final String PROPERTY_FILE = "keywordsearch-config.xml";

    // required parameter definition
    public static final String PARAM_TEXT_EXPRESSION = "text_expression";
    public static final String PARAM_DATASETS = "text_fields";
    public static final String PARAM_SPECIES_NAME = "text_search_organism";
    public static final String PARAM_PROJECT_ID = "project_id";
    public static final String PARAM_COMPONENT_INSTANCE = "component_instance";

    public static final String COLUMN_GENE_ID = "GeneID";
    public static final String COLUMN_PROJECT_ID = "ProjectId";
    public static final String COLUMN_DATASETS = "Datasets";
    public static final String COLUMN_SNIPPETS = "Snippets";

    // field definition
    private static final String FIELD_COMMENT_INSTANCE = "commentInstance";
    private static final String FIELD_COMMENT_PASSWORD = "commentPassword";

    private String projectId;
    private Connection commentDbConnection;
    private String commentInstance;
    private String commentPassword;

    /**
     * @throws WsfServiceException
     * 
     */
    public KeywordSearchPlugin() throws WsfServiceException {
        super(PROPERTY_FILE);
        // load properties
        commentInstance = getProperty(FIELD_COMMENT_INSTANCE);
        if (commentInstance == null)
            throw new WsfServiceException(
                    "Required field in property file is missing: "
                            + FIELD_COMMENT_INSTANCE);

        commentPassword = getProperty(FIELD_COMMENT_PASSWORD);
        if (commentPassword == null)
            throw new WsfServiceException(
                    "Required field in property file is missing: "
                            + FIELD_COMMENT_PASSWORD);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getRequiredParameters()
     */
    @Override
    protected String[] getRequiredParameterNames() {
        return new String[] { PARAM_TEXT_EXPRESSION, PARAM_DATASETS };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getColumns()
     */
    @Override
    protected String[] getColumns() {
        return new String[] { COLUMN_GENE_ID, COLUMN_PROJECT_ID, COLUMN_DATASETS, COLUMN_SNIPPETS };
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
    protected WsfResult execute(String invokeKey, Map<String, String> params,
            String[] orderedColumns) throws WsfServiceException {
        logger.info("Invoking KeywordSearchPlugin...");

        // get parameters
        String datasets = params.get(PARAM_DATASETS);
        String speciesName = params.get(PARAM_SPECIES_NAME);
        String projectId = params.get(PARAM_PROJECT_ID);
        String textExpression = params.get(PARAM_TEXT_EXPRESSION);
        String componentInstance = params.get(PARAM_COMPONENT_INSTANCE);

        Map<String, Set<String>> commentMatches = new HashMap<String, Set<String>>();
        Map<String, Set<String>> tableMatches = new HashMap<String, Set<String>>();

        if (speciesName == null) {
            speciesName = "";
        }

        // iterate through datasets
        int signal = 0;
        boolean searchComments = false;
        String[] ds = datasets.split(",");
	StringBuffer tableNames = new StringBuffer();
        for (String dataset : ds) {
            dataset = dataset.trim();
	    if (dataset.equals("comments")) {
		searchComments = true;
	    } else {
		tableNames.append(tableNames == null ? "'" + dataset + "'" : tableNames +  ", '" + dataset + "'");
	    }
	}

	if (searchComments) {
	    commentMatches = commentSearch(speciesName, projectId, textExpression);
	}

	if (tableNames != null) {
	    tableMatches = tableSearch(componentInstance, speciesName, projectId, textExpression);
	}

/*        Map<String, Set<String>> matches = joinMatches(commentMatches, tableMatches);*/
        Map<String, Set<String>> matches = new HashMap<String, Set<String>>();

        // construct results
        String[][] result = prepareResult(matches, orderedColumns);
        WsfResult wsfResult = new WsfResult();
        wsfResult.setResult(result);
        wsfResult.setSignal(signal);
        return wsfResult;
    }

    private HashMap<String, Set<String>> commentSearch(String speciesName, String projectId, String textExpression) {

        HashMap<String, Set<String>> matches = new HashMap<String, Set<String>>();

	try {
	    if (commentDbConnection == null) {
		DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
		commentDbConnection = DriverManager.getConnection(commentInstance, "apidb", commentPassword);
	    }

	    String sql = 
		"select stable_id as source_id, max(rank) as max_rank, " +
		" apidb.tab_to_string(cast(collect(table_name) as apidb.varchartab), ', ') as fields_matched, " +
		" max(comment_id) keep (dense_rank first order by rank desc) as best_comment_id " +
		" from (select SCORE(1) as rank, stable_id, comment_id, " +
		" cast('community comments' as varchar2(20)) as table_name " +
		" from iodice.comments " +
		" where CONTAINS(content, '(' || " +
		"                                   REPLACE('" + textExpression + "', ' ', ' NEAR ')  || " +
		"                                   ') * 1 OR (' || " +
		"                                   REPLACE('" + textExpression + "', ' ', ' ACCUM ') || " +
		"                                   ') * .1', " +
		"                                   1) > 0 " +
		"         AND project_name = '" + projectId + "' " +
		"         ORDER BY stable_id, rank DESC " +
		"         ) " +
		"  GROUP BY stable_id " +
		"  ORDER BY max_rank desc";

	    ResultSet rs = commentDbConnection.createStatement().executeQuery(sql);
	    while (rs.next()) {
		String sourceId = rs.getString("source_id");
		String maxRank = rs.getString("max_rank");
		String fieldsMatched = rs.getString("fields_matched");
		String bestCommentId = rs.getString("best_comment_id");

		if (!matches.containsKey(sourceId)) {
		    matches.put(sourceId, new HashSet<String>());
		}
		matches.get(sourceId).add(maxRank);
	    }
	    rs.close();
        } catch (SQLException e) {
            logger.info("caught SQLException " + e.getMessage());
	}

	return matches;
    }

    private HashMap<String, Set<String>> tableSearch(String componentInstance, String speciesName, String projectId, String textExpression) {
        HashMap<String, Set<String>> matches = new HashMap<String, Set<String>>();

	return matches;
    }

    private HashMap<String, Set<String>> joinMatches(HashMap<String, Set<String>> commentMatches, HashMap<String, Set<String>> tableMatches){

	return commentMatches;
    }

    private String[][] prepareResult(Map<String, Set<String>> matches,
            String[] cols) {
        String[][] result = new String[matches.size()][cols.length];
        // create an column order map
        Map<String, Integer> orders = new HashMap<String, Integer>();
        for (int i = 0; i < cols.length; i++)
            orders.put(cols[i], i);

        ArrayList<String> sortedIds = new ArrayList<String>(matches.keySet());
        Collections.sort(sortedIds);

        for (int i = 0; i < sortedIds.size(); i++) {
            String id = sortedIds.get(i);
            result[i][orders.get(COLUMN_GENE_ID)] = id;
            String fields = matches.get(id).toString();
            result[i][orders.get(COLUMN_DATASETS)] = fields.substring(1,
                    fields.length() - 1);
            result[i][orders.get(COLUMN_PROJECT_ID)] = this.projectId;
        }
        return result;
    }

}
