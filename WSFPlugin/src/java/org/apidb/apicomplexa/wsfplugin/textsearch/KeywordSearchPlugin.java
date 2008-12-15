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
import java.util.Iterator;
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
    public static final String PARAM_WDK_RECORD_TYPE = "wdk_record_type";

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
        return new String[] { PARAM_TEXT_EXPRESSION, PARAM_DATASETS, PARAM_WDK_RECORD_TYPE };
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
        String recordType = params.get(PARAM_WDK_RECORD_TYPE);
        String textExpression = params.get(PARAM_TEXT_EXPRESSION);
        String componentInstance = params.get(PARAM_COMPONENT_INSTANCE);

        Map<String, SearchResult> commentMatches = new HashMap<String, SearchResult>();
        Map<String, SearchResult> componentMatches = new HashMap<String, SearchResult>();

        if (speciesName == null) {
            speciesName = "";
        }

        // iterate through datasets
        int signal = 0;
        boolean searchComments = false;
        boolean searchComponent = false;
        String[] ds = datasets.split(",");
	StringBuffer tableNames = new StringBuffer();
        for (String dataset : ds) {
            dataset = dataset.trim();
	    if (dataset.equals("comments")) {
		searchComments = true;
	    } else {
		searchComponent = true;
		tableNames.append(tableNames == null ? "'" + dataset + "'" : tableNames +  ", '" + dataset + "'");
	    }
	}

	if (searchComments) {
	    commentMatches = commentSearch(speciesName, projectId, textExpression);
	}

	if (searchComponent) {
	    componentMatches = componentSearch(componentInstance, speciesName, projectId, textExpression);
	}

/*        Map<String, SearchResult> matches = joinMatches(commentMatches, componentMatches);*/
        Map<String, SearchResult> matches = new HashMap<String, SearchResult>();

        // construct results
        String[][] result = prepareResult(matches, orderedColumns);
        WsfResult wsfResult = new WsfResult();
        wsfResult.setResult(result);
        wsfResult.setSignal(signal);
        return wsfResult;
    }

    private HashMap<String, SearchResult> commentSearch(String speciesName, String projectId, String textExpression) throws WsfServiceException {

        HashMap<String, SearchResult> matches = new HashMap<String, SearchResult>();

	try {
	    if (commentDbConnection == null) {
		DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
		commentDbConnection = DriverManager.getConnection(commentInstance, "apidb", commentPassword);
	    }

	    String sql =
		"SELECT source_id,\n" +
		"           max_score,\n" +
		"           fields_matched,\n" +
		"           CTX_DOC.SNIPPET('comments_text_ix', best_rowid,\n" +
		"                           '(' || REPLACE('calcium binding', ' ', ' NEAR ')  ||  ') * 1 OR ('\n" +
		"                           || REPLACE('calcium binding', ' ', ' ACCUM ') || ') * .1'\n" +
		"                                      ) as snippet\n" +
		"FROM (SELECT source_id, MAX(scoring) as max_score,\n" +
		"             'community comments' as fields_matched,\n" +
		"             max(oracle_rowid) keep (dense_rank first order by scoring desc) as best_rowid\n" +
		"      FROM (SELECT SCORE(1)* 1 -- weight\n" +
		"                     as scoring,\n" +
		"                   stable_id as source_id, rowid as oracle_rowid\n" +
		"            FROM iodice.comments\n" +
		"            WHERE CONTAINS(content,\n" +
		"                           '(' || REPLACE('calcium binding', ' ', ' NEAR ')  ||\n" +
		"                           ') * 1 OR (' || REPLACE('calcium binding', ' ', ' ACCUM ') ||\n" +
		"                           ') * .1', 1) > 0\n" +
		"           )\n" +
		"      GROUP BY source_id\n" +
		"      ORDER BY max_score desc\n" +
		"     )";

	    ResultSet rs = commentDbConnection.createStatement().executeQuery(sql);
	    while (rs.next()) {
		String sourceId = rs.getString("source_id") ;

		if (matches.containsKey(sourceId)) {
		    throw new WsfServiceException("duplicate sourceId " + sourceId);
		} else {
		    SearchResult match =
			new SearchResult(sourceId, rs.getFloat("max_score"), rs.getString("fields_matched"), rs.getString("snippet"));
		    matches.put(sourceId, match);
		}
	    }
	    rs.close();
        } catch (SQLException e) {
            logger.info("caught SQLException " + e.getMessage());
	}

	return matches;
    }

    private HashMap<String, SearchResult> componentSearch(String componentInstance, String speciesName, String projectId, String textExpression)
	throws WsfServiceException {
        HashMap<String, SearchResult> matches = new HashMap<String, SearchResult>();
	try {
	    if (commentDbConnection == null) {
		DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
		commentDbConnection = DriverManager.getConnection(commentInstance, "apidb", commentPassword);
	    }

	    String sql =
                "SELECT source_id,\n" +
                "       max_score,\n" +
                "       fields_matched,\n" +
                "       CTX_DOC.SNIPPET(index_name, best_rowid,\n" +
                "                       '(' || REPLACE('calcium binding', ' ', ' NEAR ')  ||  ') * 1 OR ('|| \n" +
                "                       REPLACE('calcium binding', ' ', ' ACCUM ') || ') * .1'\n" +             
                "                      ) as snippet\n" +
                "FROM (SELECT source_id, MAX(scoring) as max_score,\n" +
                "             apidb.tab_to_string(CAST(COLLECT(DISTINCT table_name) AS apidb.varchartab), ', ')  fields_matched,\n" +
                "             max(oracle_rowid) keep (dense_rank first order by scoring desc) as best_rowid,\n" +
                "             max(index_name) keep (dense_rank first order by scoring desc) as index_name\n" +
                "      FROM (  SELECT SCORE(1)* 1 -- weight\n" +
                "                       as scoring,\n" +
                "                    'blastp_text_ix' as index_name, source_id,\n" +
                "                    external_database_name as table_name, rowid as oracle_rowid\n" +
                "              FROM apidb.Blastp\n" +
                "              WHERE CONTAINS(description,\n" +
                "                             '(' || REPLACE('calcium binding', ' ', ' NEAR ')  ||\n" +
                "                             ') * 1 OR (' || REPLACE('calcium binding', ' ', ' ACCUM ') ||\n" +
                "                             ') * .1', 1) > 0\n" +
                "                AND 'Blastp' = 'not Blastp' -- is blastp one of the datasets to search?\n" +
                "                AND 'recordType = genes' = 'recordType = genes'\n" +
                "            UNION\n" +
                "              SELECT SCORE(1)* 1 -- weight\n" +
                "                       as scoring,\n" +
                "                     'gene_text_ix' as index_name, source_id, table_name,\n" +
                "                     rowid as oracle_rowid\n" +
                "              FROM apidb.GeneTable\n" +
                "              WHERE CONTAINS(content,\n" +
                "                             '(' || REPLACE('calcium binding', ' ', ' NEAR ') ||\n" +
                "                             ') * 1 OR (' || REPLACE('calcium binding', ' ', ' ACCUM ') ||\n" +
                "                             ') * .1', 1) > 0\n" +
                "                AND table_name in ('Notes', 'MetabolicPathways', 'Orthologs', 'GoTerms')\n" +
                "                AND 'recordType = genes' = 'recordType = genes'\n" +
                "            UNION\n" +
                "              SELECT SCORE(1)* 1 -- weight\n" +
                "                       as scoring,\n" +
                "                    'isolate_text_ix' as index_name, source_id, table_name, rowid as oracle_rowid\n" +
                "              FROM apidb.WdkIsolateTable\n" +
                "              WHERE CONTAINS(content,\n" +
                "                             '(' || REPLACE('calcium binding', ' ', ' NEAR ')  ||\n" +
                "                             ') * 1 OR (' || REPLACE('calcium binding', ' ', ' ACCUM ') ||\n" +
                "                             ') * .1', 1) > 0\n" +
                "                AND 'isolate search lacks table granularity' =  'isolate search lacks table granularity'\n" +
                "                AND 'recordType = isolates' = 'recordType = isolates'\n" +
                "           )\n" +
                "      GROUP BY source_id\n" +
                "      ORDER BY max_score desc, source_id\n" +
                "     )";

	    ResultSet rs = commentDbConnection.createStatement().executeQuery(sql);
	    while (rs.next()) {
		String sourceId = rs.getString("source_id") ;

		if (matches.containsKey(sourceId)) {
		    throw new WsfServiceException("duplicate sourceId " + sourceId);
		} else {
		    SearchResult match =
			new SearchResult(sourceId, rs.getFloat("max_score"), rs.getString("fields_matched"), rs.getString("snippet"));
		    matches.put(sourceId, match);
		}
	    }
	    rs.close();
        } catch (SQLException e) {
            logger.info("caught SQLException " + e.getMessage());
	}

	return matches;
    }

    private HashMap<String, SearchResult> joinMatches(HashMap<String, SearchResult> commentMatches, HashMap<String, SearchResult> componentMatches){

	Iterator commentIterator = commentMatches.keySet().iterator();
	while (commentIterator.hasNext()) {
	    String sourceId = (String) commentIterator.next();
	    SearchResult commentMatch = commentMatches.get(sourceId);
	    SearchResult componentMatch = componentMatches.get(sourceId);

	    if (componentMatch == null) {
		componentMatches.put(sourceId, commentMatch);
	    } else {
		componentMatch.combine(commentMatch);
	    }
	}

	return componentMatches;
    }

    private String[][] prepareResult(Map<String, SearchResult> matches,
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
