/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.textsearch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Arrays;
import java.util.Iterator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.servlet.ServletContext;

import org.apache.axis.MessageContext;
import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfResult;
import org.gusdb.wsf.plugin.WsfServiceException;
import org.gusdb.wdk.model.ModelConfig;
import org.gusdb.wdk.model.jspwrap.WdkModelBean;

/**
 * @author John I
 * @created Nov 16, 2008
 */
public class KeywordSearchPlugin extends WsfPlugin {

    private static final String PROPERTY_FILE = "keywordsearch-config.xml";

    // required parameter definition
    public static final String PARAM_PROJECT_ID = "project_id";
    public static final String PARAM_TEXT_EXPRESSION = "text_expression";
    public static final String PARAM_DATASETS = "text_search_fields";
    public static final String PARAM_ORGANISMS = "text_search_organism";
    public static final String PARAM_COMPONENT_INSTANCE = "component_instance";
    public static final String PARAM_WDK_RECORD_TYPE = "wdk_record_type";

    public static final String COLUMN_GENE_ID = "GeneID";
    public static final String COLUMN_PROJECT_ID = "ProjectId";
    public static final String COLUMN_DATASETS = "Datasets";
    public static final String COLUMN_SNIPPET = "Snippet";
    public static final String COLUMN_MAX_SCORE = "MaxScore";

    // field definition
    private static final String FIELD_COMMENT_INSTANCE = "commentInstance";
    private static final String FIELD_COMMENT_PASSWORD = "commentPassword";

    private Connection commentDbConnection;
    private Connection componentDbConnection;
    /**
     * @throws WsfServiceException
     * 
     */
    public KeywordSearchPlugin() throws WsfServiceException {
	super();
        // super(PROPERTY_FILE); -- load properties
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getRequiredParameters()
     */
    @Override
    protected String[] getRequiredParameterNames() {
        return new String[] { PARAM_PROJECT_ID, PARAM_TEXT_EXPRESSION, PARAM_DATASETS, PARAM_WDK_RECORD_TYPE };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getColumns()
     */
    @Override
    protected String[] getColumns() {
        return new String[] { COLUMN_GENE_ID, COLUMN_PROJECT_ID, COLUMN_DATASETS, COLUMN_SNIPPET, COLUMN_MAX_SCORE };
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

	int signal = 0;
        // get parameters
        String recordType = params.get(PARAM_WDK_RECORD_TYPE).trim().replaceAll("^'", "").replaceAll("'$", "");
        String fields = params.get(PARAM_DATASETS).trim().replaceAll("^'", "").replaceAll("'$", "");
        String textExpression = params.get(PARAM_TEXT_EXPRESSION).trim().replaceAll("^'", "").replaceAll("'$", "");
	//        String x = params.get(X);
        String organisms = params.get(PARAM_ORGANISMS).trim().replaceAll("^'", "").replaceAll("'$", "");
        String projectId = params.get(PARAM_PROJECT_ID).trim().replaceAll("^'", "").replaceAll("'$", "");

        Map<String, SearchResult> commentMatches = new HashMap<String, SearchResult>();
        Map<String, SearchResult> componentMatches = new HashMap<String, SearchResult>();

        boolean searchComments = false;
        boolean searchComponent = false;

        String[] ds = fields.split(",\\s*");
        for (String field : ds) {
            if (field.equals("Comments")) {
                searchComments = true;
            } else {
                searchComponent = true;
            }
        }

	String oracleTextExpression = transformQueryString(textExpression);

	logger.debug("projectId = \"" + projectId + "\"");
        if (searchComments) {
            commentMatches = textSearch(getCommentDbConnection(),
					getCommentSql(projectId, recordType, organisms, oracleTextExpression));
        }

        if (searchComponent) {
            componentMatches = textSearch(getComponentDbConnection(),
					  getComponentSql(projectId, recordType, organisms, oracleTextExpression, fields));
        }

        SearchResult[] matches = joinMatches(commentMatches, componentMatches);

        // construct results
        String[][] result = flattenMatches(matches, orderedColumns);
        WsfResult wsfResult = new WsfResult();
        wsfResult.setResult(result);
        wsfResult.setSignal(signal);
        return wsfResult;
    }

    private String transformQueryString(String queryExpression) {

	// get the query, which is a sequence of words. we must transform this into an Oracle Text expression suitable for passing to CONTAINS() or SNIPPET()
        // e.g. "calcium binding" becomes "(calcium NEAR binding) * 1 OR (calcium ACCUM binding) * .1"

	double nearWeight = 1;
	double accumWeight = .1;
	String transformed;

	String trimmed = queryExpression.trim();
	if (trimmed.matches(".*\\s+.*")) {
	    String nearString = trimmed.replaceAll("\\s+", " NEAR ");
	    String accumString = trimmed.replaceAll("\\s+", " ACCUM ");
	    transformed = "(" + nearString + ") * " + nearWeight + " OR (" + accumString + ") * " + accumWeight;
	} else {
	    transformed = trimmed;
	}

	return transformed;
    }

    private String getCommentSql(String projectId, String recordType, String organisms, String oracleTextExpression) {

	
	String sql = new String("SELECT source_id, '" + projectId + "' as project_id, \n" +
                "           max_score,\n" +
                "           fields_matched,\n" +
                "           CTX_DOC.SNIPPET('comments_text_ix', best_rowid,\n" +
                "                           '" + oracleTextExpression + "') as snippet\n" +
                "FROM (SELECT source_id, MAX(scoring) as max_score,\n" +
                "             'community comments' as fields_matched,\n" +
                "             max(oracle_rowid) keep (dense_rank first order by scoring desc) as best_rowid\n" +
                "      FROM (SELECT SCORE(1)* 1 -- weight\n" +
                "                     as scoring,\n" +
                "                   source_id, rowid as oracle_rowid\n" +
                "            FROM apidb.TextSearchableComment\n" +
                "            WHERE project_id = '" + projectId + "' \n" +
                "              AND CONTAINS(content,\n" +
                "                           '" + oracleTextExpression + "', 1) > 0 ) \n" +
                "      GROUP BY source_id\n" +
                "      ORDER BY max_score desc\n" +
                "     )");

	logger.debug("comment SQL: " + sql);
	return sql;
    }

    private String getComponentSql(String projectId, String recordType, String organisms, String oracleTextExpression, String fields) {

	String sql = new String("SELECT source_id, '" + projectId + "' as project_id, \n" +
               "       max_score, \n" +
               "       fields_matched, \n" +
               "       CTX_DOC.SNIPPET(index_name, oracle_rowid, \n" +
               "                           '" + oracleTextExpression + "'\n" +
               "                      ) as snippet \n" +
               "FROM (SELECT source_id, MAX(scoring) as max_score, \n" +
               "             apidb.tab_to_string(CAST(COLLECT(DISTINCT table_name) AS apidb.varchartab), ', ')  fields_matched, \n" +
               "             max(index_name) keep (dense_rank first order by scoring desc, source_id, table_name) as index_name, \n" +
               "             max(oracle_rowid) keep (dense_rank first order by scoring desc, source_id, table_name) as oracle_rowid \n" +
               "      FROM (  SELECT SCORE(1)* 1 -- weight \n" +
               "                       as scoring, \n" +
               "                    'blastp_text_ix' as index_name, rowid as oracle_rowid, source_id, \n" +
               "                    external_database_name as table_name \n" +
               "              FROM apidb.Blastp \n" +
               "              WHERE CONTAINS(description, \n" +
               "                           '" + oracleTextExpression + "', 1) > 0 \n" +
               "                AND '" + fields + "' like '%Blastp%' \n" +
               "                AND '" + recordType + "' = 'gene' \n" +
               "            UNION \n" +
               "              SELECT SCORE(1)* 1 -- weight \n" +
               "                       as scoring, \n" +
               "                     'gene_text_ix' as index_name, rowid as oracle_rowid, source_id, table_name \n" +
               "              FROM apidb.GeneTable \n" +
               "              WHERE CONTAINS(content, \n" +
               "                           '" + oracleTextExpression + "', 1) > 0\n" +
               "                AND '" + fields + "' like '%' || table_name || '%' \n" +
               "                AND '" + recordType + "' = 'gene' \n" +
               "            UNION \n" +
               "              SELECT SCORE(1)* 1 -- weight \n" +
               "                       as scoring, \n" +
               "                    'isolate_text_ix' as index_name, rowid as oracle_rowid, source_id, table_name \n" +
               "              FROM apidb.WdkIsolateTable \n" +
               "              WHERE CONTAINS(content, \n" +
               "                           '" + oracleTextExpression + "', 1) > 0 \n" +
               "                AND '" + fields + "' like '%' || table_name || '%' \n" +
               "                AND '" + recordType + "' = 'isolate' \n" +
               "           ) \n" +
               "      GROUP BY source_id \n" +
               "      ORDER BY max_score desc, source_id \n" +
               "     )");

	logger.debug("component SQL: " + sql);
	return sql;
    }

    private Map<String, SearchResult> textSearch(Connection dbConnection, String sql)
        throws WsfServiceException {
        Map<String, SearchResult> matches = new HashMap<String, SearchResult>();
        try {
            if (dbConnection == null) {
		throw new WsfServiceException("null database connection");
            }

            ResultSet rs = dbConnection.createStatement().executeQuery(sql);
            while (rs.next()) {
                String sourceId = rs.getString("source_id") ;

                if (matches.containsKey(sourceId)) {
                    throw new WsfServiceException("duplicate sourceId " + sourceId);
                } else {
                    SearchResult match =
                        new SearchResult(rs.getString("project_id"), sourceId, rs.getFloat("max_score"), rs.getString("fields_matched"), rs.getString("snippet"));
                    matches.put(sourceId, match);
                }
            }
            rs.close();
        } catch (SQLException e) {
            logger.info("caught SQLException " + e.getMessage());
        }

        return matches;
    }

    private SearchResult[] joinMatches(Map<String, SearchResult> commentMatches, Map<String, SearchResult> componentMatches){

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

        // componentMatches now has all results combined; get it into a sorted array
        Collection<SearchResult> matchCollection = componentMatches.values();
        SearchResult[] matchArray = matchCollection.toArray(new SearchResult[0]);
        Arrays.sort(matchArray);

        return matchArray;
        
    }

    private String[][] flattenMatches(SearchResult[] matches, String[] orderedColumns) throws WsfServiceException{

        // validate that WDK expects the columns in the order we want
        String[] expectedColumns = {"GeneID", "ProjectId", "MaxScore", "Datasets", "Snippet"};

        int i = 0;
        for (String expected : expectedColumns) {
            if (!expected.equals(orderedColumns[i])) {
                throw new WsfServiceException("misordered WSF column: expected \"" + expected + "\", got \"" + orderedColumns[i]);
            }
            i++;
        }

        String[][] flat = new String[matches.length][];

        i = 0;
        for (SearchResult match : matches) {
            String[] a = {match.getSourceId(), match.getProjectId(), Float.toString(match.getMaxScore()), match.getFieldsMatched(), match.getSnippet()};
            flat[i++] = a;
        }

        return flat;
    }

    Connection getCommentDbConnection() {

	WdkModelBean wdkModel = (WdkModelBean)servletContext.getAttribute("wdkModel");
	ModelConfig modelConfig = wdkModel.getModel().getModelConfig();

	if (commentDbConnection == null) {
	    try {
		DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
		commentDbConnection = DriverManager.getConnection(modelConfig.getUserDB().getConnectionUrl(),
								  modelConfig.getUserDB().getLogin(),
								  modelConfig.getUserDB().getPassword());
	    } catch (SQLException e) {
		logger.info("caught SQLException " + e.getMessage());
	    }
	}
	return commentDbConnection;
    }

    Connection getComponentDbConnection() {

	WdkModelBean wdkModel = (WdkModelBean)servletContext.getAttribute("wdkModel");
	ModelConfig modelConfig = wdkModel.getModel().getModelConfig();
	
	if (componentDbConnection == null) {
	    try {
		DriverManager.registerDriver (new oracle.jdbc.driver.OracleDriver());
		componentDbConnection = DriverManager.getConnection(modelConfig.getApplicationDB().getConnectionUrl(),
								  modelConfig.getApplicationDB().getLogin(),
								  modelConfig.getApplicationDB().getPassword());
	    } catch (SQLException e) {
		logger.info("caught SQLException " + e.getMessage());
	    }
	}
	return componentDbConnection;
    }
}
