/**
 * KeywordSearchPlugin -- text search using Oracle Text
 */
package org.apidb.apicomplexa.wsfplugin.textsearch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
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

    //    private static final String PROPERTY_FILE = "keywordsearch-config.xml";
    private static final String PROPERTY_FILE = "profileSimilarity-config.xml";

    // required parameter definition
    public static final String PARAM_TEXT_EXPRESSION = "text_expression";
    public static final String PARAM_DATASETS = "text_fields";
    public static final String PARAM_ORGANISMS = "text_search_organism";
    public static final String PARAM_COMPONENT_INSTANCE = "component_instance";
    public static final String PARAM_WDK_RECORD_TYPE = "wdk_record_type";
    public static final String PARAM_MAX_PVALUE = "max_pvalue";

    public static final String COLUMN_GENE_ID = "RecordID";
    public static final String COLUMN_PROJECT_ID = "ProjectId";
    public static final String COLUMN_DATASETS = "Datasets";
    public static final String COLUMN_MAX_SCORE = "MaxScore";

    // field definition
    private static final String FIELD_COMMENT_INSTANCE = "commentInstance";
    private static final String FIELD_COMMENT_PASSWORD = "commentPassword";

    private Connection commentDbConnection;
    private Connection componentDbConnection;
    private PreparedStatement validationQuery = null;
    private String projectId;

    /**
     * @throws WsfServiceException
     * 
     */
    public KeywordSearchPlugin() throws WsfServiceException {
	super();

        projectId = servletContext.getInitParameter("model");
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
        return new String[] { COLUMN_GENE_ID, COLUMN_PROJECT_ID, COLUMN_DATASETS, COLUMN_MAX_SCORE };
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
	String recordType;
        if (params.get(PARAM_WDK_RECORD_TYPE) == null) {
	    recordType = "gene";
	} else {
	    recordType = params.get(PARAM_WDK_RECORD_TYPE).trim().replaceAll("'", "");
	}
        String fields = params.get(PARAM_DATASETS).trim().replaceAll("'", "");
	logger.debug("fields = \"" + fields + "\"");
        String textExpression = params.get(PARAM_TEXT_EXPRESSION).trim().replaceAll("'", "")
	    .replaceAll("[-&|~,=;%]", "\\\\$0").replaceAll("\\*", "%");
	//        String x = params.get(X);
        String organisms = params.get(PARAM_ORGANISMS);
	logger.debug("organisms = \"" + organisms + "\"");
        String maxPvalue = params.get(PARAM_MAX_PVALUE);

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

        if (searchComments) {
            commentMatches
		= textSearch(getCommentQuery(getCommentDbConnection(),
					     recordType, organisms, oracleTextExpression));
	    commentMatches = validateRecords(commentMatches);
	    logger.debug("after validation commentMatches = " + commentMatches.toString());
        }

        if (searchComponent) {
            componentMatches
		= textSearch(getComponentQuery(getComponentDbConnection(),
					       recordType, organisms, oracleTextExpression, fields, maxPvalue));
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

    private PreparedStatement getCommentQuery(Connection dbConnection, String recordType, String organisms, String oracleTextExpression) {

	
	String sql = new String("SELECT source_id, project_id, \n" +
                "           max_score as max_score, -- should be weighted using component TableWeight \n" +
                "       fields_matched \n" +
                "FROM (SELECT source_id, project_id, MAX(scoring) as max_score, \n" +
                "             'community comments' as fields_matched, \n" +
                "             max(oracle_rowid) keep (dense_rank first order by scoring desc) as best_rowid \n" +
                "      FROM (SELECT SCORE(1) \n" +
                "                     as scoring, \n" +
                "                   tsc.source_id, tsc.project_id, tsc.rowid as oracle_rowid \n" +
                "            FROM apidb.TextSearchableComment tsc \n" +
                "            WHERE ? like '%' || tsc.organism || '%' \n" +
                "              AND CONTAINS(tsc.content, ?, 1) > 0  \n" +
                "              AND project_id = '" + projectId + "') \n" +
                "      GROUP BY source_id, project_id \n" +
                "      ORDER BY max_score desc \n" +
                "     )");

	logger.debug("comment SQL: " + sql);
	logger.debug("organisms = \"" + organisms + "\"; oracleTextExpression = \"" + oracleTextExpression + "\"");

	PreparedStatement ps = null;;
        try {
	    ps = dbConnection.prepareStatement(sql);
	    ps.setString(1, organisms);
	    ps.setString(2, oracleTextExpression);
        } catch (SQLException e) {
            logger.info("caught SQLException " + e.getMessage());
        }

	return ps;
    }

    private PreparedStatement getComponentQuery(Connection dbConnection, String recordType, String organisms, String oracleTextExpression, String fields, String maxPvalue) {

	String pvalueTerm;
	if (maxPvalue == null || maxPvalue.equals("")) {
	    maxPvalue = "0";
	}

	String sql = new String("SELECT source_id, project_id, \n" +
               "       max_score, \n" +
               "       fields_matched \n" +
               "FROM (SELECT source_id, project_id, MAX(scoring) as max_score, \n" +
               "             apidb.tab_to_string(set(CAST(COLLECT(table_name) AS apidb.varchartab)), ', ')  fields_matched, \n" +
               "             max(index_name) keep (dense_rank first order by scoring desc, source_id, table_name) as index_name, \n" +
               "             max(oracle_rowid) keep (dense_rank first order by scoring desc, source_id, table_name) as oracle_rowid \n" +
               "      FROM (  SELECT SCORE(1) * (select nvl(max(weight), 1) from apidb.TableWeight where table_name = 'Blastp') \n" +
               "                       as scoring, \n" +
               "                    'apidb.blastp_text_ix' as index_name, b.rowid as oracle_rowid, b.source_id, b.project_id, \n" +
               "                    external_database_name as table_name \n" +
               "              FROM apidb.Blastp b \n" +
               "              WHERE CONTAINS(b.description, ?, 1) > 0 \n" +
               "                AND ? like '%Blastp%' \n" +
               "                AND ? = 'gene' \n" +
               "                AND b.pvalue_exp < ? \n" +
               "                AND ? like  '%' || b.query_organism  || '%' \n" +
               "            UNION \n" +
               "              SELECT SCORE(1)* nvl(tw.weight, 1) \n" +
               "                       as scoring, \n" +
               "                     'apidb.gene_text_ix' as index_name, gt.rowid as oracle_rowid, gt.source_id, gt.project_id, gt.field_name as table_name\n" +
               "              FROM apidb.GeneDetail gt, apidb.TableWeight tw, apidb.GeneAttributes ga \n" +
               "              WHERE CONTAINS(content, ?, 1) > 0\n" +
               "                AND ? like '%' || gt.field_name || '%' \n" +
               "                AND ? = 'gene' \n" +
               "                AND gt.field_name = tw.table_name(+) \n" +
               "                AND gt.source_id = ga.source_id \n" +
               "                AND ? like '%' || ga.species || '%' \n" +
               "            UNION \n" +
               "              SELECT SCORE(1) * nvl(tw.weight, 1)  \n" +
               "                       as scoring, \n" +
               "                    'apidb.isolate_text_ix' as index_name, wit.rowid as oracle_rowid, wit.source_id, wit.project_id, wit.field_name as table_name \n" +
               "              FROM apidb.IsolateDetail wit, apidb.TableWeight tw \n" +
               "              WHERE CONTAINS(content, ?, 1) > 0 \n" +
               "                AND ? like '%' || wit.field_name || '%' \n" +
               "                AND ? = 'isolate' \n" +
               "                AND wit.field_name = tw.table_name(+) \n" +
               "           ) \n" +
               "      GROUP BY source_id, project_id \n" +
               "      ORDER BY max_score desc, source_id \n" +
               "     )");
	logger.debug("component SQL: " + sql);
	logger.debug("organisms = \"" + organisms + "\"; oracleTextExpression = \"" + oracleTextExpression + "\"");
	logger.debug("fields = \"" + fields + "\"");
	logger.debug("recordType = \"" + recordType + "\"");
	logger.debug("maxPvalue = \"" + maxPvalue + "\"");

	PreparedStatement ps = null;
	try {
    ps = dbConnection.prepareStatement(sql);
	    // Blastp
	    ps.setString(1, oracleTextExpression);
	    ps.setString(2, fields);
	    ps.setString(3, recordType);
	    ps.setInt(4, Integer.parseInt(maxPvalue));
	    ps.setString(5, organisms);
	    // GeneTable
	    ps.setString(6, oracleTextExpression);
	    ps.setString(7, fields);
	    ps.setString(8, recordType);
	    ps.setString(9, organisms);
	    // WdkIsolateTable
	    ps.setString(10, oracleTextExpression);
	    ps.setString(11, fields);
	    ps.setString(12, recordType);
	} catch (SQLException e) {
	    logger.info("caught SQLException " + e.getMessage());
	}

	return ps;
    }

    private PreparedStatement getValidationQuery() {

	if (validationQuery == null) {
	    String sql = new String("select attrs.source_id, attrs.project_id \n" +
				    "from apidb.GeneAlias alias, apidb.GeneAttributes attrs \n" +
				    "where alias.alias = ? \n" +
				    "  and alias.gene = attrs.source_id \n" +
				    "  and attrs.project_id = ?");

	    try {
		Connection dbConnection = getComponentDbConnection();
		validationQuery = dbConnection.prepareStatement(sql);
	    } catch (SQLException e) {
		logger.info("caught SQLException " + e.getMessage());
	    }

	}
	return validationQuery;
    }

    private Map<String, SearchResult> textSearch(PreparedStatement query)
        throws WsfServiceException {
        Map<String, SearchResult> matches = new HashMap<String, SearchResult>();
        try {
            ResultSet rs = query.executeQuery();
            while (rs.next()) {
                String sourceId = rs.getString("source_id") ;

                if (matches.containsKey(sourceId)) {
                    throw new WsfServiceException("duplicate sourceId " + sourceId);
                } else {
                    SearchResult match =
                        new SearchResult(rs.getString("project_id"), sourceId, rs.getFloat("max_score"), rs.getString("fields_matched"));
                    matches.put(sourceId, match);
                }
            }
            rs.close();
        } catch (SQLException e) {
            logger.info("caught SQLException " + e.getMessage());
        }

        return matches;
    }

    private Map<String, SearchResult> validateRecords(Map<String, SearchResult> commentMatches){

        PreparedStatement validationQuery = getValidationQuery();
	Map<String, SearchResult> newCommentMatches = new HashMap<String, SearchResult>();
	newCommentMatches.putAll(commentMatches);

        Iterator commentIterator = commentMatches.keySet().iterator();
        while (commentIterator.hasNext()) {
            String sourceId = (String) commentIterator.next();
	    logger.debug("validating sourceId \"" + sourceId + "\"");
	    try {
		validationQuery.setString(1, sourceId);
		validationQuery.setString(2, projectId);
		ResultSet rs = validationQuery.executeQuery();
		if (!rs.next()) {
		    // no match; drop result
		    logger.info("dropping unrecognized ID \"" + sourceId + "\" from comment-search result set.");
		    newCommentMatches.remove(sourceId);
		} else {
		    String returnedSourceId = rs.getString("source_id");
		    logger.debug("validation query returned \"" + returnedSourceId + "\"");
		    if (!returnedSourceId.equals(sourceId)) {
			// ID changed; substitute returned value
			logger.info("Substituting valid ID \"" + returnedSourceId + "\" for ID \"" + sourceId + "\" returned from comment-search result set.");
			SearchResult result = newCommentMatches.get(sourceId);
			result.setSourceId(returnedSourceId);
			newCommentMatches.remove(sourceId);
			newCommentMatches.put(returnedSourceId, result);
		    }
		}
		rs.close();
	    } catch (SQLException e) {
		logger.info("caught SQLException " + e.getMessage());
		}

	}
	//	Map<String, SearchResult> otherCommentMatches = new HashMap<String, SearchResult>();
	return newCommentMatches;
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
        String[] expectedColumns = {"RecordID", "ProjectId", "MaxScore", "Datasets"};

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
            String[] a = {match.getSourceId(), match.getProjectId(), Float.toString(match.getMaxScore()), match.getFieldsMatched()};
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
