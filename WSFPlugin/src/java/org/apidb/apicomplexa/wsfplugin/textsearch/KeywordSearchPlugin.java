/**
 * KeywordSearchPlugin -- text search using Oracle Text
 */
package org.apidb.apicomplexa.wsfplugin.textsearch;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apidb.apicommon.controller.CommentActionUtility;
import org.apidb.apicommon.model.CommentFactory;
import org.gusdb.wdk.model.dbms.DBPlatform;
import org.gusdb.wdk.model.dbms.SqlUtils;
import org.gusdb.wdk.model.jspwrap.WdkModelBean;
import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfResult;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author John I
 * @created Nov 16, 2008
 */
public class KeywordSearchPlugin extends WsfPlugin {

    // private static final String PROPERTY_FILE = "keywordsearch-config.xml";
    // private static final String PROPERTY_FILE =
    // "profileSimilarity-config.xml";

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
    // private static final String FIELD_COMMENT_INSTANCE = "commentInstance";
    // private static final String FIELD_COMMENT_PASSWORD = "commentPassword";

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
        return new String[] { COLUMN_GENE_ID, COLUMN_PROJECT_ID,
                COLUMN_DATASETS, COLUMN_MAX_SCORE };
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
            recordType = params.get(PARAM_WDK_RECORD_TYPE).trim().replaceAll(
                    "'", "");
        }
        String fields = params.get(PARAM_DATASETS).trim().replaceAll("'", "");
        logger.debug("fields = \"" + fields + "\"");
        String textExpression = params.get(PARAM_TEXT_EXPRESSION).trim().replaceAll(
                "'", "").replaceAll("[-&|~,=;%_]", "\\\\$0").replaceAll("\\*",
                "%");

        String organisms = params.get(PARAM_ORGANISMS);
        logger.debug("organisms = \"" + organisms + "\"");
        String maxPvalue = params.get(PARAM_MAX_PVALUE);

        Map<String, SearchResult> commentMatches = new HashMap<String, SearchResult>();
        Map<String, SearchResult> componentMatches = new HashMap<String, SearchResult>();

        boolean searchComments = false;
        boolean searchComponent = false;
        boolean communityAnnotationRecords = false;
        boolean commentRecords = false;

        StringBuffer quotedFields = new StringBuffer("");
        boolean notFirst = false;
        String[] ds = fields.split(",\\s*");
        for (String field : ds) {
	    if (notFirst) quotedFields.append(",");
            quotedFields.append("'" + field + "'");
            notFirst = true;

            if (field.equals("Comments")) {
                searchComments = true;
		commentRecords = true;
            } else if (field.equals("CommunityAnnotation")) {
                searchComments = true;
		communityAnnotationRecords = true;
            } else {
                searchComponent = true;
            }
        }

        String oracleTextExpression = transformQueryString(textExpression);

        if (searchComments) {
            PreparedStatement ps = null;
            try {
                ps = getCommentQuery(getCommentDbConnection(), recordType,
                        oracleTextExpression, commentRecords, communityAnnotationRecords);
                commentMatches = textSearch(ps);
                commentMatches = validateRecords(commentMatches, organisms);
                logger.debug("after validation commentMatches = "
                        + commentMatches.toString());
            } catch (SQLException ex) {
                throw new WsfServiceException(ex);
            } finally {
                SqlUtils.closeStatement(ps);
            }
        }

        if (searchComponent) {
            PreparedStatement ps = null;
            try {
                ps = getComponentQuery(getComponentDbConnection(), recordType,
                        organisms, oracleTextExpression, quotedFields.toString(), maxPvalue);
                componentMatches = textSearch(ps);
            } catch (SQLException ex) {
                throw new WsfServiceException(ex);
            } finally {
                SqlUtils.closeStatement(ps);
            }
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

        // get the query, which is a sequence of words. we must transform this
        // into an Oracle Text expression suitable for passing to CONTAINS() or
        // SNIPPET()
        // e.g. "calcium binding" becomes
        // "(calcium NEAR binding) * 1 OR (calcium ACCUM binding) * .1"

        double nearWeight = 1;
        double accumWeight = .1;
        String transformed;

        String trimmed = queryExpression.trim();
        ArrayList<String> tokenized = tokenizer(trimmed);
        if (tokenized.size() > 1) {
            transformed = "(" + join(tokenized, " NEAR ") + ") * " + nearWeight
                    + " OR (" + join(tokenized, " ACCUM ") + ") * "
                    + accumWeight;
        } else {
            transformed = trimmed;
        }

        return transformed;
    }

    private static ArrayList<String> tokenizer(String input) {

        ArrayList<String> tokenized = new ArrayList<String>();

        boolean insideQuotes = false;
        for (String quoteChunk : input.split("\"")) {
            if (insideQuotes && quoteChunk.length() > 0) {
                tokenized.add(quoteChunk);
            } else {
                for (String spaceChunk : quoteChunk.split(" ")) {
                    if (spaceChunk.length() > 0) {
                        tokenized.add(spaceChunk);
                    }
                }
            }
            insideQuotes = !insideQuotes;
        }

        return tokenized;
    }

    private static String join(ArrayList<String> parts, String delimiter) {
        boolean notFirstChunk = false;

        StringBuffer conjunction = new StringBuffer("");

        for (String part : parts) {
            if (notFirstChunk) {
                conjunction.append(delimiter);
            }

            conjunction.append(part);

            notFirstChunk = true;
        }

        return conjunction.toString();
    }

    private PreparedStatement getCommentQuery(Connection dbConnection,
            String recordType, String oracleTextExpression,
            boolean commentRecords, boolean communityAnnotationRecords) {

	String recordTypePredicate = new String("");

	if (commentRecords && !communityAnnotationRecords) {
	    recordTypePredicate = new String(" and review_status_id != 'community' ");
	} else if (!commentRecords && communityAnnotationRecords) {
	    recordTypePredicate = new String(" and review_status_id = 'community' ");
	}


        String sql = new String(
                "SELECT source_id, project_id, \n"
                        + "           max_score as max_score, -- should be weighted using component TableWeight \n"
                        + "       fields_matched \n"
                        + "FROM (SELECT source_id, project_id, MAX(scoring) as max_score, \n"
                        + "             apidb.tab_to_string(set(CAST(COLLECT(table_name) AS apidb.varchartab)), ', ') as fields_matched, "
                        + "             max(oracle_rowid) keep (dense_rank first order by scoring desc) as best_rowid \n"
                        + "      FROM (SELECT SCORE(1) \n"
                        + "                     as scoring, \n"
                        + "             DECODE(c.review_status_id, 'community', 'community annotation', 'user comments') as table_name, \n"
                        + "                   tsc.source_id, tsc.project_id, tsc.rowid as oracle_rowid \n"
                        + "            FROM apidb.TextSearchableComment tsc, comments2.Comments c \n"
                        + "            WHERE CONTAINS(tsc.content, ?, 1) > 0  \n"
                        + "              AND tsc.comment_id = c.comment_id\n"
                        + "              AND c.is_visible = 1\n"
                        + recordTypePredicate
                        + "              AND project_id = '" + projectId
                        + "') \n" + "      GROUP BY source_id, project_id \n"
                        + "      ORDER BY max_score desc \n" + "     )");

        logger.debug("comment SQL: " + sql);
        logger.debug("oracleTextExpression = \"" + oracleTextExpression + "\"");

        PreparedStatement ps = null;
        ;
        try {
            ps = dbConnection.prepareStatement(sql);
            ps.setString(1, oracleTextExpression);
        } catch (SQLException e) {
            logger.info("caught SQLException " + e.getMessage());
        }

        return ps;
    }

    private PreparedStatement getComponentQuery(Connection dbConnection,
            String recordType, String organisms, String oracleTextExpression,
            String fields, String maxPvalue) {

        if (maxPvalue == null || maxPvalue.equals("")) {
            maxPvalue = "0";
        }

        String sql = new String(
                "SELECT source_id, project_id, \n"
                        + "       max_score, \n"
                        + "       fields_matched \n"
                        + "FROM (SELECT source_id, project_id, MAX(scoring) as max_score, \n"
                        + "             apidb.tab_to_string(set(CAST(COLLECT(table_name) AS apidb.varchartab)), ', ')  fields_matched, \n"
                        + "             max(index_name) keep (dense_rank first order by scoring desc, source_id, table_name) as index_name, \n"
                        + "             max(oracle_rowid) keep (dense_rank first order by scoring desc, source_id, table_name) as oracle_rowid \n"
                        + "      FROM (  SELECT SCORE(1) * (select nvl(max(weight), 1) from apidb.TableWeight where table_name = 'Blastp') \n"
                        + "                       as scoring, \n"
                        + "                    'apidb.blastp_text_ix' as index_name, b.rowid as oracle_rowid, b.source_id, b.project_id, \n"
                        + "                    external_database_name as table_name \n"
                        + "              FROM apidb.Blastp b \n"
                        + "              WHERE CONTAINS(b.description, ?, 1) > 0 \n"
                        + "                AND 'Blastp' in (" + fields + ") \n"
                        + "                AND '" + recordType + "' = 'gene' \n"
                        + "                AND b.pvalue_exp < ? \n"
                        + "                AND b.query_organism in (" + organisms + ") \n"
                        + "            UNION \n"
                        + "              SELECT SCORE(1)* nvl(tw.weight, 1) \n"
                        + "                       as scoring, \n"
                        + "                     'apidb.gene_text_ix' as index_name, gt.rowid as oracle_rowid, gt.source_id, gt.project_id, gt.field_name as table_name\n"
                        + "              FROM apidb.GeneDetail gt, apidb.TableWeight tw, apidb.GeneAttributes ga \n"
                        + "              WHERE CONTAINS(content, ?, 1) > 0\n"
                        + "                AND gt.field_name in (" + fields + ") \n"
                        + "                AND '" + recordType + "' = 'gene' \n"
                        + "                AND gt.field_name = tw.table_name(+) \n"
                        + "                AND gt.source_id = ga.source_id \n"
                        + "                AND ga.species in (" + organisms + ") \n"
                        + "            UNION \n"
                        + "              SELECT SCORE(1) * nvl(tw.weight, 1)  \n"
                        + "                       as scoring, \n"
                        + "                    'apidb.isolate_text_ix' as index_name, wit.rowid as oracle_rowid, wit.source_id, wit.project_id, wit.field_name as table_name \n"
                        + "              FROM apidb.IsolateDetail wit, apidb.TableWeight tw \n"
                        + "              WHERE CONTAINS(content, ?, 1) > 0 \n"
                        + "                AND wit.field_name in (" + fields + ") \n"
                        + "                AND '" + recordType + "' = 'isolate' \n"
                        + "                AND wit.field_name = tw.table_name(+) \n"
                        + "           ) \n"
                        + "      GROUP BY source_id, project_id \n"
                        + "      ORDER BY max_score desc, source_id \n"
                        + "     )");
        logger.debug("component SQL: " + sql);
        logger.debug("organisms = \"" + organisms
                + "\"; oracleTextExpression = \"" + oracleTextExpression + "\"");
        logger.debug("fields = \"" + fields + "\"");
        logger.debug("recordType = \"" + recordType + "\"");
        logger.debug("maxPvalue = \"" + maxPvalue + "\"");

        PreparedStatement ps = null;
        try {
            ps = dbConnection.prepareStatement(sql);
            ps.setString(1, oracleTextExpression);
            ps.setInt(2, Integer.parseInt(maxPvalue));
            ps.setString(3, oracleTextExpression);
            ps.setString(4, oracleTextExpression);
        } catch (SQLException e) {
            logger.info("caught SQLException " + e.getMessage());
        }

        return ps;
    }

    private PreparedStatement getValidationQuery() {
        String sql = new String("select attrs.source_id, attrs.project_id \n"
				+ "from apidb.GeneAlias alias, apidb.GeneAttributes attrs \n"
				+ "where alias.alias = ? \n"
				+ "  and alias.gene = attrs.source_id \n"
				+ "  and attrs.project_id = ? \n"
				+ "  and ? like '%' || attrs.species || '%'");

        WdkModelBean wdkModel = (WdkModelBean) servletContext.getAttribute("wdkModel");
        DBPlatform platform = wdkModel.getModel().getQueryPlatform();
        DataSource dataSource = platform.getDataSource();

        try {
            return SqlUtils.getPreparedStatement(dataSource, sql);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private Map<String, SearchResult> textSearch(PreparedStatement query)
            throws WsfServiceException, SQLException {
        Map<String, SearchResult> matches = new HashMap<String, SearchResult>();
        ResultSet rs = null;
        try {
	    logger.info("about to execute (one or the other) text-search query");
            query.setFetchSize(5000);
            rs = query.executeQuery();
	    logger.info("finshed execute");
            while (rs.next()) {
                String sourceId = rs.getString("source_id");

                if (matches.containsKey(sourceId)) {
                    throw new WsfServiceException("duplicate sourceId "
                            + sourceId);
                } else {
                    SearchResult match = new SearchResult(
                            rs.getString("project_id"), sourceId,
                            rs.getFloat("max_score"),
                            rs.getString("fields_matched"));
                    matches.put(sourceId, match);
                }
            }
	    logger.info("finished fetching rows");
        } catch (SQLException ex) {
            logger.info("caught SQLException " + ex.getMessage());
            ex.printStackTrace();
        } finally {
            if (rs != null) rs.close();
        }

        return matches;
    }

    private Map<String, SearchResult> validateRecords(
            Map<String, SearchResult> commentMatches, String organisms) {

        Map<String, SearchResult> newCommentMatches = new HashMap<String, SearchResult>();
        newCommentMatches.putAll(commentMatches);

        logger.debug("organisms = \"" + organisms + "\"");

        PreparedStatement validationQuery = null;
        try {
            validationQuery = getValidationQuery();
            for (String sourceId : commentMatches.keySet()) {
                logger.debug("validating sourceId \"" + sourceId + "\"");
                ResultSet rs = null;
                try {
                    validationQuery.setString(1, sourceId);
                    validationQuery.setString(2, projectId);
                    validationQuery.setString(3, organisms);
                    rs = validationQuery.executeQuery();
                    if (!rs.next()) {
                        // no match; drop result
                        logger.info("dropping unrecognized ID \"" + sourceId
                                + "\" from comment-search result set.");
                        newCommentMatches.remove(sourceId);
                    } else {
                        String returnedSourceId = rs.getString("source_id");
                        logger.debug("validation query returned \""
                                + returnedSourceId + "\"");
                        if (!returnedSourceId.equals(sourceId)) {
                            // ID changed; substitute returned value
                            logger.info("Substituting valid ID \""
                                    + returnedSourceId
                                    + "\" for ID \""
                                    + sourceId
                                    + "\" returned from comment-search result set.");
                            SearchResult result = newCommentMatches.get(sourceId);
                            result.setSourceId(returnedSourceId);
                            newCommentMatches.remove(sourceId);
                            newCommentMatches.put(returnedSourceId, result);
                        }
                    }

                } catch (SQLException ex) {
                    logger.info("caught SQLException " + ex.getMessage());
                } finally {
                    try {
                        rs.close();
                    } catch (SQLException ex) {
                        logger.info("caught SQLException " + ex.getMessage());
                    }
                }
            }
        } finally {
            SqlUtils.closeStatement(validationQuery);
        }
        // Map<String, SearchResult> otherCommentMatches = new HashMap<String,
        // SearchResult>();
        return newCommentMatches;
    }

    private SearchResult[] joinMatches(
            Map<String, SearchResult> commentMatches,
            Map<String, SearchResult> componentMatches) {

        for (String sourceId : commentMatches.keySet()) {
            SearchResult commentMatch = commentMatches.get(sourceId);
            SearchResult componentMatch = componentMatches.get(sourceId);

            if (componentMatch == null) {
                componentMatches.put(sourceId, commentMatch);
            } else {
                componentMatch.combine(commentMatch);
            }
        }

        // componentMatches now has all results combined; get it into a sorted
        // array
        Collection<SearchResult> matchCollection = componentMatches.values();
        SearchResult[] matchArray = matchCollection.toArray(new SearchResult[0]);
        Arrays.sort(matchArray);

        return matchArray;

    }

    private String[][] flattenMatches(SearchResult[] matches,
            String[] orderedColumns) throws WsfServiceException {

        // validate that WDK expects the columns in the order we want
        String[] expectedColumns = { "RecordID", "ProjectId", "MaxScore",
                "Datasets" };

        int i = 0;
        for (String expected : expectedColumns) {
            if (!expected.equals(orderedColumns[i])) {
                throw new WsfServiceException(
                        "misordered WSF column: expected \"" + expected
                                + "\", got \"" + orderedColumns[i]);
            }
            i++;
        }

        String[][] flat = new String[matches.length][];

        i = 0;
        for (SearchResult match : matches) {
            String[] a = { match.getSourceId(), match.getProjectId(),
                    Float.toString(match.getMaxScore()),
                    match.getFieldsMatched() };
            flat[i++] = a;
        }

        return flat;
    }

    private Connection getCommentDbConnection() throws SQLException {
        CommentFactory factory = CommentActionUtility.getCommentFactory(servletContext);
        DBPlatform platform = factory.getCommentPlatform();
        return platform.getDataSource().getConnection();
    }

    private Connection getComponentDbConnection() throws SQLException {
        WdkModelBean wdkModel = (WdkModelBean) servletContext.getAttribute("wdkModel");
        DBPlatform platform = wdkModel.getModel().getQueryPlatform();
        return platform.getDataSource().getConnection();
    }
}
