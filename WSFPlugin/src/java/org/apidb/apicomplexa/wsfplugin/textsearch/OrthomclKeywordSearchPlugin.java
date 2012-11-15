/**
 * OrthomclKeywordSearchPlugin -- text search using Oracle Text
 */
package org.apidb.apicomplexa.wsfplugin.textsearch;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.gusdb.wdk.model.dbms.SqlUtils;
import org.gusdb.wsf.plugin.WsfRequest;
import org.gusdb.wsf.plugin.WsfResponse;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author Steve and John I
 * @created Nov 2012
 */
public class OrthomclKeywordSearchPlugin extends AbstractOracleTextSearchPlugin {

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
     */
    public WsfResponse execute(WsfRequest request) throws WsfServiceException {
        logger.info("Invoking OrthomclKeywordSearchPlugin...");

        // get parameters
        Map<String, String> params = request.getParams();
	String recordType = params.get(PARAM_WDK_RECORD_TYPE).trim().replaceAll("'", "");
        String fields = params.get(PARAM_DATASETS).trim().replaceAll("'", "");
        logger.debug("fields = \"" + fields + "\"");
        String textExpression = params.get(PARAM_TEXT_EXPRESSION).trim();
	String detailTable = params.get(PARAM_DETAIL_TABLE).trim().replaceAll("'", "");
	String primaryKeyColumn = params.get(PARAM_PRIMARY_KEY_COLUMN).trim().replaceAll("'", "");
	String projectId = params.get(PARAM_PROJECT_ID).trim().replaceAll("'", "");

	// get fields as quoted list
        StringBuffer quotedFields = new StringBuffer("");
        boolean notFirst = false;
        String[] ds = fields.split(",\\s*");
        for (String field : ds) {
            if (notFirst) quotedFields.append(",");
            quotedFields.append("'" + field + "'");
            notFirst = true;
        }

        String oracleTextExpression = transformQueryString(textExpression);
	PreparedStatement ps = null;
        Map<String, SearchResult> matches = new HashMap<String, SearchResult>();
	try {
	    ps = getQuery(recordType, detailTable, primaryKeyColumn, projectId,
			  oracleTextExpression, quotedFields.toString());
	    matches = textSearch(ps, primaryKeyColumn);
	} catch (SQLException ex) {
	    throw new WsfServiceException(ex);
	} finally {
	    SqlUtils.closeStatement(ps);
	}

        // construct results
        String[][] result = flattenMatches(getMatchesSortedArray(matches), request.getOrderedColumns());
        WsfResponse wsfResult = new WsfResponse();
        wsfResult.setResult(result);
        wsfResult.setSignal(0);
        return wsfResult;
    }


    private PreparedStatement getQuery(String recordType, String detailTable,
				       String primaryKeyColumn, String projectId,
				       String oracleTextExpression, String fields)
	throws WsfServiceException,
            SQLException {
        Connection dbConnection = getDbConnection();

        String sql = new String(
				"select " + primaryKeyColumn + ",\n"
			        + "       '" + projectId + "' as project_id, max(scoring) as max_score,\n"
			        + "       apidb.tab_to_string(set(CAST(COLLECT(field_name) as apidb.varchartab)), ', ')\n"
			        + "        as fields_matched\n"
			        + "from (select  " + primaryKeyColumn + ", field_name, score(1) as scoring\n"
			        + "      from " + detailTable + "\n"
			        + "      where field_name in (" + fields + ")\n"
			        + "        and contains(content, ?, 1) > 0)\n"
			        + "group by  " + primaryKeyColumn + "\n"
			        + "order by max(scoring) desc");
        logger.debug("SQL: " + sql);
        logger.debug("oracleTextExpression = \"" + oracleTextExpression + "\"");
        logger.debug("fields = \"" + fields + "\"");
        logger.debug("recordType = \"" + recordType + "\"");

        PreparedStatement ps = null;
        try {
            ps = dbConnection.prepareStatement(sql);
            ps.setString(1, oracleTextExpression);
        } catch (SQLException e) {
            logger.info("caught SQLException " + e.getMessage());
            throw new WsfServiceException(e);
        }

        return ps;
    }

}
