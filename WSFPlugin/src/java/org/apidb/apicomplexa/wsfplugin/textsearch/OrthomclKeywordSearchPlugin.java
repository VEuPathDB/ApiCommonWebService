/**
 * KeywordSearchPlugin -- text search using Oracle Text
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

    // required parameter definition
    public static final String PARAM_WDK_RECORD_TYPE = "wdk_record_type";

    public static final String COLUMN_RECORD_ID = "RecordID";
    public static final String COLUMN_DATASETS = "Datasets";

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
        String textExpression = params.get(PARAM_TEXT_EXPRESSION).trim();
        logger.debug("fields = \"" + fields + "\"");

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
	    ps = getQuery("dontcare", recordType,
				   oracleTextExpression, quotedFields.toString());
	    matches = textSearch(ps);
	} catch (SQLException ex) {
	    throw new WsfServiceException(ex);
	} finally {
	    SqlUtils.closeStatement(ps);
	}

        // construct results
	SearchResult[] TMP_MATCHES = new SearchResult[1];
        String[][] result = flattenMatches(TMP_MATCHES, request.getOrderedColumns());
        WsfResponse wsfResult = new WsfResponse();
        wsfResult.setResult(result);
        wsfResult.setSignal(0);
        return wsfResult;
    }


    private PreparedStatement getQuery(String projectId,
            String recordType, String oracleTextExpression,
            String fields) throws WsfServiceException,
            SQLException {
        Connection dbConnection = getDbConnection();
	String organisms = "don't need this";

        String sql = new String(
                "SELECT source_id, project_id, \n"
                        + "       max_score, \n"
                        + "       fields_matched \n"
                        + "FROM (SELECT source_id, project_id, MAX(scoring) as max_score, \n"
                        + "             apidb.tab_to_string(set(CAST(COLLECT(table_name) AS apidb.varchartab)), ', ')  fields_matched, \n"
                        + "             max(index_name) keep (dense_rank first order by scoring desc, source_id, table_name) as index_name, \n"
                        + "             max(oracle_rowid) keep (dense_rank first order by scoring desc, source_id, table_name) as oracle_rowid \n"
                        + "      FROM (  SELECT SCORE(1) * (select nvl(max(weight), 1) from ApidbTuning.TableWeight where table_name = 'Blastp') \n"
                        + "                       as scoring, \n"
                        + "                    'ApidbTuning.Blastp_text_ix' as index_name, b.rowid as oracle_rowid, b.source_id, b.project_id, \n"
                        + "                    external_database_name as table_name \n"
                        + "              FROM ApidbTuning.Blastp b \n"
                        + "              WHERE CONTAINS(b.description, ?, 1) > 0 \n"
                        + "                AND 'Blastp' in ("
                        + fields
                        + ") \n"
                        + "                AND '"
                        + recordType
                        + "' = 'gene' \n"
                        + "                AND b.pvalue_exp < ? \n"
                        + "                AND b.query_taxon_id in ("
                        + organisms
                        + ") \n"
                        + "            UNION ALL \n"
                        + "              SELECT SCORE(1)* nvl(tw.weight, 1) \n"
                        + "                       as scoring, \n"
                        + "                     'apidb.gene_text_ix' as index_name, gt.rowid as oracle_rowid, gt.source_id, gt.project_id, gt.field_name as table_name\n"
                        + "              FROM apidb.GeneDetail gt, ApidbTuning.TableWeight tw, ApidbTuning.GeneAttributes ga \n"
                        + "              WHERE CONTAINS(content, ?, 1) > 0\n"
                        + "                AND gt.field_name in ("
                        + fields
                        + ") \n"
                        + "                AND '"
                        + recordType
                        + "' = 'gene' \n"
                        + "                AND gt.field_name = tw.table_name(+) \n"
                        + "                AND gt.source_id = ga.source_id \n"
                        + "                AND ga.taxon_id in ("
                        + organisms
                        + ") \n"
                        + "            UNION ALL \n"
                        + "              SELECT SCORE(1) * nvl(tw.weight, 1)  \n"
                        + "                       as scoring, \n"
                        + "                    'apidb.isolate_text_ix' as index_name, wit.rowid as oracle_rowid, wit.source_id, wit.project_id, wit.field_name as table_name \n"
                        + "              FROM apidb.IsolateDetail wit, ApidbTuning.TableWeight tw \n"
                        + "              WHERE CONTAINS(content, ?, 1) > 0 \n"
                        + "                AND wit.field_name in ("
                        + fields
                        + ") \n"
                        + "                AND '"
                        + recordType
                        + "' = 'isolate' \n"
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

        PreparedStatement ps = null;
        try {
            ps = dbConnection.prepareStatement(sql);
            ps.setString(1, oracleTextExpression);
            ps.setString(3, oracleTextExpression);
            ps.setString(4, oracleTextExpression);
        } catch (SQLException e) {
            logger.info("caught SQLException " + e.getMessage());
            throw new WsfServiceException(e);
        }

        return ps;
    }

}
