/**
 * TranscriptSearchPlugin -- text search using Oracle Text
 */
package org.apidb.apicomplexa.wsfplugin.textsearch;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.apidb.apicommon.model.comment.CommentFactory;
import org.eupathdb.websvccommon.wsfplugin.EuPathServiceException;
import org.eupathdb.websvccommon.wsfplugin.textsearch.AbstractOracleTextSearchPlugin;
import org.eupathdb.websvccommon.wsfplugin.textsearch.SearchResult;
import org.gusdb.fgputil.db.SqlUtils;
import org.gusdb.fgputil.db.pool.DatabaseInstance;
import org.gusdb.fgputil.runtime.InstanceManager;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;


/**
 * @author John I
 * @created Nov 16, 2008
 */
public class TranscriptSearchPlugin extends AbstractOracleTextSearchPlugin {

  private static final Logger logger = Logger.getLogger(TranscriptSearchPlugin.class);

  // required parameter definition
  public static final String PARAM_ORGANISMS = "text_search_organism";
  public static final String PARAM_COMPONENT_INSTANCE = "component_instance";
  public static final String PARAM_MAX_PVALUE = "max_pvalue";
  
  @Override
  public String[] getColumns() {
      return new String[] { COLUMN_RECORD_ID, COLUMN_GENE_SOURCE_ID, COLUMN_MATCHED_RESULT, COLUMN_MAX_SCORE };
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
   */
  @Override
  public int execute(PluginRequest request, PluginResponse response) throws PluginModelException, PluginUserException
      {
    logger.debug("Invoking TranscriptSearchPlugin...");

    // get parameters
    Map<String, String> params = request.getParams();
    String fields = params.get(PARAM_DATASETS).trim().replaceAll("'", "");
    logger.debug("fields = \"" + fields + "\"");
    String textExpression = params.get(PARAM_TEXT_EXPRESSION).trim();

    String organisms = params.get(PARAM_ORGANISMS);
    // isolate and compound text search do not use this parameter
    if (organisms != null)
      organisms = cleanOrgs(organisms);

    String maxPvalue = params.get(PARAM_MAX_PVALUE);

    boolean searchComments = false;
    boolean searchComponent = false;
    boolean communityAnnotationRecords = false;
    boolean commentRecords = false;

    StringBuffer quotedFields = new StringBuffer("");
    boolean notFirst = false;
    String[] ds = fields.split(",\\s*");
    for (String field : ds) {
      if (notFirst)
        quotedFields.append(",");
      quotedFields.append("'" + field + "'");
      notFirst = true;

      if (field.equals("UserComments")) {
        searchComments = true;
        commentRecords = true;
      } else if (field.equals("CommunityAnnotation")) {
        searchComments = true;
        communityAnnotationRecords = true;
      } else {
        searchComponent = true;
      }
    }

    // search comments
    Map<String, SearchResult> commentResults = new LinkedHashMap<>();
    String oracleTextExpression = transformQueryString(textExpression);
    logger.debug("oracleTextExpression = \"" + oracleTextExpression + "\"");
    String projectId = request.getProjectId();
    String sql;
    if (searchComments) {
        logger.debug("searching comment instance\n");
	PreparedStatement ps = null;
	try {
	    sql = getCommentQuery(projectId, commentRecords, communityAnnotationRecords);
	    
	    CommentFactory commentFactory = InstanceManager.getInstance(CommentFactory.class, projectId);
	    ps = SqlUtils.getPreparedStatement(commentFactory.getCommentDataSource(), sql);
	    ps.setString(1, oracleTextExpression);
	    ps.setString(2, oracleTextExpression);
	    BufferedResultContainer commentContainer = new BufferedResultContainer();
	    textSearch(commentContainer, ps, "source_id", sql, "commentTextSearch");
	    commentResults = validateRecords(projectId, commentContainer.getResults(), organisms);
	} catch (SQLException | EuPathServiceException ex) {
	    throw new PluginModelException(ex);
	} finally {
	    SqlUtils.closeStatement(ps);
	}
    }

    // merge the result from component with the ones from comments
    MergeResultContainer componentContainer =
        new MergeResultContainer(response, request.getOrderedColumns(), commentResults);

    // search component database
    if (searchComponent) {
        logger.debug("searching component instance\n");
	PreparedStatement ps = null;
	try {
	    sql = getComponentQuery(projectId, organisms, quotedFields.toString());

	    if (maxPvalue == null || maxPvalue.equals("")) {
		maxPvalue = "0";
	    }

	    WdkModel wdkModel = InstanceManager.getInstance(WdkModel.class, projectId);
	    ps = SqlUtils.getPreparedStatement(wdkModel.getAppDb().getDataSource(), sql);
	    ps.setString(1, oracleTextExpression);
	    ps.setString(2, oracleTextExpression);
	    ps.setFloat(3, Float.valueOf(maxPvalue));
	    ps.setString(4, oracleTextExpression);
	    ps.setString(5, oracleTextExpression);

	    textSearch(componentContainer, ps, "source_id", sql, "componentTextSearch");
	}
	catch (SQLException | EuPathServiceException ex) {
	    throw new PluginModelException(ex);
	}
	finally {
	    SqlUtils.closeStatement(ps);
	}
    }

    // process the remaining ones from comments, but not found in component
    componentContainer.processRemainingResults();
    
    return 0;
  }

  private String cleanOrgs(String orgs) {
    String[] temp;
    StringBuffer sb = new StringBuffer();
    String delimiter = ",";
    temp = orgs.split(delimiter);
    for (int i = 0; i < temp.length; i++) {
      if (!temp[i].contains("-1"))
        sb.append(temp[i].trim() + ",");
      else
        logger.debug("organism value: (" + temp[i]
            + ") not included, we only care about leaf nodes\n");
    }
    int strLen = sb.toString().trim().length();
    if (strLen < 2) {
      return "";
    }
    return sb.toString().trim().substring(0, strLen - 1);
  }

  private String getCommentQuery(String projectId, boolean commentRecords,
				 boolean communityAnnotationRecords) {

    String recordTypePredicate;
    if (commentRecords && !communityAnnotationRecords) {
      recordTypePredicate = new String(" and review_status_id != 'community' ");
    } else if (!commentRecords && communityAnnotationRecords) {
      recordTypePredicate = new String(" and review_status_id = 'community' ");
    } else {
      recordTypePredicate = " AND comment_target_id = 'gene' \n";
    }

    WdkModel wdkModel = InstanceManager.getInstance(WdkModel.class, projectId);
    String commentSchema = wdkModel.getModelConfig().getUserDB().getUserSchema();

    // this query has to run on the comment db, because dblinks don't support the clob operations we are doing
    String sql = "SELECT null as source_id, gene_source_id, '" + projectId + "' as project_id, 'Y' as matched_result, \n"
        + "           max_score as max_score, -- should be weighted using component TableWeight \n"
        + "           fields_matched \n"
        + "       FROM (" +
        "            SELECT gene_source_id, MAX(scoring) as max_score, \n"
        + "                apidb.tab_to_string(set(CAST(COLLECT(table_name) AS apidb.varchartab)), ', ') as fields_matched, "
        + "                max(oracle_rowid) keep (dense_rank first order by scoring desc) as best_rowid \n"
        + "           FROM ("
        + "              SELECT SCORE(1) as scoring, \n"
        + "                   DECODE(c.review_status_id, 'community', 'Community Annotation', 'User Comments') as table_name, \n"
        + "                   tsc.source_id as gene_source_id, tsc.rowid as oracle_rowid \n"
        + "              FROM apidb.TextSearchableComment tsc, " + commentSchema + "Comments c \n"
        + "              WHERE ( (CONTAINS(tsc.content, ?, 1) > 0) OR (? = '%') )  \n"
        + "               AND tsc.comment_id = c.comment_id\n"
        + "               AND c.is_visible = 1\n"
        + "               AND c.review_status_id != 'task'\n"
        + "               AND c.review_status_id != 'rejected'\n"
        +                 recordTypePredicate
        + "               AND project_id = '" + projectId + "'"
        + "           ) \n" 
        + "          GROUP BY gene_source_id \n"
        + "          ORDER BY max_score desc \n"
        + "       )";

    logger.info("comment SQL: " + sql);

    return sql;
  }

  private String getComponentQuery(String projectId, String organisms, String fields) {

    String sql = new String(
        "select source_id, gene_source_id, '" + projectId + "' as project_id, 'Y' as matched_result, count(*) as max_score,  \n"
            + "       apidb.tab_to_string(set(cast(collect(table_name) AS apidb.varchartab)), ', ')  fields_matched \n"
            + "from (  SELECT distinct min(trans.source_id) as source_id, b.gene_source_id, regexp_replace(external_database_name, '_RSRC$', '') as table_name \n"
            + "        FROM ApidbTuning.Blastp b, ApidbTuning.TranscriptAttributes trans  \n"
            + "        WHERE (CONTAINS(b.description, ?, 1) > 0 OR ? = '%') \n"
            + "          AND 'Blastp' in (" + fields + ") \n"
            + "          AND b.pvalue_exp < ? \n"
            + "          AND b.query_taxon_id in (" + organisms + ") \n"
            + "          AND trans.gene_source_id = b.gene_source_id \n"
            + "          GROUP BY b.gene_source_id, external_database_name \n"
            + "      UNION ALL  \n"
            + "        SELECT min(trans.source_id) as source_id, gd.source_id as gene_source_id, gd.field_name as table_name \n"
            + "        FROM Apidb.GeneDetail gd, apidbtuning.transcriptattributes trans\n"
            + "        WHERE (CONTAINS(gd.content, ?, 1) > 0 OR ? = '%')\n"
            + "         AND gd.field_name in (" + fields + ") \n"
            + "         AND trans.taxon_id in (" + organisms + ") \n"
            + "         AND trans.gene_source_id = gd.source_id \n"
            + "         GROUP BY gd.source_id, gd.field_name \n"
            + "     )  \n"
            + "GROUP BY source_id, gene_source_id \n"
            + "ORDER BY max_score desc, gene_source_id \n");
    logger.debug("component SQL: " + sql);

    return sql;
  }


  private Map<String, SearchResult> validateRecords(String projectId,
      Map<String, SearchResult> commentResults, String organisms) throws PluginModelException
       {

    Map<String, SearchResult> newCommentResults = new HashMap<String, SearchResult>();

    logger.debug("organisms = \"" + organisms + "\"");

    PreparedStatement validationQuery = null;
    try {
      String sql = new String(
          "select attrs.source_id\n"
              + "from ApidbTuning.GeneId alias, ApidbTuning.TranscriptAttributes attrs \n"
              + "where alias.Id = ? \n"
              + "  and alias.gene = attrs.gene_source_id \n"
              + "  and alias.unique_mapping = 1 \n"
              + "  and attrs.taxon_id in ("
              + organisms + ")");

      ResultSet rs = null;
      try {
        WdkModel wdkModel = InstanceManager.getInstance(WdkModel.class, projectId);
        DatabaseInstance platform = wdkModel.getAppDb();
        DataSource dataSource = platform.getDataSource();

        validationQuery = SqlUtils.getPreparedStatement(dataSource, sql);

        for (String sourceId : commentResults.keySet()) {
          logger.debug("validating sourceId \"" + sourceId + "\"");
          rs = null;
          validationQuery.setString(1, sourceId);
          rs = SqlUtils.executePreparedQuery(validationQuery, sql, "ApicommValidateQuery");
          SearchResult result = commentResults.get(sourceId);
          // commentResults.remove(sourceId);
          while (rs.next()) {
            String returnedTranscript = rs.getString("source_id");
            SearchResult newResult = new SearchResult(returnedTranscript, result.getMaxScore(), result.getFieldsMatched());
            newCommentResults.put(returnedTranscript, newResult);
          }
          SqlUtils.closeResultSetOnly(rs);
        }
      } catch (SQLException ex) {
        logger.error("caught SQLException " + ex.getMessage());
        throw new PluginModelException(ex);
      }
    }
    finally {
      SqlUtils.closeStatement(validationQuery);
    }

    return newCommentResults;
  }
}
