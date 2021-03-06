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
 * KeywordSearchPlugin -- text search using Oracle Text
 * 
 * @author John I
 * @created Nov 16, 2008
 */
public class KeywordSearchPlugin extends AbstractOracleTextSearchPlugin {

  private static final Logger logger = Logger.getLogger(KeywordSearchPlugin.class);

  // required parameter definition
  public static final String PARAM_ORGANISMS = "text_search_organism";
  public static final String PARAM_COMPONENT_INSTANCE = "component_instance";
  public static final String PARAM_MAX_PVALUE = "max_pvalue";

  @Override
  public int execute(PluginRequest request, PluginResponse response)
      throws PluginModelException, PluginUserException {

    logger.info("Invoking KeywordSearchPlugin...");

    // get parameters
    String recordType;
    Map<String, String> params = request.getParams();
    if (params.get(PARAM_WDK_RECORD_TYPE) == null) {
      recordType = "gene";
    }
    else {
      recordType = params.get(PARAM_WDK_RECORD_TYPE).trim().replaceAll("'", "");
    }
    String fields = params.get(PARAM_DATASETS).trim().replaceAll("'", "");
    logger.debug("fields = \"" + fields + "\"");
    String textExpression = params.get(PARAM_TEXT_EXPRESSION).trim();

    String organisms = params.get(PARAM_ORGANISMS);
    logger.debug("organisms before cleaning = \"" + organisms + "\"");
    // isolate and compound text search do not use this parameter
    if (organisms != null)
      organisms = cleanOrgs(organisms);
    logger.debug("organisms after cleaning= \"" + organisms + "\"");

    // String maxPvalue = params.get(PARAM_MAX_PVALUE);

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

      if (field.equals("Comments")) {
        searchComments = true;
        commentRecords = true;
      }
      else if (field.equals("CommunityAnnotation")) {
        searchComments = true;
        communityAnnotationRecords = true;
      }
      else {
        searchComponent = true;
      }
    }

    // check for all-wildcard search
    boolean pureWildcard = textExpression.equals("'%'");

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
        sql = getCommentQuery(projectId, recordType, commentRecords, communityAnnotationRecords,
            pureWildcard);

        CommentFactory commentFactory = InstanceManager.getInstance(CommentFactory.class, projectId);
        ps = SqlUtils.getPreparedStatement(commentFactory.getCommentDataSource(), sql);
        if (!pureWildcard) {
          ps.setString(1, oracleTextExpression);
        }
        BufferedResultContainer commentContainer = new BufferedResultContainer();
        textSearch(commentContainer, ps, "source_id", sql, "commentTextSearch");
        commentResults = validateRecords(projectId, commentContainer.getResults(), organisms);
        // logger.debug("after validation commentMatches = "
        // + commentMatches.toString());
      }
      catch (SQLException | EuPathServiceException ex) {
        throw new PluginModelException(ex);
      }
      finally {
        SqlUtils.closeStatement(ps);
      }
    }

    // merge the result from component with the ones from comments
    MergeResultContainer componentContainer = new MergeResultContainer(response, request.getOrderedColumns(),
        commentResults);

    // search component database
    if (searchComponent) {
      logger.debug("searching component instance\n");
      PreparedStatement ps = null;
      try {
        sql = getComponentQuery(projectId, recordType, quotedFields.toString(), pureWildcard);

        // if (maxPvalue == null || maxPvalue.equals("")) {
        // maxPvalue = "0";
        // }

        WdkModel wdkModel = InstanceManager.getInstance(WdkModel.class, projectId);
        ps = SqlUtils.getPreparedStatement(wdkModel.getAppDb().getDataSource(), sql);
        if (!pureWildcard) {
          ps.setString(1, oracleTextExpression);
          ps.setString(2, oracleTextExpression);
        }

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
        logger.debug("organism value: (" + temp[i] + ") not included, we only care for leave nodes\n");
    }
    int strLen = sb.toString().trim().length();
    if (strLen < 2) {
      return "";
    }
    return sb.toString().trim().substring(0, strLen - 1);
  }

  private String getCommentQuery(String projectId, String recordType, boolean commentRecords,
      boolean communityAnnotationRecords, boolean pureWildcard) {

    String recordTypePredicate;
    if (commentRecords && !communityAnnotationRecords) {
      recordTypePredicate = new String(" and review_status_id != 'community' ");
    }
    else if (!commentRecords && communityAnnotationRecords) {
      recordTypePredicate = new String(" and review_status_id = 'community' ");
    }
    else {
      recordTypePredicate = " AND comment_target_id = '" + recordType + "'\n";
    }

    WdkModel wdkModel = InstanceManager.getInstance(WdkModel.class, projectId);
    String commentSchema = wdkModel.getModelConfig().getUserDB().getUserSchema();

    String sql =
        "SELECT source_id, '" + projectId + "' as project_id, \n" +
        "  max_score as max_score, -- should be weighted using component TableWeight \n" +
        "  fields_matched \n" +
        "FROM (" +
        "  SELECT source_id, MAX(scoring) as max_score, \n" +
        "    apidb.tab_to_string(set(CAST(COLLECT(table_name) AS apidb.varchartab)), ', ') as fields_matched, " +
        "    max(oracle_rowid) keep (dense_rank first order by scoring desc) as best_rowid \n" +
        "  FROM (" +
        "    SELECT " + (pureWildcard ? " 1 " : " SCORE(1) ") + " as scoring, \n" +
        "      DECODE(c.review_status_id, 'community', 'Community Annotation', 'User Comments') as table_name, \n" +
        "             tsc.source_id, tsc.rowid as oracle_rowid \n" +
        "    FROM apidb.TextSearchableComment tsc, " + commentSchema + "Comments c \n" +
        "    WHERE " + (pureWildcard ? " 1 = 1 " : " CONTAINS(tsc.content, ?, 1) > 0 ") + " \n" +
        "      AND tsc.comment_id = c.comment_id\n" + "              AND c.is_visible = 1\n" +
        "      AND c.review_status_id != 'task'\n" +
        "      AND c.review_status_id != 'rejected'\n" + recordTypePredicate +
        "      AND project_id = '" + projectId + "'" +
        "  ) \n" +
        "  GROUP BY source_id \n" +
        "  ORDER BY max_score desc \n" +
        ")";

    logger.debug("comment SQL: " + sql);

    return sql;
  }

  private String getComponentQuery(String projectId, String recordType, String fields, boolean pureWildcard) {

    String sql =
        "SELECT source_id, '" + projectId + "' as project_id, count(*) as max_score,  \n" +
        "       apidb.tab_to_string(set(cast(collect(table_name) AS apidb.varchartab)), ', ')  fields_matched \n" +
        "FROM ( \n" +
        "    SELECT wit.source_id, wit.field_name as table_name  \n" +
        "    FROM apidb.IsolateDetail wit \n" +
        "    WHERE " + (pureWildcard ? " 1 = 1 " : " CONTAINS(content, ?, 1) > 0 ") + " \n" +
        "      AND wit.field_name in (" + fields + ") \n" +
        "      AND '" + recordType + "' = 'popset' \n" +
        "  UNION ALL  \n" +
        "    SELECT wit.source_id, replace(replace(replace(wit.field_name, 'CompoundsMetabolicPathways', 'Reactions and Enzymes'), 'PathwaysFromCompounds', 'Pathways'), 'CompoundName', 'Compound Name')  as table_name  \n" +
        "    FROM apidb.CompoundDetail wit \n" +
        "    WHERE " + (pureWildcard ? " 1 = 1 " : " CONTAINS(content, ?, 1) > 0 ") + " \n" +
        "      AND wit.field_name in (" + fields + ") \n" +
        "      AND '" + recordType + "' = 'compound' \n" +
        ")  \n" +
        "GROUP BY source_id \n" +
        "ORDER BY max_score desc, source_id \n";

    logger.debug("component SQL: " + sql);

    return sql;
  }

  private Map<String, SearchResult> validateRecords(String projectId,
      Map<String, SearchResult> commentResults, String organisms) throws PluginModelException {

    Map<String, SearchResult> newCommentResults = new HashMap<String, SearchResult>();
    newCommentResults.putAll(commentResults);

    logger.debug("organisms = \"" + organisms + "\"");

    PreparedStatement validationQuery = null;
    try {
      String sql =
          "select attrs.source_id \n" +
          "from ApidbTuning.GeneId alias, ApidbTuning.GeneAttributes attrs \n" +
          "where alias.Id = ? \n" +
          "  and alias.gene = attrs.source_id \n" +
          "  and alias.unique_mapping = 1 \n" +
          "  and attrs.taxon_id in (" + organisms + ")";

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
          if (!rs.next()) {
            // no match; drop result
            logger.trace("dropping unrecognized ID \"" + sourceId + "\" (organisms \"" + organisms +
                "\") from comment-search result set.");
            newCommentResults.remove(sourceId);
          }
          else {
            String returnedSourceId = rs.getString("source_id");
            // logger.debug("validation query returned \"" + returnedSourceId
            // + "\"");
            if (!returnedSourceId.equals(sourceId)) {
              // ID changed; substitute returned value
              logger.trace("Substituting valid ID \"" + returnedSourceId + "\" for ID \"" + sourceId +
                  "\" returned from comment-search result set.");
              SearchResult result = newCommentResults.get(sourceId);
              result.setSourceId(returnedSourceId);
              result.setProjectId(projectId);
              newCommentResults.remove(sourceId);
              newCommentResults.put(returnedSourceId, result);
            }
          }
          SqlUtils.closeResultSetOnly(rs);
        }
      }
      catch (SQLException ex) {
        logger.error("caught SQLException " + ex.getMessage());
        throw new PluginModelException(ex);
      }
      finally {
        // try {
        // rs.close();
        // } catch (SQLException ex) {
        // logger.info("caught SQLException " + ex.getMessage());
        // throw new WsfServiceException(ex);
        // }
      }
    }
    finally {
      SqlUtils.closeStatement(validationQuery);
    }
    // Map<String, SearchResult> otherCommentMatches = new HashMap<String,
    // SearchResult>();
    return newCommentResults;
  }
}
