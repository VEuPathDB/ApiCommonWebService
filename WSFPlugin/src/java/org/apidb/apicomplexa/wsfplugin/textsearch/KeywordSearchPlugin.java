/**
 * KeywordSearchPlugin -- text search using Oracle Text
 */
package org.apidb.apicomplexa.wsfplugin.textsearch;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.eupathdb.websvccommon.wsfplugin.EuPathServiceException;
import org.eupathdb.websvccommon.wsfplugin.textsearch.AbstractOracleTextSearchPlugin;
import org.eupathdb.websvccommon.wsfplugin.textsearch.SearchResult;
import org.gusdb.fgputil.db.SqlUtils;
import org.gusdb.fgputil.db.pool.DatabaseInstance;
import org.gusdb.wdk.controller.CConstants;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.jspwrap.WdkModelBean;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author John I
 * @created Nov 16, 2008
 */
public class KeywordSearchPlugin extends AbstractOracleTextSearchPlugin {

  // required parameter definition
  public static final String PARAM_ORGANISMS = "text_search_organism";
  public static final String PARAM_COMPONENT_INSTANCE = "component_instance";
  public static final String PARAM_WDK_RECORD_TYPE = "wdk_record_type";
  public static final String PARAM_MAX_PVALUE = "max_pvalue";

  private static final String CTX_CONTAINER_APP = "wdkModel";
  private static final String CTX_CONTAINER_COMMENT = "comment-factory";

  private static final String CONNECTION_APP = WdkModel.CONNECTION_APP;

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
   */
  @Override
  public void execute(PluginRequest request, PluginResponse response)
      throws WsfServiceException {
    logger.info("Invoking KeywordSearchPlugin...");

    // get parameters
    String recordType;
    Map<String, String> params = request.getParams();
    if (params.get(PARAM_WDK_RECORD_TYPE) == null) {
      recordType = "gene";
    } else {
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

    // search comments
    Map<String, SearchResult> commentResults = new LinkedHashMap<>();
    String oracleTextExpression = transformQueryString(textExpression);
    String projectId = request.getProjectId();
    if (searchComments) {
      PreparedStatement ps = null;
      try {
        ps = getCommentQuery(projectId, recordType, oracleTextExpression,
            commentRecords, communityAnnotationRecords);
        BufferedResultContainer commentContainer = new BufferedResultContainer();
        textSearch(commentContainer, ps, "source_id");
        commentResults = validateRecords(projectId,
            commentContainer.getResults(), organisms);
        // logger.debug("after validation commentMatches = "
        // + commentMatches.toString());
      } catch (SQLException | WdkModelException | EuPathServiceException ex) {
        throw new WsfServiceException(ex);
      } finally {
        SqlUtils.closeStatement(ps);
      }
    }

    // search component database
    if (searchComponent) {
      PreparedStatement ps = null;
      try {
        ps = getComponentQuery(projectId, recordType, organisms,
            oracleTextExpression, quotedFields.toString(), maxPvalue);
        // merge the result from component with the ones from comments
        MergeResultContainer componentContainer = new MergeResultContainer(
            response, request.getOrderedColumns(), commentResults);
        textSearch(componentContainer, ps, "source_id");
        // process the remaining ones from comments, but not found in component
        componentContainer.processRemainingResults();
      } catch (SQLException | WdkModelException | EuPathServiceException ex) {
        throw new WsfServiceException(ex);
      } finally {
        SqlUtils.closeStatement(ps);
      }
    }
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
            + ") not included, we only care for leave nodes\n");
    }
    int strLen = sb.toString().trim().length();
    if (strLen < 2) {
      return "";
    }
    return sb.toString().trim().substring(0, strLen - 1);
  }

  private PreparedStatement getCommentQuery(String projectId,
      String recordTypePredicate, String oracleTextExpression,
      boolean commentRecords, boolean communityAnnotationRecords)
      throws WsfServiceException, SQLException, WdkModelException,
      EuPathServiceException {
    Connection dbConnection = getDbConnection(CTX_CONTAINER_COMMENT, null);

    if (commentRecords && !communityAnnotationRecords) {
      recordTypePredicate = new String(" and review_status_id != 'community' ");
    } else if (!commentRecords && communityAnnotationRecords) {
      recordTypePredicate = new String(" and review_status_id = 'community' ");
    } else {
      // Added by Jerric - the code is unclear, why a value is passed in
      // while it will be over-written within the method?
      // handle the default case
      recordTypePredicate = " AND comment_target_id = '" + recordTypePredicate
          + "'\n";
    }

    String sql = "SELECT source_id, project_id, \n"
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
        + "              AND c.review_status_id != 'task'\n"
        + "              AND c.review_status_id != 'rejected'\n"
        + recordTypePredicate + "              AND project_id = '" + projectId
        + "') \n" + "      GROUP BY source_id, project_id \n"
        + "      ORDER BY max_score desc \n" + "     )";

    logger.debug("comment SQL: " + sql);
    logger.debug("oracleTextExpression = \"" + oracleTextExpression + "\"");

    PreparedStatement ps = null;
    try {
      ps = dbConnection.prepareStatement(sql);
      ps.setString(1, oracleTextExpression);
    } catch (SQLException e) {
      logger.error("caught SQLException " + e.getMessage());
      throw new WsfServiceException(e);
    }

    return ps;
  }

  private PreparedStatement getComponentQuery(String projectId,
      String recordType, String organisms, String oracleTextExpression,
      String fields, String maxPvalue) throws WsfServiceException,
      SQLException, WdkModelException, EuPathServiceException {
    Connection dbConnection = getDbConnection(CTX_CONTAINER_APP, CONNECTION_APP);

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
            + "            UNION ALL \n"
            + "              SELECT SCORE(1) * nvl(tw.weight, 1)  \n"
            + "                       as scoring, \n"
            + "                    'apidb.compound_text_ix' as index_name, wit.rowid as oracle_rowid, wit.source_id, wit.project_id, wit.field_name as table_name \n"
            + "              FROM apidb.CompoundDetail wit, ApidbTuning.TableWeight tw \n"
            + "              WHERE CONTAINS(content, ?, 1) > 0 \n"
            + "                AND wit.field_name in ("
            + fields
            + ") \n"
            + "                AND '"
            + recordType
            + "' = 'compound' \n"
            + "                AND wit.field_name = tw.table_name(+) \n"
            + "           ) \n"
            + "      GROUP BY source_id, project_id \n"
            + "      ORDER BY max_score desc, source_id \n" + "     )");
    logger.debug("component SQL: " + sql);
    logger.debug("organisms = \"" + organisms + "\"; oracleTextExpression = \""
        + oracleTextExpression + "\"");
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
      ps.setString(5, oracleTextExpression);
    } catch (SQLException e) {
      logger.error("caught SQLException " + e.getMessage());
      throw new WsfServiceException(e);
    }

    return ps;
  }

  // private PreparedStatement getValidationQuery() throws WsfServiceException {
  // String sql = new String("select attrs.source_id, attrs.project_id \n"
  // + "from ApidbTuning.GeneId alias, ApidbTuning.GeneAttributes attrs \n"
  // + "where alias.Id = ? \n"
  // + "  and alias.gene = attrs.source_id \n"
  // + "  and alias.unique_mapping = 1 \n"
  // + "  and attrs.project_id = ? \n"
  // + "  and ? like '%' || attrs.organism || '%'");
  //
  // WdkModelBean wdkModel = (WdkModelBean) this.context
  // .get(CConstants.WDK_MODEL_KEY);
  // DBPlatform platform = wdkModel.getModel().getQueryPlatform();
  // DataSource dataSource = platform.getDataSource();
  //
  // try {
  // return SqlUtils.getPreparedStatement(dataSource, sql);
  // } catch (SQLException ex) {
  // throw new WsfServiceException(ex);
  // }
  // }

  private Map<String, SearchResult> validateRecords(String projectId,
      Map<String, SearchResult> commentResults, String organisms)
      throws WsfServiceException {

    Map<String, SearchResult> newCommentResults = new HashMap<String, SearchResult>();
    newCommentResults.putAll(commentResults);

    logger.debug("organisms = \"" + organisms + "\"");

    PreparedStatement validationQuery = null;
    try {
      String sql = new String(
          "select attrs.source_id, attrs.project_id \n"
              + "from ApidbTuning.GeneId alias, ApidbTuning.GeneAttributes attrs \n"
              + "where alias.Id = ? \n"
              + "  and alias.gene = attrs.source_id \n"
              + "  and alias.unique_mapping = 1 \n"
              + "  and attrs.project_id = ? \n" + "  and attrs.taxon_id in ("
              + organisms + ")");

      WdkModelBean wdkModel = (WdkModelBean) this.context.get(CConstants.WDK_MODEL_KEY);
      DatabaseInstance platform = wdkModel.getModel().getAppDb();
      DataSource dataSource = platform.getDataSource();

      ResultSet rs = null;

      try {
        validationQuery = SqlUtils.getPreparedStatement(dataSource, sql);

        for (String sourceId : commentResults.keySet()) {
          // logger.debug("validating sourceId \"" + sourceId + "\"");
          rs = null;
          validationQuery.setString(1, sourceId);
          validationQuery.setString(2, projectId);
          rs = validationQuery.executeQuery();
          if (!rs.next()) {
            // no match; drop result
            logger.trace("dropping unrecognized ID \"" + sourceId
                + "\" (project \"" + projectId + "\", organisms \"" + organisms
                + "\") from comment-search result set.");
            newCommentResults.remove(sourceId);
          } else {
            String returnedSourceId = rs.getString("source_id");
            // logger.debug("validation query returned \"" + returnedSourceId
            // + "\"");
            if (!returnedSourceId.equals(sourceId)) {
              // ID changed; substitute returned value
              logger.trace("Substituting valid ID \"" + returnedSourceId
                  + "\" for ID \"" + sourceId
                  + "\" returned from comment-search result set.");
              SearchResult result = newCommentResults.get(sourceId);
              result.setSourceId(returnedSourceId);
              newCommentResults.remove(sourceId);
              newCommentResults.put(returnedSourceId, result);
            }
          }

        }
      } catch (SQLException ex) {
        logger.error("caught SQLException " + ex.getMessage());
        throw new WsfServiceException(ex);
      } finally {
        // try {
        // rs.close();
        // } catch (SQLException ex) {
        // logger.info("caught SQLException " + ex.getMessage());
        // throw new WsfServiceException(ex);
        // }
      }
    } finally {
      SqlUtils.closeStatement(validationQuery);
    }
    // Map<String, SearchResult> otherCommentMatches = new HashMap<String,
    // SearchResult>();
    return newCommentResults;
  }

  @Override
  protected String[] defineContextKeys() {
    return new String[] { CTX_CONTAINER_APP, CTX_CONTAINER_COMMENT };
  }
}
