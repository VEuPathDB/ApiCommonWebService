package org.apidb.apicomplexa.wsfplugin.spanlogic;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.sql.DataSource;

import org.gusdb.fgputil.db.SqlUtils;
import org.gusdb.fgputil.db.platform.DBPlatform;
import org.gusdb.wdk.controller.CConstants;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.answer.AnswerValue;
import org.gusdb.wdk.model.jspwrap.WdkModelBean;
import org.gusdb.wdk.model.user.User;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.WsfResponse;
import org.gusdb.wsf.plugin.WsfServiceException;

public class SpanCompositionPlugin extends AbstractPlugin {

  private static class Feature {

    private static NumberFormat format = NumberFormat.getIntegerInstance();

    public String sourceId;
    public String projectId;
    public int begin;
    public int end;
    public int weight;
    public boolean reversed;
    public List<Feature> matched = new ArrayList<Feature>();

    public String getBegin() {
      return format.format(begin);
    }

    public String getEnd() {
      return format.format(end);
    }

    public String getReversed() {
      return reversed ? "-" : "+";
    }
  }

  public static final String COLUMN_SOURCE_ID = "source_id";
  public static final String COLUMN_PROJECT_ID = "project_id";
  public static final String COLUMN_WDK_WEIGHT = "wdk_weight";
  public static final String COLUMN_FEATURE_REGION = "feature_region";
  public static final String COLUMN_MATCHED_COUNT = "matched_count";
  public static final String COLUMN_MATCHED_REGIONS = "matched_regions";

  public static final String PARAM_OPERATION = "span_operation";
  public static final String PARAM_STRAND = "span_strand";
  public static final String PARAM_OUTPUT = "span_output";
  public static final String PARAM_SPAN_PREFIX = "span_";
  public static final String PARAM_BEGIN_PREFIX = "span_begin_";
  public static final String PARAM_BEGIN_DIRECTION_PREFIX = "span_begin_direction_";
  public static final String PARAM_BEGIN_OFFSET_PREFIX = "span_begin_offset_";
  public static final String PARAM_END_PREFIX = "span_end_";
  public static final String PARAM_END_DIRECTION_PREFIX = "span_end_direction_";
  public static final String PARAM_END_OFFSET_PREFIX = "span_end_offset_";

  // values for span_operation
  public static final String PARAM_VALUE_OVERLAP = "overlap";
  public static final String PARAM_VALUE_A_CONTAIN_B = "a_contain_b";
  public static final String PARAM_VALUE_B_CONTAIN_A = "b_contain_a";

  // values for begin/end
  public static final String PARAM_VALUE_START = "start";
  public static final String PARAM_VALUE_STOP = "stop";

  // values for span_output
  public static final String PARAM_VALUE_OUTPUT_A = "a";
  public static final String PARAM_VALUE_OUTPUT_B = "b";

  // values for begin/end_direction
  public static final String PARAM_VALUE_UPSTREAM = "-";
  public static final String PARAM_VALUE_DOWNSTREAM = "+";

  // values for span_strand
  public static final String PARAM_VALUE_BOTH_STRANDS = "both_strands";
  public static final String PARAM_VALUE_SAME_STRAND = "same_strand";
  public static final String PARAM_VALUE_OPPOSITE_STRANDS = "opposite_strands";

  private static final String TEMP_TABLE_PREFIX = "spanlogic";

  private final Random random = new Random();

  @Override
  public String[] getColumns() {
    return new String[] { COLUMN_PROJECT_ID, COLUMN_SOURCE_ID,
        COLUMN_WDK_WEIGHT, COLUMN_FEATURE_REGION, COLUMN_MATCHED_COUNT,
        COLUMN_MATCHED_REGIONS };
  }

  @Override
  public String[] getRequiredParameterNames() {
    return new String[] { PARAM_OPERATION, PARAM_SPAN_PREFIX + "a",
        PARAM_SPAN_PREFIX + "b" };
  }

  @Override
  public void validateParameters(PluginRequest request) throws WsfServiceException {
    Map<String, String> params = request.getParams();
    Set<String> operators = new HashSet<String>(Arrays.asList(
        PARAM_VALUE_OVERLAP, PARAM_VALUE_A_CONTAIN_B, PARAM_VALUE_B_CONTAIN_A));
    Set<String> outputs = new HashSet<String>(Arrays.asList(
        PARAM_VALUE_OUTPUT_A, PARAM_VALUE_OUTPUT_B));
    Set<String> strands = new HashSet<String>(Arrays.asList(
        PARAM_VALUE_BOTH_STRANDS, PARAM_VALUE_SAME_STRAND,
        PARAM_VALUE_OPPOSITE_STRANDS));
    Set<String> anchors = new HashSet<String>(Arrays.asList(PARAM_VALUE_START,
        PARAM_VALUE_STOP));
    Set<String> directions = new HashSet<String>(Arrays.asList(
        PARAM_VALUE_DOWNSTREAM, PARAM_VALUE_UPSTREAM));

    // validate operator
    if (params.containsKey(PARAM_OPERATION)) {
      String op = params.get(PARAM_OPERATION);
      if (!operators.contains(op))
        throw new WsfServiceException("Invalid " + PARAM_OPERATION + ": " + op);
    }

    // validate output choice
    if (params.containsKey(PARAM_OUTPUT)) {
      String out = params.get(PARAM_OUTPUT);
      if (!outputs.contains(out))
        throw new WsfServiceException("Invalid " + PARAM_OUTPUT + ": " + out);
    }

    // validate strand
    if (params.containsKey(PARAM_STRAND)) {
      String strand = params.get(PARAM_STRAND);
      if (!strands.contains(strand))
        throw new WsfServiceException("Invalid " + PARAM_STRAND + ": " + strand);
    }

    // validate begin a
    validateAnchorParams(params, anchors, PARAM_BEGIN_PREFIX + "a");
    validateDirectionParams(params, directions, PARAM_BEGIN_DIRECTION_PREFIX
        + "a");
    validateOffsetParams(params, PARAM_BEGIN_OFFSET_PREFIX + "a");

    // validate end a
    validateAnchorParams(params, anchors, PARAM_END_PREFIX + "a");
    validateDirectionParams(params, directions, PARAM_END_DIRECTION_PREFIX
        + "a");
    validateOffsetParams(params, PARAM_END_OFFSET_PREFIX + "a");

    // validate begin b
    validateAnchorParams(params, anchors, PARAM_BEGIN_PREFIX + "b");
    validateDirectionParams(params, directions, PARAM_BEGIN_DIRECTION_PREFIX
        + "b");
    validateOffsetParams(params, PARAM_BEGIN_OFFSET_PREFIX + "b");

    // validate end b
    validateAnchorParams(params, anchors, PARAM_END_PREFIX + "b");
    validateDirectionParams(params, directions, PARAM_END_DIRECTION_PREFIX
        + "a");
    validateOffsetParams(params, PARAM_END_OFFSET_PREFIX + "b");
  }

  private void validateAnchorParams(Map<String, String> params,
      Set<String> anchors, String param) throws WsfServiceException {
    if (params.containsKey(param)) {
      String anchor = params.get(param).intern();
      if (!anchors.contains(anchor))
        throw new WsfServiceException("Invalid " + param + ": " + anchor);
    }
  }

  private void validateDirectionParams(Map<String, String> params,
      Set<String> directions, String param) throws WsfServiceException {
    if (params.containsKey(param)) {
      String direction = params.get(param).intern();
      if (!directions.contains(direction))
        throw new WsfServiceException("Invalid " + param + ": " + direction);
    }
  }

  private void validateOffsetParams(Map<String, String> params, String param)
      throws WsfServiceException {
    if (params.containsKey(param)) {
      String offset = params.get(param);
      try {
        Integer.parseInt(offset);
      } catch (NumberFormatException ex) {
        throw new WsfServiceException("Invalid " + param
            + " (expected number): " + offset);
      }
    }
  }

  @Override
  public WsfResponse execute(PluginRequest request) throws WsfServiceException {
    Map<String, String> params = request.getParams();
    String operation = params.get(PARAM_OPERATION);

    String output = params.get(PARAM_OUTPUT);
    if (output == null || !output.equalsIgnoreCase(PARAM_VALUE_OUTPUT_B))
      output = PARAM_VALUE_OUTPUT_A;

    String strand = params.get(PARAM_STRAND);
    if (strand == null)
      strand = PARAM_VALUE_BOTH_STRANDS;

    // get the proper begin & end of the derived regions.
    String[] startStopA = getStartStop(params, "a");
    String[] startStopB = getStartStop(params, "b");

    WdkModelBean wdkModelBean = (WdkModelBean) this.context.get(CConstants.WDK_MODEL_KEY);
    WdkModel wdkModel = wdkModelBean.getModel();
    String tempA = null, tempB = null;
    try {
      // get the answerValue from the step id
      String signature = request.getContext().get(Utilities.QUERY_CTX_USER);
      User user = wdkModel.getUserFactory().getUser(signature);

      // create temp tables from caches
      tempA = getSpanSql(wdkModel, user, params, startStopA, "a");
      tempB = getSpanSql(wdkModel, user, params, startStopB, "b");

      // compose the final sql by comparing two regions with span
      // operation.
      String sql = composeSql(request.getProjectId(), operation, tempA, tempB,
          strand);

      logger.debug("SPAN LOGIC SQL:\n" + sql);

      // execute the final sql, and fetch the result for the output.
      WsfResponse response = getResult(wdkModel, sql, request.getOrderedColumns(), output);
      
      // drop the cache tables
      DBPlatform platform = wdkModel.getAppDb().getPlatform();
      DataSource dataSource = wdkModel.getAppDb().getDataSource();
      String schema = wdkModel.getAppDb().getDefaultSchema();
      platform.dropTable(dataSource, schema, tempA, true);
      platform.dropTable(dataSource, schema, tempB, true);
      
      return response;
    } catch (Exception ex) {
      throw new WsfServiceException(ex);
    } finally {
      // dropTempTables(wdkModel, tempA, tempB);
    }
  }

  private String[] getStartStop(Map<String, String> params, String suffix) {
    // get the user's choice of begin & end, and the offsets from params.
    String begin = params.get(PARAM_BEGIN_PREFIX + suffix);
    if (begin == null)
      begin = PARAM_VALUE_START;
    String end = params.get(PARAM_END_PREFIX + suffix);
    if (end == null)
      end = PARAM_VALUE_STOP;

    String beginDir = params.get(PARAM_BEGIN_DIRECTION_PREFIX + suffix);
    if (beginDir == null)
      beginDir = PARAM_VALUE_UPSTREAM;
    String endDir = params.get(PARAM_END_DIRECTION_PREFIX + suffix);
    if (endDir == null)
      endDir = PARAM_VALUE_UPSTREAM;

    int beginOff = 0;
    if (params.containsKey(PARAM_BEGIN_OFFSET_PREFIX + suffix))
      beginOff = Integer.valueOf(params.get(PARAM_BEGIN_OFFSET_PREFIX + suffix));
    int endOff = 0;
    if (params.containsKey(PARAM_END_OFFSET_PREFIX + suffix))
      endOff = Integer.valueOf(params.get(PARAM_END_OFFSET_PREFIX + suffix));

    if (beginDir.equals(PARAM_VALUE_UPSTREAM))
      beginOff *= -1;
    if (endDir.equals(PARAM_VALUE_UPSTREAM))
      endOff *= -1;

    String table = "fl.";

    // depending on whether the feature is on forward or reversed strand,
    // and user's choice of begin and end, we get the proper begin of the
    // region.
    StringBuilder sql = new StringBuilder("(CASE ");
    sql.append(" WHEN NVL(" + table + "is_reversed, 0) = 0 THEN (");
    sql.append(begin.equals(PARAM_VALUE_START) ? "start_min" : "end_max");
    sql.append(" + 1*(" + beginOff + ")) ");
    sql.append(" WHEN NVL(" + table + "is_reversed, 0) = 1 THEN (");
    sql.append(end.equals(PARAM_VALUE_START) ? "end_max" : "start_min");
    sql.append(" - 1*(" + endOff + ")) ");
    sql.append("END)");
    String start = sql.toString();

    // we get the proper end of the region.
    sql = new StringBuilder("(CASE ");
    sql.append(" WHEN NVL(" + table + "is_reversed, 0) = 0 THEN (");
    sql.append(end.equals(PARAM_VALUE_START) ? "start_min" : "end_max");
    sql.append(" + 1*(" + endOff + ")) ");
    sql.append(" WHEN NVL(" + table + "is_reversed, 0) = 1 THEN (");
    sql.append(begin.equals(PARAM_VALUE_START) ? "end_max" : "start_min");
    sql.append(" - 1*(" + beginOff + ")) ");
    sql.append("END)");
    String stop = sql.toString();

    return new String[] { start, stop };
  }

  private String composeSql(String projectId, String operation,
      String tempTableA, String tempTableB, String strand) {
    StringBuilder builder = new StringBuilder();

    // determine the output type
    builder.append("SELECT fa.source_id AS source_id_a, ");
    builder.append("       fa.project_id AS project_id_a, ");
    builder.append("       fa.wdk_weight AS wdk_weight_a, ");
    builder.append("       fa.begin AS begin_a, fa.end AS end_a, ");
    builder.append("       fa.is_reversed AS is_reversed_a, ");
    builder.append("       fb.source_id AS source_id_b, ");
    builder.append("       fb.project_id AS project_id_b, ");
    builder.append("       fb.wdk_weight AS wdk_weight_b, ");
    builder.append("       fb.begin AS begin_b, fb.end AS end_b, ");
    builder.append("       fb.is_reversed AS is_reversed_b ");
    builder.append("FROM (" + tempTableA + ") fa, (" + tempTableB + ") fb ");

    // make sure the regions come from sequence source.
    builder.append("WHERE fa.sequence_source_id = fb.sequence_source_id ");

    // restrict the regions to have start_min <= end_max
    builder.append("  AND fa.begin <= fa.end ");
    builder.append("  AND fb.begin <= fb.end ");

    // check the strand choice
    if (strand.equalsIgnoreCase(PARAM_VALUE_SAME_STRAND)) {
      builder.append("  AND fa.is_reversed = fb.is_reversed ");
    } else if (strand.equalsIgnoreCase(PARAM_VALUE_OPPOSITE_STRANDS)) {
      builder.append("  AND fa.is_reversed != fb.is_reversed ");
    }

    // apply span operation.
    if (operation.equals(PARAM_VALUE_OVERLAP)) {
      builder.append("  AND fa.begin <= fb.end ");
      builder.append("  AND fa.end >= fb.begin ");
    } else if (operation.equals(PARAM_VALUE_A_CONTAIN_B)) {
      builder.append("  AND fa.begin <= fb.begin ");
      builder.append("  AND fa.end >= fb.end ");
    } else { // b_contain_a
      builder.append("  AND fa.begin >= fb.begin ");
      builder.append("  AND fa.end <= fb.end ");
    }

    return builder.toString();
  }

  private String getSpanSql(WdkModel wdkModel, User user,
      Map<String, String> params, String[] region, String suffix)
      throws WdkModelException {
    int stepId = Integer.parseInt(params.get(PARAM_SPAN_PREFIX + suffix));
    AnswerValue answerValue = user.getStep(stepId).getAnswerValue();

    // get the sql to the cache table
    String cacheSql = "(" + answerValue.getIdSql() + ")";

    // get the table or sql that returns the location information
    String rcName = answerValue.getQuestion().getRecordClass().getFullName();
    String locTable;
    if (rcName.equals("DynSpanRecordClasses.DynSpanRecordClass")) {
      locTable = "(SELECT source_id AS feature_source_id, project_id, "
          + "        regexp_substr(source_id, '[^:]+', 1, 1) as sequence_source_id, "
          + "        regexp_substr(regexp_substr(source_id, '[^:]+', 1, 2), '[^\\-]+', 1,1) as start_min, "
          + "        regexp_substr(regexp_substr(source_id, '[^:]+', 1, 2), '[^\\-]+', 1,2) as end_max, "
          + "        DECODE(regexp_substr(source_id, '[^:]+', 1, 3), 'r', 1, 0) AS is_reversed, "
          + "        1 AS is_top_level, 'DynamicSpanFeature' AS feature_type "
          + "  FROM " + cacheSql + ")";
    } else {
      locTable = "ApidbTuning.FeatureLocation";
    }

    // get a temp table name
    DBPlatform platform = wdkModel.getAppDb().getPlatform();
    DataSource dataSource = wdkModel.getAppDb().getDataSource();
    String schema = wdkModel.getAppDb().getDefaultSchema();
    try {
      String tableName = null;
      while (true) {
        tableName = TEMP_TABLE_PREFIX + random.nextInt(Integer.MAX_VALUE);
        if (!platform.checkTableExists(dataSource, schema, tableName))
          break;
      }

      StringBuilder builder = new StringBuilder();
      builder.append("CREATE TABLE " + tableName + " AS ");
      builder.append("SELECT DISTINCT fl.feature_source_id AS source_id, ");
      builder.append("       fl.sequence_source_id, fl.feature_type, ");
      builder.append("       ca.wdk_weight, ca.project_id, ");
      builder.append("       NVL(fl.is_reversed, 0) AS is_reversed, ");
      builder.append("   " + region[0] + " AS begin, " + region[1] + " AS end ");
      builder.append("FROM " + locTable + " fl, " + cacheSql + " ca ");
      builder.append("WHERE fl.feature_source_id = ca.source_id ");
      builder.append("  AND fl.is_top_level = 1");
      builder.append("  AND fl.feature_type = (");
      builder.append("    SELECT fl.feature_type ");
      builder.append("    FROM " + locTable + " fl, " + cacheSql + " ca");
      builder.append("    WHERE fl.feature_source_id = ca.source_id ");
      builder.append("      AND rownum = 1) ");

      String sql = builder.toString();
      logger.debug("SPAN SQL: " + sql);

      // cache the sql
      SqlUtils.executeUpdate(dataSource, sql, "span-logic-child");

      return tableName;
    } catch (SQLException ex) {
      throw new WdkModelException(ex);
    }
  }

  private WsfResponse getResult(WdkModel wdkModel, String sql,
      String[] orderedColumns, String output) throws SQLException {
    List<Map<String, String>> results = prepareResults(wdkModel, sql, output);
    List<String[]> records = new ArrayList<String[]>();
    for (Map<String, String> result : results) {
      String[] record = new String[orderedColumns.length];
      for (int i = 0; i < record.length; i++) {
        if (result.get(orderedColumns[i]) == null)
          logger.error("No value match for column: " + orderedColumns[i]);
        record[i] = result.get(orderedColumns[i]).toString();
      }
      records.add(record);
    }

    // now copy the records into an array
    String[][] array = new String[records.size()][orderedColumns.length];
    for (int i = 0; i < array.length; i++) {
      String[] record = records.get(i);
      System.arraycopy(record, 0, array[i], 0, record.length);
    }

    WsfResponse wsfResult = new WsfResponse();
    wsfResult.setResult(array);
    return wsfResult;
  }

  private List<Map<String, String>> prepareResults(WdkModel wdkModel,
      String sql, String output) throws SQLException {
    DataSource dataSource = wdkModel.getAppDb().getDataSource();
    ResultSet results = null;

    try {
      results = SqlUtils.executeQuery(dataSource, sql, "span-logic-cached");
      Map<String, Feature> fas = new LinkedHashMap<String, Feature>();
      Map<String, Feature> fbs = new LinkedHashMap<String, Feature>();
      while (results.next()) {
        // get feature a
        String sourceIdA = results.getString("source_id_a");
        Feature a = fas.get(sourceIdA);
        if (a == null) {
          a = new Feature();
          a.sourceId = sourceIdA;
          a.projectId = results.getString("project_id_a");
          a.begin = results.getInt("begin_a");
          a.end = results.getInt("end_a");
          a.weight = results.getInt("wdk_weight_a");
          a.reversed = results.getBoolean("is_reversed_a");
          fas.put(sourceIdA, a);
        }

        // get feature b
        String sourceIdB = results.getString("source_id_b");
        Feature b = fbs.get(sourceIdB);
        if (b == null) {
          b = new Feature();
          b.sourceId = sourceIdB;
          b.projectId = results.getString("project_id_b");
          b.begin = results.getInt("begin_b");
          b.end = results.getInt("end_b");
          b.weight = results.getInt("wdk_weight_b");
          b.reversed = results.getBoolean("is_reversed_b");
          fbs.put(sourceIdB, b);
        }

        a.matched.add(b);
        b.matched.add(a);
      }

      // now format the features int map
      Map<String, Feature> fs = output.equals("a") ? fas : fbs;
      List<Map<String, String>> records = new ArrayList<Map<String, String>>();
      for (Feature feature : fs.values()) {
        Map<String, String> record = new LinkedHashMap<String, String>();
        record.put(COLUMN_SOURCE_ID, feature.sourceId);
        record.put(COLUMN_PROJECT_ID, feature.projectId);
        String region = feature.getBegin() + "&nbsp;-&nbsp;" + feature.getEnd()
            + "&nbsp;(" + feature.getReversed() + ")";
        record.put(COLUMN_FEATURE_REGION, region);
        record.put(COLUMN_MATCHED_COUNT, "" + feature.matched.size());
        record.put(COLUMN_WDK_WEIGHT, "" + feature.weight);

        StringBuilder builder = new StringBuilder();
        for (Feature fr : feature.matched) {
          if (builder.length() > 0)
            builder.append("; ");
          builder.append(fr.sourceId + ":&nbsp;" + fr.getBegin()
              + "&nbsp;-&nbsp;" + fr.getEnd() + "&nbsp;(" + fr.getReversed()
              + ")");
        }
        String regions = builder.toString();
        if (regions.length() > 4000)
          regions = regions.substring(0, 3998) + "...";
        record.put(COLUMN_MATCHED_REGIONS, regions);
        records.add(record);
      }
      return records;
    } finally {
      SqlUtils.closeResultSetAndStatement(results);
    }
  }

  @Override
  protected String[] defineContextKeys() {
    return new String[] { CConstants.WDK_MODEL_KEY };
  }
}
