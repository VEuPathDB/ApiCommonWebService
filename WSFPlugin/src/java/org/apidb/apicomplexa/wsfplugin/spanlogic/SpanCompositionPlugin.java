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

import org.apache.log4j.Logger;
import org.apidb.apicommon.model.TranscriptUtil;
import org.gusdb.fgputil.db.SqlUtils;
import org.gusdb.fgputil.db.platform.DBPlatform;
import org.gusdb.fgputil.runtime.InstanceManager;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.WdkUserException;
import org.gusdb.wdk.model.answer.AnswerValue;
import org.gusdb.wdk.model.user.User;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;

public class SpanCompositionPlugin extends AbstractPlugin {

  private static final Logger logger = Logger.getLogger(SpanCompositionPlugin.class);

  private static class Feature {

    private static NumberFormat format = NumberFormat.getIntegerInstance();

    public String sourceId;
    public String geneSourceId;
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

    /**
     * format the region of the feature
     * 
     * @return
     */
    public String getRegion() {
      StringBuilder buffer = new StringBuilder();
      buffer.append(begin).append("&nbsp;-&nbsp;").append(end);
      buffer.append("&nbsp;(").append(getReversed()).append(")");
      return buffer.toString();
    }

  }

  private static class Flag {
    private boolean hasSnp = false;
  }

  public static final String COLUMN_SOURCE_ID = "source_id";
  public static final String COLUMN_GENE_SOURCE_ID = "gene_source_id";
  public static final String COLUMN_PROJECT_ID = "project_id";
  public static final String COLUMN_WDK_WEIGHT = "wdk_weight";
  public static final String COLUMN_FEATURE_REGION = "feature_region";
  public static final String COLUMN_MATCHED_COUNT = "matched_count";
  public static final String COLUMN_MATCHED_REGIONS = "matched_regions";
  public static final String COLUMN_MATCHED_RESULT = "matched_result";

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
    return new String[] { COLUMN_PROJECT_ID, COLUMN_SOURCE_ID, COLUMN_GENE_SOURCE_ID, COLUMN_WDK_WEIGHT, COLUMN_FEATURE_REGION,
			  COLUMN_MATCHED_COUNT, COLUMN_MATCHED_REGIONS, COLUMN_MATCHED_RESULT };
  }

  @Override
  public String[] getRequiredParameterNames() {
    return new String[] { PARAM_OPERATION, PARAM_SPAN_PREFIX + "a", PARAM_SPAN_PREFIX + "b" };
  }

  @Override
  public void validateParameters(PluginRequest request) throws PluginUserException {
    Map<String, String> params = request.getParams();
    Set<String> operators = new HashSet<String>(Arrays.asList(PARAM_VALUE_OVERLAP, PARAM_VALUE_A_CONTAIN_B,
        PARAM_VALUE_B_CONTAIN_A));
    Set<String> outputs = new HashSet<String>(Arrays.asList(PARAM_VALUE_OUTPUT_A, PARAM_VALUE_OUTPUT_B));
    Set<String> strands = new HashSet<String>(Arrays.asList(PARAM_VALUE_BOTH_STRANDS,
        PARAM_VALUE_SAME_STRAND, PARAM_VALUE_OPPOSITE_STRANDS));
    Set<String> anchors = new HashSet<String>(Arrays.asList(PARAM_VALUE_START, PARAM_VALUE_STOP));
    Set<String> directions = new HashSet<String>(Arrays.asList(PARAM_VALUE_DOWNSTREAM, PARAM_VALUE_UPSTREAM));

    // validate operator
    if (params.containsKey(PARAM_OPERATION)) {
      String op = params.get(PARAM_OPERATION);
      if (!operators.contains(op))
        throw new PluginUserException("Invalid " + PARAM_OPERATION + ": " + op);
    }

    // validate output choice
    if (params.containsKey(PARAM_OUTPUT)) {
      String out = params.get(PARAM_OUTPUT);
      if (!outputs.contains(out))
        throw new PluginUserException("Invalid " + PARAM_OUTPUT + ": " + out);
    }

    // validate strand
    if (params.containsKey(PARAM_STRAND)) {
      String strand = params.get(PARAM_STRAND);
      if (!strands.contains(strand))
        throw new PluginUserException("Invalid " + PARAM_STRAND + ": " + strand);
    }

    // validate begin a
    validateAnchorParams(params, anchors, PARAM_BEGIN_PREFIX + "a");
    validateDirectionParams(params, directions, PARAM_BEGIN_DIRECTION_PREFIX + "a");
    validateOffsetParams(params, PARAM_BEGIN_OFFSET_PREFIX + "a");

    // validate end a
    validateAnchorParams(params, anchors, PARAM_END_PREFIX + "a");
    validateDirectionParams(params, directions, PARAM_END_DIRECTION_PREFIX + "a");
    validateOffsetParams(params, PARAM_END_OFFSET_PREFIX + "a");

    // validate begin b
    validateAnchorParams(params, anchors, PARAM_BEGIN_PREFIX + "b");
    validateDirectionParams(params, directions, PARAM_BEGIN_DIRECTION_PREFIX + "b");
    validateOffsetParams(params, PARAM_BEGIN_OFFSET_PREFIX + "b");

    // validate end b
    validateAnchorParams(params, anchors, PARAM_END_PREFIX + "b");
    validateDirectionParams(params, directions, PARAM_END_DIRECTION_PREFIX + "a");
    validateOffsetParams(params, PARAM_END_OFFSET_PREFIX + "b");
  }

  private void validateAnchorParams(Map<String, String> params, Set<String> anchors, String param)
      throws PluginUserException {
    if (params.containsKey(param)) {
      String anchor = params.get(param).intern();
      if (!anchors.contains(anchor))
        throw new PluginUserException("Invalid " + param + ": " + anchor);
    }
  }

  private void validateDirectionParams(Map<String, String> params, Set<String> directions, String param)
      throws PluginUserException {
    if (params.containsKey(param)) {
      String direction = params.get(param).intern();
      if (!directions.contains(direction))
        throw new PluginUserException("Invalid " + param + ": " + direction);
    }
  }

  private void validateOffsetParams(Map<String, String> params, String param) throws PluginUserException {
    if (params.containsKey(param)) {
      String offset = params.get(param);
      try {
        Integer.parseInt(offset);
      }
      catch (NumberFormatException ex) {
        throw new PluginUserException("Invalid " + param + " (expected number): " + offset);
      }
    }
  }

  @Override
  public int execute(PluginRequest request, PluginResponse response) throws PluginModelException {
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

    String tempA = null, tempB = null;
    try {
      WdkModel wdkModel = InstanceManager.getInstance(WdkModel.class, request.getProjectId());
      // get the answerValue from the step id
      String signature = request.getContext().get(Utilities.QUERY_CTX_USER);
      User user = wdkModel.getUserFactory().getUser(signature);

      // create temp tables from caches
      Flag flag = new Flag();
      tempA = getSpanSql(wdkModel, user, params, startStopA, "a", flag);
      tempB = getSpanSql(wdkModel, user, params, startStopB, "b", flag);

      // compose the final sql by comparing two regions with span
      // operation.
      String sql = composeSql(request.getProjectId(), operation, tempA, tempB, strand, output, flag);

      logger.debug("SPAN LOGIC SQL:\n" + sql);

      // execute the final sql, and fetch the result for the output.
      prepareResult(wdkModel, response, sql, request.getOrderedColumns(), output);

      // drop the cache tables
      DBPlatform platform = wdkModel.getAppDb().getPlatform();
      DataSource dataSource = wdkModel.getAppDb().getDataSource();
      String schema = wdkModel.getAppDb().getDefaultSchema();
      platform.dropTable(dataSource, schema, tempA, true);
      platform.dropTable(dataSource, schema, tempB, true);

      return 0;
    }
    catch (Exception ex) {
      throw new PluginModelException(ex);
    }
    finally {
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

  private String composeSql(String projectId, String operation, String tempTableA, String tempTableB,
      String strand, String output, Flag flag) {
    StringBuilder builder = new StringBuilder();

    // determine the output type
    builder.append("SELECT fa.source_id AS source_id_a, ");
    builder.append("       fa.gene_source_id AS gene_source_id_a, ");
    builder.append("       fa.project_id AS project_id_a, ");
    builder.append("       fa.wdk_weight AS wdk_weight_a, ");
    builder.append("       fa.begin AS begin_a, fa.end AS end_a, ");
    builder.append("       fa.is_reversed AS is_reversed_a, ");
    builder.append("       fb.source_id AS source_id_b, ");
    builder.append("       fb.gene_source_id AS gene_source_id_b, ");
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
    if (!flag.hasSnp) {
      if (strand.equalsIgnoreCase(PARAM_VALUE_SAME_STRAND)) {
        builder.append("  AND fa.is_reversed = fb.is_reversed ");
      }
      else if (strand.equalsIgnoreCase(PARAM_VALUE_OPPOSITE_STRANDS)) {
        builder.append("  AND fa.is_reversed != fb.is_reversed ");
      }
    }

    // apply span operation.
    if (operation.equals(PARAM_VALUE_OVERLAP)) {
      builder.append("  AND fa.begin <= fb.end ");
      builder.append("  AND fa.end >= fb.begin ");
    }
    else if (operation.equals(PARAM_VALUE_A_CONTAIN_B)) {
      builder.append("  AND fa.begin <= fb.begin ");
      builder.append("  AND fa.end >= fb.end ");
    }
    else { // b_contain_a
      builder.append("  AND fa.begin >= fb.begin ");
      builder.append("  AND fa.end <= fb.end ");
    }

    // sort the result by output records
    builder.append("ORDER BY f" + output + ".project_id ASC, ");
    builder.append("         f" + output + ".source_id ASC ");

    return builder.toString();
  }

  private String getSpanSql(WdkModel wdkModel, User user, Map<String, String> params, String[] region,
      String suffix, Flag flag) throws WdkModelException, WdkUserException {
    int stepId = Integer.parseInt(params.get(PARAM_SPAN_PREFIX + suffix));
    AnswerValue answerValue = user.getStep(stepId).getAnswerValue();

    // get the sql to the cache table
    String cacheSql = "(" + answerValue.getIdSql() + ")";

    // get the table or sql that returns the location information
    String rcName = answerValue.getQuestion().getRecordClass().getFullName();
    String locTable;
    if (rcName.equals("DynSpanRecordClasses.DynSpanRecordClass")) {
      locTable = "(SELECT source_id AS feature_source_id, project_id, " +
          "        regexp_substr(source_id, '[^:]+', 1, 1) as sequence_source_id, " +
          "        regexp_substr(regexp_substr(source_id, '[^:]+', 1, 2), '[^\\-]+', 1,1) as start_min, " +
          "        regexp_substr(regexp_substr(source_id, '[^:]+', 1, 2), '[^\\-]+', 1,2) as end_max, " +
          "        DECODE(regexp_substr(source_id, '[^:]+', 1, 3), 'r', 1, 0) AS is_reversed, " +
          "        1 AS is_top_level, 'DynamicSpanFeature' AS feature_type " + "  FROM " + cacheSql + ")";
    }
    else if (rcName.equals("SnpRecordClasses.SnpRecordClass")) {
      flag.hasSnp = true;
      String projectId = wdkModel.getProjectId();
      locTable = "(SELECT sn.source_id AS feature_source_id, '" + projectId + "' AS project_id, " +
          "      sa.source_id AS sequence_source_id, sn.location AS start_min, sn.location AS end_max, " +
          "      0 AS is_reversed, 1 AS is_top_level, 'SnpFeature' AS feature_type " +
          " FROM Apidb.Snp sn, ApidbTuning.GenomicSeqAttributes sa " +
          " WHERE sn.na_sequence_id = sa.na_sequence_id)";
    }
    else {
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

      String sql = rcName.equals(TranscriptUtil.TRANSCRIPT_RECORDCLASS)
          ? getTranscriptSpanSql(tableName, region, cacheSql)
          : getStandardSpanSql(tableName, region, locTable, cacheSql);
      logger.debug("SPAN SQL: " + sql);

      // cache the sql
      SqlUtils.executeUpdate(dataSource, sql, "span-logic-child");

      return tableName;
    }
    catch (SQLException ex) {
      throw new WdkModelException(ex);
    }
  }

  private String getStandardSpanSql(String tableName, String[] region, String locTable, String cacheSql ) {
    StringBuilder builder = new StringBuilder();
    builder.append("CREATE TABLE " + tableName + " AS ");
    builder.append("SELECT DISTINCT fl.feature_source_id AS source_id, 'dontcare' as gene_source_id, ");
    builder.append("       fl.sequence_source_id, fl.feature_type, ");
    builder.append("       ca.wdk_weight, ca.project_id, ");
    builder.append("       NVL(fl.is_reversed, 0) AS is_reversed, ");
    builder.append("   " + region[0] + " AS begin, " + region[1] + " AS end ");
    builder.append("FROM " + locTable + " fl, " + cacheSql + " ca ");
    builder.append("WHERE fl.feature_source_id = ca.source_id");
    builder.append("  AND fl.is_top_level = 1");
    builder.append("  AND fl.feature_type = (");
    builder.append("    SELECT fl.feature_type ");
    builder.append("    FROM " + locTable + " fl, " + cacheSql + " ca");
    builder.append("    WHERE fl.feature_source_id = ca.source_id");
    builder.append("      AND rownum = 1) ");

    return builder.toString();
    
  }
  
  private String getTranscriptSpanSql(String tableName, String[] region, String cacheSql ) {
    StringBuilder builder = new StringBuilder();
    builder.append("CREATE TABLE " + tableName + " AS ");
    builder.append("SELECT DISTINCT ca.source_id, ca.gene_source_id, ");
    builder.append("       fl.sequence_source_id, fl.feature_type, ");
    builder.append("       ca.wdk_weight, ca.project_id, ");
    builder.append("       NVL(fl.is_reversed, 0) AS is_reversed, ");
    builder.append("   " + region[0] + " AS begin, " + region[1] + " AS end ");
    builder.append("FROM ApidbTuning.FeatureLocation fl, " + cacheSql + " ca ");
    builder.append("WHERE fl.feature_source_id = ca.gene_source_id");
    builder.append("  AND fl.is_top_level = 1");
    builder.append("  AND fl.feature_type = (");
    builder.append("    SELECT fl.feature_type ");
    builder.append("    FROM ApidbTuning.FeatureLocation fl, " + cacheSql + " ca");
    builder.append("    WHERE fl.feature_source_id = ca.gene_source_id");
    builder.append("      AND rownum = 1) ");
    return builder.toString();
    
  }
  
  private void prepareResult(WdkModel wdkModel, PluginResponse response, String sql, String[] orderedColumns,
      String output) throws SQLException, PluginModelException, PluginUserException {
    // prepare column order
    Map<String, Integer> columnOrders = new LinkedHashMap<>(orderedColumns.length);
    for (int i = 0; i < orderedColumns.length; i++) {
      columnOrders.put(orderedColumns[i], i);
    }

    // read results
    DataSource dataSource = wdkModel.getAppDb().getDataSource();
    ResultSet resultSet = null;
    try {
      resultSet = SqlUtils.executeQuery(dataSource, sql, "span-logic-cached");
      Feature feature = null;
      String ref = output.equals("a") ? "b" : "a";
      while (resultSet.next()) {
        String sourceId = resultSet.getString("source_id_" + output);
        // the result is sorted by the ids of the output result
        if (feature == null) {
          // reading the first line
          feature = new Feature();
        }
        else if (!feature.sourceId.equals(sourceId)) {
          // start on a new record, output the previous feature
          writeFeature(response, columnOrders, feature);
          feature = new Feature();
        }
        // store info into feature
        readFeature(resultSet, feature, output);

        // read the reference
        Feature reference = new Feature();
        readFeature(resultSet, reference, ref);
        feature.matched.add(reference);
      }
      if (feature != null) { // write the last feature
        writeFeature(response, columnOrders, feature);
      }
    }
    finally {
      SqlUtils.closeResultSetAndStatement(resultSet, null);
    }
  }

  private void writeFeature(PluginResponse response, Map<String, Integer> columnOrders, Feature feature)
      throws PluginModelException, PluginUserException {
    // format the matched regions
    StringBuilder builder = new StringBuilder();
    for (Feature fr : feature.matched) {
      if (builder.length() > 0)
        builder.append("; ");
      builder.append(fr.sourceId + ":&nbsp;" + fr.getBegin() + "&nbsp;-&nbsp;" + fr.getEnd() + "&nbsp;(" +
          fr.getReversed() + ")");
    }
    String matched = builder.toString();
    if (matched.length() > 4000)
      matched = matched.substring(0, 3997) + "...";

    // construct row by column orders
    String[] row = new String[columnOrders.size()];
    row[columnOrders.get(COLUMN_SOURCE_ID)] = feature.sourceId;
    row[columnOrders.get(COLUMN_GENE_SOURCE_ID)] = feature.geneSourceId;
    row[columnOrders.get(COLUMN_PROJECT_ID)] = feature.projectId;
    row[columnOrders.get(COLUMN_FEATURE_REGION)] = feature.getRegion();
    row[columnOrders.get(COLUMN_MATCHED_COUNT)] = Integer.toString(feature.matched.size());
    row[columnOrders.get(COLUMN_WDK_WEIGHT)] = Integer.toString(feature.weight);
    row[columnOrders.get(COLUMN_MATCHED_REGIONS)] = matched;
    row[columnOrders.get(COLUMN_MATCHED_RESULT)] = "Y";

    // save the row
    response.addRow(row);
  }

  private void readFeature(ResultSet resultSet, Feature feature, String suffix) throws SQLException {
    feature.sourceId = resultSet.getString("source_id_" + suffix);
    feature.geneSourceId = resultSet.getString("gene_source_id_" + suffix);
    feature.projectId = resultSet.getString("project_id_" + suffix);
    feature.begin = resultSet.getInt("begin_" + suffix);
    feature.end = resultSet.getInt("end_" + suffix);
    feature.weight = resultSet.getInt("wdk_weight_" + suffix);
    feature.reversed = resultSet.getBoolean("is_reversed_" + suffix);
  }
}
