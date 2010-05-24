package org.apidb.apicomplexa.wsfplugin.spanlogic;

import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.gusdb.wdk.controller.CConstants;
import org.gusdb.wdk.model.AnswerValue;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.WdkUserException;
import org.gusdb.wdk.model.dbms.SqlUtils;
import org.gusdb.wdk.model.jspwrap.WdkModelBean;
import org.gusdb.wdk.model.query.ProcessQueryInstance;
import org.gusdb.wdk.model.user.Step;
import org.gusdb.wdk.model.user.User;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.WsfRequest;
import org.gusdb.wsf.plugin.WsfResponse;
import org.gusdb.wsf.plugin.WsfServiceException;
import org.json.JSONException;

public class SpanCompositionPlugin extends AbstractPlugin {

    public static String COLUMN_SOURCE_ID = "source_id";
    public static String COLUMN_PROJECT_ID = "project_id";
    public static String COLUMN_WDK_WEIGHT = "wdk_weight";

    public static String PARAM_OPERATION = "span_operation";
    public static String PARAM_STRAND = "span_strand";
    public static String PARAM_OUTPUT = "span_output";
    public static String PARAM_SPAN_PREFIX = "span_";
    public static String PARAM_BEGIN_PREFIX = "span_begin_";
    public static String PARAM_BEGIN_DIRECTION_PREFIX = "span_begin_direction_";
    public static String PARAM_BEGIN_OFFSET_PREFIX = "span_begin_offset_";
    public static String PARAM_END_PREFIX = "span_end_";
    public static String PARAM_END_DIRECTION_PREFIX = "span_end_direction_";
    public static String PARAM_END_OFFSET_PREFIX = "span_end_offset_";

    // values for span_operation
    public static String PARAM_VALUE_OVERLAP = "overlap";
    public static String PARAM_VALUE_A_CONTAIN_B = "a_contain_b";
    public static String PARAM_VALUE_B_CONTAIN_A = "b_contain_a";

    // values for begin/end
    public static String PARAM_VALUE_START = "start";
    public static String PARAM_VALUE_STOP = "stop";

    // values for span_output
    public static String PARAM_VALUE_OUTPUT_A = "a";
    public static String PARAM_VALUE_OUTPUT_B = "b";

    // values for begin/end_direction
    public static String PARAM_VALUE_UPSTREAM = "-";
    public static String PARAM_VALUE_DOWNSTREAM = "+";

    // values for span_strand
    public static String PARAM_VALUE_BOTH_STRANDS = "same_strands";
    public static String PARAM_VALUE_SAME_STRAND = "same_strand";
    public static String PARAM_VALUE_OPPOSITE_STRANDS = "opposite_strands";

    public String[] getColumns() {
        return new String[] { COLUMN_PROJECT_ID, COLUMN_SOURCE_ID,
                COLUMN_WDK_WEIGHT };
    }

    public String[] getRequiredParameterNames() {
        return new String[] { PARAM_OPERATION, PARAM_SPAN_PREFIX + "a",
                PARAM_SPAN_PREFIX + "b" };
    }

    public void validateParameters(WsfRequest request)
            throws WsfServiceException {
        Map<String, String> params = request.getParams();
        Set<String> operators = new HashSet<String>(Arrays.asList(
                PARAM_VALUE_OVERLAP, PARAM_VALUE_A_CONTAIN_B,
                PARAM_VALUE_B_CONTAIN_A));
        Set<String> anchors = new HashSet<String>(Arrays.asList(
                PARAM_VALUE_START, PARAM_VALUE_STOP));
        Set<String> directions = new HashSet<String>(Arrays.asList(
                PARAM_VALUE_DOWNSTREAM, PARAM_VALUE_UPSTREAM));
        Set<String> outputs = new HashSet<String>(Arrays.asList(
                PARAM_VALUE_OUTPUT_A, PARAM_VALUE_OUTPUT_B));
        Set<String> strands = new HashSet<String>(Arrays.asList(
                PARAM_VALUE_BOTH_STRANDS, PARAM_VALUE_SAME_STRAND,
                PARAM_VALUE_OPPOSITE_STRANDS));

        // validate operator
        if (params.containsKey(PARAM_OPERATION)) {
            String operator = params.get(PARAM_OPERATION);
            if (!operators.contains(operator))
                throw new WsfServiceException("Invalid " + PARAM_OPERATION
                        + ": " + operator);
        }

        // validate output choice
        if (params.containsKey(PARAM_OUTPUT)) {
            String output = params.get(PARAM_OUTPUT);
            if (!outputs.contains(output))
                throw new WsfServiceException("Invalid " + PARAM_OUTPUT + ": "
                        + output);
        }

        // validate strand
        if (params.containsKey(PARAM_STRAND)) {
            String strand = params.get(PARAM_STRAND);
            if (!strands.contains(strand))
                throw new WsfServiceException("Invalid " + PARAM_STRAND + ": "
                        + strand);
        }

        // validate begin a
        validateAnchorParams(params, anchors, PARAM_BEGIN_PREFIX + "a");
        validateDirectionParams(params, directions,
                PARAM_BEGIN_DIRECTION_PREFIX + "a");
        validateOffsetParams(params, PARAM_BEGIN_OFFSET_PREFIX + "a");

        // validate end a
        validateAnchorParams(params, anchors, PARAM_END_PREFIX + "a");
        validateDirectionParams(params, directions, PARAM_END_DIRECTION_PREFIX
                + "a");
        validateOffsetParams(params, PARAM_END_OFFSET_PREFIX + "a");

        // validate begin b
        validateAnchorParams(params, anchors, PARAM_BEGIN_PREFIX + "b");
        validateDirectionParams(params, directions,
                PARAM_BEGIN_DIRECTION_PREFIX + "b");
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
            String anchor = params.get(param);
            if (!anchors.contains(anchor))
                throw new WsfServiceException("Invalid " + param + ": "
                        + anchor);
        }
    }

    private void validateDirectionParams(Map<String, String> params,
            Set<String> directions, String param) throws WsfServiceException {
        if (params.containsKey(param)) {
            String direction = params.get(param);
            if (!directions.contains(direction))
                throw new WsfServiceException("Invalid " + param + ": "
                        + direction);
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

    public WsfResponse execute(WsfRequest request) throws WsfServiceException {
        Map<String, String> params = request.getParams();
        String operation = params.get(PARAM_OPERATION);

        String output = params.get(PARAM_OUTPUT);
        if (output == null || !output.equalsIgnoreCase(PARAM_VALUE_OUTPUT_B))
            output = PARAM_VALUE_OUTPUT_A;

        String strand = params.get(PARAM_STRAND);
        if (strand == null) strand = PARAM_VALUE_BOTH_STRANDS;

        // get the proper begin & end of the derived regions.
        String[] startStopA = getStartStop(params, "a");
        String[] startStopB = getStartStop(params, "b");

        WdkModelBean wdkModelBean = (WdkModelBean) this.context.get(CConstants.WDK_MODEL_KEY);
        WdkModel wdkModel = wdkModelBean.getModel();

        Map<String, String> context = request.getContext();
        String userSignature = context.get(ProcessQueryInstance.CTX_USER);
        try {
            // get the sql to the cache table that represents user's two input
            // operands.
            String cacheA = getCache(wdkModel, userSignature, params, "a");
            String cacheB = getCache(wdkModel, userSignature, params, "b");

            // compose the final sql by comparing two regions with span
            // operation.
            String sql = composeSql(request.getProjectId(), operation, cacheA,
                    cacheB, startStopA, startStopB, strand, output);

            logger.debug("SPAN LOGIC SQL:\n" + sql);

            // execute the final sql, and fetch the result for the output.
            return getResult(wdkModel, sql, request.getOrderedColumns());
        } catch (Exception ex) {
            throw new WsfServiceException(ex);
        }
    }

    private String getCache(WdkModel wdkModel, String userSignature,
            Map<String, String> params, String suffix)
            throws NoSuchAlgorithmException, WdkUserException,
            WdkModelException, SQLException, JSONException {
        User user;
        if (userSignature == null) {
            user = wdkModel.getSystemUser();
        } else {
            user = wdkModel.getUserFactory().getUser(userSignature);
        }
        int stepId = Integer.valueOf(params.get(PARAM_SPAN_PREFIX + suffix));
        Step step = user.getStep(stepId);
        AnswerValue answerValue = step.getAnswerValue();
        return answerValue.getIdSql();
    }

    private String[] getStartStop(Map<String, String> params, String suffix) {
        // get the user's choice of begin & end, and the offsets from params.
        String begin = params.get(PARAM_BEGIN_PREFIX + suffix);
        if (begin == null) begin = PARAM_VALUE_START;
        String end = params.get(PARAM_END_PREFIX + suffix);
        if (end == null) end = PARAM_VALUE_STOP;

        String beginDir = params.get(PARAM_BEGIN_DIRECTION_PREFIX + suffix);
        if (beginDir == null) beginDir = PARAM_VALUE_UPSTREAM;
        String endDir = params.get(PARAM_END_DIRECTION_PREFIX + suffix);
        if (endDir == null) endDir = PARAM_VALUE_UPSTREAM;

        int beginOff = 0;
        if (params.containsKey(PARAM_BEGIN_OFFSET_PREFIX + suffix))
            beginOff = Integer.valueOf(params.get(PARAM_BEGIN_OFFSET_PREFIX
                    + suffix));
        int endOff = 0;
        if (params.containsKey(PARAM_END_OFFSET_PREFIX + suffix))
            endOff = Integer.valueOf(params.get(PARAM_END_OFFSET_PREFIX
                    + suffix));

        if (beginDir.equals(PARAM_VALUE_UPSTREAM)) beginOff *= -1;
        if (endDir.equals(PARAM_VALUE_UPSTREAM)) endOff *= -1;

        String table = "fl" + suffix + ".";

        // depending on whether the feature is on forward or reversed strand,
        // and user's choice of begin and end, we get the proper begin of the
        // region.
        StringBuilder sql = new StringBuilder("(CASE ");
        sql.append(" WHEN " + table + "is_reversed = 0 THEN (");
        sql.append(begin.equals(PARAM_VALUE_START) ? "start_min" : "end_max");
        sql.append(" + 1*(" + beginOff + ")) ");
        sql.append(" WHEN " + table + "is_reversed = 1 THEN (");
        sql.append (end.equals(PARAM_VALUE_START)? "end_max":"start_min");
        sql.append(" - 1*(" + endOff + ")) ");
        sql.append("END)");
        String start = sql.toString();

        // we get the proper end of the region.
        sql = new StringBuilder("(CASE ");
        sql.append("WHEN " + table + "is_reversed = 0 THEN (");
        sql.append(end.equals(PARAM_VALUE_START) ? "start_min" :"end_max");
        sql.append(" + 1*(" + endOff + ")) ");
        sql.append(" WHEN " + table + "is_reversed = 1 THEN (");
        sql.append (begin.equals(PARAM_VALUE_START)? "end_max":"start_min");
        sql.append(" - 1*(" + beginOff + ")) ");
        sql.append("END)");
        String stop = sql.toString();

        return new String[] { start, stop };
    }

    private String composeSql(String projectId, String operation, String sqlA,
            String sqlB, String[] regionA, String[] regionB, String strand,
            String output) {
        StringBuilder builder = new StringBuilder("SELECT ");

        // determine the output type
        builder.append(" DISTINCT f" + output + ".* FROM ");

        // construct the first sub-sql for region A.
        builder.append("(SELECT fla.feature_source_id AS source_id, ");
        builder.append("   fla.na_sequence_id, ca.project_id, ca.wdk_weight, ");
        builder.append(regionA[0] + " AS begin, " + regionA[1] + " AS end");
        builder.append(" FROM apidb.FEATURELOCATION fla, " + sqlA + " ca ");
        builder.append(" WHERE fla.feature_source_id = ca.source_id ");
        builder.append("   AND fla.is_top_level = 1) fa, ");

        // construct the second sub-sql for region B.
        builder.append("(SELECT flb.feature_source_id AS source_id, ");
        builder.append("   flb.na_sequence_id, cb.project_id, cb.wdk_weight, ");
        builder.append(regionB[0] + " AS begin, " + regionB[1] + " AS end");
        builder.append(" FROM apidb.FEATURELOCATION flb, " + sqlB + " cb ");
        builder.append(" WHERE flb.feature_source_id = cb.source_id ");
        builder.append("   AND flb.is_top_level = 1) fb ");

        // make sure the regions come from sequence source.
        builder.append(" WHERE fa.na_sequence_id = fb.na_sequence_id ");

        // restrict the regions to have start_min <= end_max
        builder.append("   AND fa.begin <= fa.end ");
        builder.append("   AND fb.begin <= fb.end ");

        // check the strand choice
        if (strand.equalsIgnoreCase(PARAM_VALUE_SAME_STRAND)) {
            builder.append("   AND fa.is_reversed = fb.is_reversed ");
        } else if (strand.equalsIgnoreCase(PARAM_VALUE_OPPOSITE_STRANDS)) {
            builder.append("   AND fa.is_reversed != fb.is_reversed ");
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

    private WsfResponse getResult(WdkModel wdkModel, String sql,
            String[] orderedColumns) throws WdkUserException,
            WdkModelException, SQLException {
        DataSource dataSource = wdkModel.getQueryPlatform().getDataSource();
        ResultSet results = null;
        try {
            results = SqlUtils.executeQuery(wdkModel, dataSource, sql);
            List<String[]> records = new ArrayList<String[]>();
            while (results.next()) {
                String[] record = new String[orderedColumns.length];
                for (int i = 0; i < record.length; i++) {
                    record[i] = results.getObject(orderedColumns[i]).toString();
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
        } finally {
            SqlUtils.closeResultSet(results);
        }
    }

    @Override
    protected String[] defineContextKeys() {
        return new String[] { CConstants.WDK_MODEL_KEY };
    }
}
