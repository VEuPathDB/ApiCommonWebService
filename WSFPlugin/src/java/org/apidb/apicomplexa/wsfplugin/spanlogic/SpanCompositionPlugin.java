package org.apidb.apicomplexa.wsfplugin.spanlogic;

import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Map;

import org.gusdb.wdk.controller.CConstants;
import org.gusdb.wdk.controller.action.ActionUtility;
import org.gusdb.wdk.model.AnswerValue;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.WdkUserException;
import org.gusdb.wdk.model.jspwrap.UserBean;
import org.gusdb.wdk.model.jspwrap.WdkModelBean;
import org.gusdb.wdk.model.user.AnswerFactory;
import org.gusdb.wdk.model.user.Step;
import org.gusdb.wdk.model.user.User;
import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfResult;
import org.gusdb.wsf.plugin.WsfServiceException;
import org.json.JSONException;

public class SpanCompositionPlugin extends WsfPlugin {

    public static String COLUMN_SOURCE_ID = "source_id";
    public static String COLUMN_PROJECT_ID = "project_id";
    public static String COLUMN_WDK_WEIGHT = "wdk_weight";

    public static String PARAM_OPERATION = "span_operation";
    public static String PARAM_OUTPUT = "span_output";
    public static String PARAM_SPAN = "span_";
    public static String PARAM_SPAN_BEGIN = "span_begin_";
    public static String PARAM_SPAN_BEGIN_DIRECTION = "span_begin_direction_";
    public static String PARAM_SPAN_BEGIN_OFFSET = "span_begin_offset_";
    public static String PARAM_SPAN_END = "span_end_";
    public static String PARAM_SPAN_END_DIRECTION = "span_end_direction_";
    public static String PARAM_SPAN_END_OFFSET = "span_end_offset_";

    public static String PARAM_VALUE_OVERLAP = "overlap";
    public static String PARAM_VALUE_A_CONTAIN_B = "a_contain_b";
    public static String PARAM_VALUE_B_CONTAIN_A = "b_contain_a";

    public static String PARAM_VALUE_START = "start";
    public static String PARAM_VALUE_STOP = "stop";

    public static String PARAM_VALUE_OUTPUT_A = "a";
    public static String PARAM_VALUE_OUTPUT_B = "b";

    public static String PARAM_VALUE_UPSTREAM = "+";
    public static String PARAM_VALUE_DOWNSTREAM = "-";

    @Override
    protected String[] getColumns() {
        return new String[] { COLUMN_PROJECT_ID, COLUMN_SOURCE_ID,
                COLUMN_WDK_WEIGHT };
    }

    @Override
    protected String[] getRequiredParameterNames() {
        return new String[] { PARAM_OPERATION, PARAM_SPAN + "a",
                PARAM_SPAN + "b" };
    }

    @Override
    protected void validateParameters(Map<String, String> params)
            throws WsfServiceException {
    // do nothing
    }

    @Override
    protected WsfResult execute(String projectId, String userSignature,
            Map<String, String> params, String[] orderedColumns)
            throws WsfServiceException {
        String operation = params.get(PARAM_OPERATION);
        String output = params.get(PARAM_OUTPUT);
        if (output == null || !output.equalsIgnoreCase("b")) output = "a";

        String[] startStopA = getStartStop(params, "a");
        String[] startStopB = getStartStop(params, "b");

        WdkModelBean wdkModelBean = (WdkModelBean) servletContext.getAttribute(CConstants.WDK_MODEL_KEY);
        WdkModel wdkModel = wdkModelBean.getModel();

        try {
            String cacheA = getCache(wdkModel, userSignature, params, "a");
            String cacheB = getCache(wdkModel, userSignature, params, "b");

            String sql = composeSql(projectId, operation, cacheA, cacheB, startStopA,
                    startStopB, output);
            return getResult(wdkModel, sql);
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
        int stepId = Integer.valueOf(params.get(PARAM_SPAN + suffix));
        Step step = user.getStep(stepId);
        AnswerValue answerValue = step.getAnswerValue();
        return answerValue.getIdSql();
    }

    private String[] getStartStop(Map<String, String> params, String suffix) {
        String begin = params.get(PARAM_SPAN_BEGIN + suffix);
        if (begin == null) begin = PARAM_VALUE_START;
        String end = params.get(PARAM_SPAN_END + suffix);
        if (end == null) end = PARAM_VALUE_STOP;
        String beginDirection = params.get(PARAM_SPAN_BEGIN_DIRECTION + suffix);
        if (beginDirection == null) beginDirection = PARAM_VALUE_UPSTREAM;
        String endDirection = params.get(PARAM_SPAN_END_DIRECTION + suffix);
        if (endDirection == null) endDirection = PARAM_VALUE_UPSTREAM;
        int beginOffset = 0;
        if (params.containsKey(PARAM_SPAN_BEGIN_OFFSET + suffix))
            beginOffset = Integer.valueOf(params.get(PARAM_SPAN_BEGIN_OFFSET
                    + suffix));
        int endOffset = 0;
        if (params.containsKey(PARAM_SPAN_END_OFFSET + suffix))
            endOffset = Integer.valueOf(params.get(PARAM_SPAN_END_OFFSET
                    + suffix));

        beginOffset *= beginDirection.equals(PARAM_VALUE_UPSTREAM) ? -1 : 1;
        endOffset *= endDirection.equals(PARAM_VALUE_UPSTREAM) ? -1 : 1;

        String table = "fl" + suffix + ".";
        // (CASE fla.IS_REVERSED WHEN 0 THEN fla.start_min - $$start_off$$ ELSE
        // fla.end_max + $$end_off$$ END)
        StringBuilder builder = new StringBuilder("(CASE ");
        builder.append(table + "is_reversed WHEN 0 THEN ");
        builder.append(table + begin);
        builder.append(((beginOffset >= 0) ? " + " : " ") + beginOffset);
        builder.append(" ELSE " + table + end);
        builder.append(((endOffset <= 0) ? " + " : " ") + (-endOffset));
        builder.append(")");
        String start = builder.toString();

        // (CASE fla.IS_REVERSED WHEN 0 THEN fla.end_max - 80 ELSE fla.start_min
        // + 50 END)
        builder = new StringBuilder("(CASE ");
        builder.append(table + "is_reversed WHEN 0 THEN ");
        builder.append(table + end);
        builder.append(((endOffset >= 0) ? " + " : " ") + endOffset);
        builder.append(" ELSE " + table + start);
        builder.append(((beginOffset <= 0) ? " + " : " ") + (-beginOffset));
        builder.append(")");

        String stop = builder.toString();
        return new String[] { start, stop };
    }

    private String composeSql(String projectId, String operation, String sqlA, String sqlB,
            String[] regionA, String[] regionB, String output) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT fl" + output + ".feature_source_id AS source_id");
        
        
        
        
        return builder.toString();
    }

    private WsfResult getResult(WdkModel wdkModel, String sql) {

        
        return null;
    }
}
