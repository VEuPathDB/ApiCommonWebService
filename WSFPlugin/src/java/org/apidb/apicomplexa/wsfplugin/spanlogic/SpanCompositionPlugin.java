package org.apidb.apicomplexa.wsfplugin.spanlogic;

import java.util.Map;

import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.WdkUserException;
import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfResult;
import org.gusdb.wsf.plugin.WsfServiceException;

public class SpanCompositionPlugin extends WsfPlugin {

    public static String COLUMN_SOURCE_ID = "source_id";
    public static String COLUMN_PROJECT_ID = "project_id";
    public static String COLUMN_WDK_WEIGHT = "wdk_weight";

    public static String PARAM_OPERATION = "span_operation";
    public static String PARAM_SPAN_A = "span_a";
    public static String PARAM_SPAN_A_BEGIN = "span_a_begin";
    public static String PARAM_SPAN_A_BEGIN_DIRECTION = "span_a_begin_direction";
    public static String PARAM_SPAN_A_BEGIN_OFFSET = "span_a_begin_offset";
    public static String PARAM_SPAN_A_END = "span_a_end";
    public static String PARAM_SPAN_A_END_DIRECTION = "span_a_end_direction";
    public static String PARAM_SPAN_A_END_OFFSET = "span_a_end_offset";
    public static String PARAM_SPAN_B = "span_b";
    public static String PARAM_SPAN_B_BEGIN = "span_b_begin";
    public static String PARAM_SPAN_B_BEGIN_DIRECTION = "span_b_begin_direction";
    public static String PARAM_SPAN_B_BEGIN_OFFSET = "span_b_begin_offset";
    public static String PARAM_SPAN_B_END = "span_b_end";
    public static String PARAM_SPAN_B_END_DIRECTION = "span_b_end_direction";
    public static String PARAM_SPAN_B_END_OFFSET = "span_b_end_offset";

    public static String PARAM_VALUE_OVERLAP = "overlap";
    public static String PARAM_VALUE_A_CONTAIN_B = "a_contain_b";
    public static String PARAM_VALUE_B_CONTAIN_A = "b_contain_a";

    public static String PARAM_VALUE_START = "start";
    public static String PARAM_VALUE_STOP = "stop";

    public static String PARAM_VALUE_UPSTREAM = "+";
    public static String PARAM_VALUE_DOWNSTREAM = "-";

    @Override
    protected WsfResult execute(String queryName, Map<String, String> params,
            String[] orderedColumns) throws WsfServiceException {
        String operation = params.get(PARAM_OPERATION);

        int answerAId = Integer.valueOf(params.get(PARAM_SPAN_A));

        
        return null;
    }

    @Override
    protected String[] getColumns() {
        return new String[] { COLUMN_PROJECT_ID, COLUMN_SOURCE_ID,
                COLUMN_WDK_WEIGHT };
    }

    @Override
    protected String[] getRequiredParameterNames() {
        return new String[] { PARAM_OPERATION, PARAM_SPAN_A, PARAM_SPAN_B };
    }

    @Override
    protected void validateParameters(Map<String, String> params)
            throws WsfServiceException {
    // do nothing
    }

    private String[] getStartEndA(Map<String, String> params)
            throws WdkModelException {
        String begin = params.get(PARAM_SPAN_A_BEGIN);
        if (begin == null) begin = PARAM_VALUE_START;
        String end = params.get(PARAM_SPAN_A_END);
        if (end == null) end = PARAM_VALUE_STOP;
        String beginDirection = params.get(PARAM_SPAN_A_BEGIN_DIRECTION);
        if (beginDirection == null) beginDirection = PARAM_VALUE_UPSTREAM;
        String endDirection = params.get(PARAM_SPAN_A_END_DIRECTION);
        if (endDirection == null) endDirection = PARAM_VALUE_UPSTREAM;
        int beginOffset = 0;
        if (params.containsKey(PARAM_SPAN_A_BEGIN_OFFSET))
            beginOffset = Integer.valueOf(params.get(PARAM_SPAN_A_BEGIN_OFFSET));
        int endOffset = 0;
        if (params.containsKey(PARAM_SPAN_A_END_OFFSET))
            endOffset = Integer.valueOf(params.get(PARAM_SPAN_A_END_OFFSET));

        String start = getStartStop("A", "begin", begin, beginDirection,
                beginOffset);
        String stop = getStartStop("A", "end", end, endDirection, endOffset);
        return new String[] { start, stop };
    }

    private String[] getStartEndB(Map<String, String> params)
            throws WdkModelException {
        String begin = params.get(PARAM_SPAN_B_BEGIN);
        if (begin == null) begin = PARAM_VALUE_START;
        String end = params.get(PARAM_SPAN_B_END);
        if (end == null) end = PARAM_VALUE_STOP;
        String beginDirection = params.get(PARAM_SPAN_B_BEGIN_DIRECTION);
        if (beginDirection == null) beginDirection = PARAM_VALUE_UPSTREAM;
        String endDirection = params.get(PARAM_SPAN_B_END_DIRECTION);
        if (endDirection == null) endDirection = PARAM_VALUE_UPSTREAM;
        int beginOffset = 0;
        if (params.containsKey(PARAM_SPAN_B_BEGIN_OFFSET))
            beginOffset = Integer.valueOf(params.get(PARAM_SPAN_B_BEGIN_OFFSET));
        int endOffset = 0;
        if (params.containsKey(PARAM_SPAN_B_END_OFFSET))
            endOffset = Integer.valueOf(params.get(PARAM_SPAN_B_END_OFFSET));

        String start = getStartStop("B", "begin", begin, beginDirection,
                beginOffset);
        String stop = getStartStop("B", "end", end, endDirection, endOffset);
        return new String[] { start, stop };
    }

    private String getStartStop(String span, String beginEnd, String anchor,
            String direction, int offset) throws WdkModelException {
        String location;
        if (anchor.equalsIgnoreCase(PARAM_VALUE_START)) location = "start_min";
        else if (anchor.equalsIgnoreCase(PARAM_VALUE_STOP)) location = "stop_max";
        else throw new WdkModelException("The " + beginEnd + " of the span "
                + span + " is invalid: " + anchor);

        if (!direction.equalsIgnoreCase(PARAM_VALUE_UPSTREAM)
                && !direction.equalsIgnoreCase(PARAM_VALUE_DOWNSTREAM))
            throw new WdkModelException("The " + beginEnd
                    + " direction of the span " + span + " is invalid: "
                    + direction);
        location += " " + direction + " " + offset;
        return location;
    }
}
