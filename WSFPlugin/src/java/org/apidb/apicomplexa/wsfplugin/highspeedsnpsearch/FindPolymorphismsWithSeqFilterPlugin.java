package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gusdb.fgputil.runtime.GusHome;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.WsfPluginException;

/**
 * @author steve
 */
public class FindPolymorphismsWithSeqFilterPlugin extends FindPolymorphismsPlugin {

  // required parameter definition
  public static final String PARAM_CHROMOSOME = "chromosomeOptionalForNgsSnps";
  public static final String PARAM_SEQUENCE = "sequenceId";
  public static final String PARAM_START_POINT = "start_point";
  public static final String PARAM_END_POINT = "end_point";

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#getRequiredParameterNames()
   */
  @Override
  public String[] getRequiredParameterNames() {
    String[] superParameters = super.getRequiredParameterNames();
    String[] extraParameters = {PARAM_CHROMOSOME, PARAM_SEQUENCE, PARAM_START_POINT, PARAM_END_POINT};
    String[] combined = new String[superParameters.length + extraParameters.length];
    System.arraycopy(superParameters, 0, combined, 0, superParameters.length);
    System.arraycopy(extraParameters, 0, combined, superParameters.length, extraParameters.length);
    return combined;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#validateParameters(java.util.Map)
   */
  @Override
  public void validateParameters(PluginRequest request)
    throws WsfPluginException {
  }

  @Override
  protected List<String> makeCommandToCreateBashScript(File jobDir, Map<String, String> params, File organismDir) throws WsfPluginException {
    String chromosome = params.get(PARAM_CHROMOSOME);
    String seq = params.get(PARAM_SEQUENCE);
    if (seq.equals("")) seq = chromosome;
    String start = params.get(PARAM_START_POINT);
    int end = Integer.parseInt(params.get(PARAM_END_POINT));
    if (end == 0) end = 1000000000;
    List<String> command = super.makeCommandToCreateBashScript(jobDir, params, organismDir);
    command.add(seq);
    command.add(start);
    command.add("" + end);
    return command;
  }
 
}
