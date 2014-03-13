package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.WsfPluginException;

/**
 * @author steve
 */
public class FindGenesWithSnpCharsPlugin extends FindPolymorphismsPlugin {

  // required parameter definition
  public static final String PARAM_SNP_CLASS = "snp_stat";
  public static final String PARAM_OCCURENCES_LOWER = "occurrences_lower";
  public static final String PARAM_OCCURENCES_UPPER = "occurrences_upper";
  public static final String PARAM_DNDS_LOWER = "dn_ds_ratio_lower";
  public static final String PARAM_DNDS_UPPER = "dn_ds_ratio_upper";
  public static final String PARAM_DENSITY_LOWER = "density_lower";
  public static final String PARAM_DENSITY_UPPER = "density_upper";

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#getRequiredParameterNames()
   */
  @Override
  public String[] getExtraParamNames() {
    return new String[] {PARAM_SNP_CLASS, PARAM_OCCURENCES_LOWER, PARAM_OCCURENCES_UPPER, PARAM_DNDS_LOWER, PARAM_DNDS_UPPER, PARAM_DENSITY_LOWER, PARAM_DENSITY_UPPER};
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
    String snpClass = params.get(PARAM_SNP_CLASS);
    String min  = params.get(PARAM_OCCURENCES_LOWER);
    String max = params.get(PARAM_OCCURENCES_UPPER);
    String dnds_min = params.get(PARAM_DNDS_LOWER);
    String dnds_max = params.get(PARAM_DNDS_UPPER);
    String density_min = params.get(PARAM_DENSITY_LOWER);
    String density_max = params.get(PARAM_DENSITY_UPPER);

    List<String> command = super.makeCommandToCreateBashScript(jobDir, params, organismDir);
    command.add(snpClass);
    command.add(min);
    command.add(max);
    command.add(dnds_min);
    command.add(dnds_max);
    command.add(density_min);
    command.add(density_max);
    return command;
  }
 
}
