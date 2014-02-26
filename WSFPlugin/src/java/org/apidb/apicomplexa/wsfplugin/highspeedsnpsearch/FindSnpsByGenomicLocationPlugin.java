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
public class FindSnpsByGenomicLocationPlugin extends FindPolymorphismsPlugin {

  // required parameter definition
  public static final String PARAM_GENES_DATASET = "ds_gene_ids";

  public static final String genomicLocationsFileName = "genomicLocations.txt";

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#getRequiredParameterNames()
   */
  @Override
  public String[] getExtraParamNames() {
    return new String[] {PARAM_GENES_DATASET};
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
  protected void initForBashScript(File jobDir, Map<String, String> params, File organismDir) throws WsfPluginException {
    String gene_list_dataset = params.get(PARAM_GENES_DATASET);
  }

  @Override
  protected List<String> makeCommandToCreateBashScript(File jobDir, Map<String, String> params, File organismDir) throws WsfPluginException {
    List<String> command = super.makeCommandToCreateBashScript(jobDir, params, organismDir);
    command.add(genomicLocationsFileName);
    return command;
  }
 
}
