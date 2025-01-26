package org.apidb.apicomplexa.wsfplugin.eda;

import org.apache.log4j.Logger;
import org.eupathdb.websvccommon.wsfplugin.PluginUtilities;
import org.gusdb.fgputil.ArrayUtil;
import org.gusdb.fgputil.FormatUtil;
import org.gusdb.wdk.model.record.PrimaryKeyDefinition;
import org.gusdb.wdk.model.record.RecordClass;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.DelayedResultException;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;
import org.json.JSONException;
import org.json.JSONObject;

public class GeneEdaDatasetPlugin extends AbstractPlugin {

  private static final Logger LOG = Logger.getLogger(GeneEdaDatasetPlugin.class);

  private static final String EDA_ANALYSIS_SPEC_PARAM_NAME = "eda_analysis_spec";

  @Override
  public String[] getRequiredParameterNames() {
    return new String[] { EDA_ANALYSIS_SPEC_PARAM_NAME };
  }

  @Override
  public String[] getColumns(PluginRequest request) throws PluginModelException {
    RecordClass recordClass = PluginUtilities.getRecordClass(request);
    PrimaryKeyDefinition pkDef = recordClass.getPrimaryKeyDefinition();
    // PK column order is: gene_source_id, source_id, project_id
    String[] columns = ArrayUtil.concatenate(pkDef.getColumnRefs(), new String[] { "matched_result" });
    LOG.info(GeneEdaDatasetPlugin.class.getName() + " instance will return the following columns: " + FormatUtil.join(columns, ", "));
    return columns;
  }

  private JSONObject getAnalysisSpec(PluginRequest request) throws PluginUserException {
    try {
      String value = request.getParams().get(EDA_ANALYSIS_SPEC_PARAM_NAME);
      if (value == null || value.isBlank()) {
        throw new PluginUserException("Request does not include required parameter: " + EDA_ANALYSIS_SPEC_PARAM_NAME);
      }
      return new JSONObject(value);
    }
    catch (JSONException e) {
      throw new PluginUserException("Parameter " + EDA_ANALYSIS_SPEC_PARAM_NAME + " must contain a EDA analysis JSON object.");
    }
  }

  @Override
  public void validateParameters(PluginRequest request) throws PluginModelException, PluginUserException {
    // will throw exception if spec missing or non-json
    getAnalysisSpec(request);
  }

  @Override
  protected int execute(PluginRequest request, PluginResponse response)
      throws PluginModelException, PluginUserException, DelayedResultException {
    JSONObject analysisSpec = getAnalysisSpec(request);
    LOG.info("Will process EDA dataset request for analysis: " + analysisSpec.toString(2));
    return 0;
  }

}
