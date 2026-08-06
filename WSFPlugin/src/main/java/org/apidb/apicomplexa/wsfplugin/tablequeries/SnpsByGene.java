package org.apidb.apicomplexa.wsfplugin.tablequeries;

import static org.gusdb.wdk.model.answer.single.SingleRecordQuestionParam.PRIMARY_KEY_PARAM_NAME;
import static org.gusdb.wdk.model.record.TableField.TABLE_NAME_PARAM_NAME;

import org.apache.log4j.Logger;
import org.gusdb.fgputil.ArrayUtil;
import org.gusdb.fgputil.json.JsonUtil;
import org.gusdb.fgputil.runtime.InstanceManager;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.DelayedResultException;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;
import org.json.JSONArray;

public class SnpsByGene extends AbstractPlugin {

  private static final Logger LOG = Logger.getLogger(SnpsByGene.class);

  @Override
  public String[] getRequiredParameterNames() {
    return new String[] { PRIMARY_KEY_PARAM_NAME, TABLE_NAME_PARAM_NAME };
  }

  @Override
  public String[] getColumns(PluginRequest request) throws PluginModelException {
    return ArrayUtil.concatenate(getPkColumnNames(request), new String[] { "a", "b", "c" });
  }

  @Override
  public void validateParameters(PluginRequest request) throws PluginModelException, PluginUserException {
    // PK value should already have been validated by record service
  }

  @Override
  protected int execute(PluginRequest request, PluginResponse response)
      throws PluginModelException, PluginUserException, DelayedResultException {

    // pull out params
    String pkValueString = request.getParams().get(PRIMARY_KEY_PARAM_NAME);
    String tableName = request.getParams().get(TABLE_NAME_PARAM_NAME);
    LOG.info("Found params: pkValueString = " + pkValueString + ", tableName = " + tableName);
    String[] pkValues = JsonUtil.toStringArray(new JSONArray(pkValueString));

    // stub out fake snp data (10 rows of 3 columns)
    int valueSuffix = 0;
    for (int i = 0; i < 10; i++) {
      response.addRow(ArrayUtil.concatenate(pkValues, new String[] {
          tableName + "-a" + valueSuffix++,
          tableName + "-b" + valueSuffix++,
          tableName + "-c" + valueSuffix++
      }));
    }

    return 0;
  }

  private String[] getPkColumnNames(PluginRequest request) {
    WdkModel wdkModel = InstanceManager.getInstance(WdkModel.class, request.getProjectId());
    String questionFullName = request.getContext().get(Utilities.CONTEXT_KEY_QUESTION_FULL_NAME);
    return wdkModel.getQuestionByFullName(questionFullName).orElseThrow()
        .getRecordClass().getPrimaryKeyDefinition().getColumnRefs();
  }

}
