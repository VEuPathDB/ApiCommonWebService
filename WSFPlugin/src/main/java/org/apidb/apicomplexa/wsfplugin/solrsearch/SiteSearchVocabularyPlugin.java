package org.apidb.apicomplexa.wsfplugin.solrsearch;

import java.util.List;

import org.apidb.apicomplexa.wsfplugin.solrsearch.SiteSearchUtil.SearchField;
import org.gusdb.wdk.model.record.RecordClass;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;

public class SiteSearchVocabularyPlugin extends AbstractPlugin {

  @Override
  public String[] getRequiredParameterNames() {
    return new String[]{};
  }

  @Override
  public String[] getColumns() {
    return new String[]{ "term", "internal" };
  }

  @Override
  public void validateParameters(PluginRequest request) throws PluginModelException, PluginUserException {
    // no params to validate
  }

  @Override
  protected int execute(PluginRequest request, PluginResponse response)
      throws PluginModelException, PluginUserException {
    RecordClass recordClass = SiteSearchUtil.getRecordClass(request);
    List<SearchField> fields = SiteSearchUtil.getSearchFields(recordClass);
    for (SearchField field : fields) {
      response.addRow(new String[] { field.getTerm(), field.getInternal() });
    }
    return 0;
  }

}
