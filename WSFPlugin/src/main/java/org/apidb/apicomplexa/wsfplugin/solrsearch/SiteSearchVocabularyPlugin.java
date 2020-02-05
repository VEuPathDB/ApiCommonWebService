package org.apidb.apicomplexa.wsfplugin.solrsearch;

import static org.apidb.apicomplexa.wsfplugin.solrsearch.SiteSearchUtil.getRequestedDocumentType;
import static org.apidb.apicomplexa.wsfplugin.solrsearch.SiteSearchUtil.getSearchFields;
import static org.apidb.apicomplexa.wsfplugin.solrsearch.SiteSearchUtil.getSiteSearchServiceUrl;

import java.util.List;

import org.apache.log4j.Logger;
import org.apidb.apicomplexa.wsfplugin.solrsearch.SiteSearchUtil.SearchField;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;

public class SiteSearchVocabularyPlugin extends AbstractPlugin {

  private static final Logger LOG = Logger.getLogger(SiteSearchVocabularyPlugin.class);

  @Override
  public String[] getRequiredParameterNames() {
    return new String[]{};
  }

  @Override
  public String[] getColumns() {
    return new String[]{ "internal", "term" };
  }

  @Override
  public void validateParameters(PluginRequest request) throws PluginModelException, PluginUserException {
    // no params to validate
  }

  @Override
  protected int execute(PluginRequest request, PluginResponse response)
      throws PluginModelException, PluginUserException {
    LOG.info("Executing " + SiteSearchVocabularyPlugin.class.getSimpleName() + "...");
    String serviceUrl = getSiteSearchServiceUrl(request);
    String docType = getRequestedDocumentType(request);
    List<SearchField> fields = getSearchFields(serviceUrl, docType);
    for (SearchField field : fields) {
      LOG.info("Adding response row: " + field);
      response.addRow(new String[] { field.getInternal(), field.getTerm() });
    }
    return 0;
  }

}
