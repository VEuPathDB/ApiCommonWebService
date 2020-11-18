package org.apidb.apicomplexa.wsfplugin.solrsearch;

import java.util.Optional;

import org.eupathdb.websvccommon.wsfplugin.solrsearch.EuPathSiteSearchVocabularyPlugin;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;

public class SiteSearchVocabularyPlugin extends EuPathSiteSearchVocabularyPlugin {

  @Override
  protected String getRequestedDocumentType(PluginRequest request) throws PluginModelException {
    return SiteSearchPlugin.convertDocumentType(request);
  }

  @Override
  protected Optional<String> getProjectIdForFilter(String projectId) {
    return SiteSearchPlugin.isPortal(projectId) ? Optional.empty() : Optional.of(projectId);
  }

}
