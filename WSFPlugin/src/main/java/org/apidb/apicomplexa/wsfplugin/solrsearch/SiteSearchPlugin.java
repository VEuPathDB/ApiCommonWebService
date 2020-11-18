package org.apidb.apicomplexa.wsfplugin.solrsearch;

import static org.apidb.apicommon.model.TranscriptUtil.getGeneRecordClass;
import static org.apidb.apicommon.model.TranscriptUtil.isTranscriptRecordClass;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apidb.apicommon.model.TranscriptUtil;
import org.eupathdb.websvccommon.wsfplugin.solrsearch.EuPathSiteSearchPlugin;
import org.eupathdb.websvccommon.wsfplugin.solrsearch.SiteSearchUtil;
import org.gusdb.wdk.model.record.RecordClass;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.json.JSONArray;
import org.json.JSONObject;

public class SiteSearchPlugin extends EuPathSiteSearchPlugin {

  private static final String ORGANISM_PARAM_NAME = "text_search_organism";

  @Override
  protected String[] getDynamicColumns(RecordClass recordClass) {
    return !TranscriptUtil.isTranscriptRecordClass(recordClass) ?
      super.getDynamicColumns(recordClass) :
      new String[]{ "matched_result", "max_score" };
  }

  @Override
  protected String[] readResultRow(RecordClass recordClass, JSONArray primaryKey,
      boolean pkHasProjectId, String recordProjectId, String score) {
    return !TranscriptUtil.isTranscriptRecordClass(recordClass) ?
      super.readResultRow(recordClass, primaryKey, pkHasProjectId, recordProjectId, score) :
      // transcript requests only return gene ID; return it + empty transcript ID (will be filled in later)
      new String[]{ primaryKey.getString(0), "", recordProjectId, "Y", score };
  }

  @Override
  protected JSONObject supplementSearchParams(PluginRequest request, JSONObject baseSolrRequestJson) {
    String projectId = request.getProjectId();
    Map<String,String> internalValues = request.getParams();
    List<String> organismTerms = internalValues.containsKey(ORGANISM_PARAM_NAME) ?
        getTermsFromInternal(internalValues.get(ORGANISM_PARAM_NAME), false) : null;
    return baseSolrRequestJson
      // only add project ID filter for non-portal sites; for portal get back all records
      .put("restrictToProject", isPortal(projectId) ? null : projectId)
      .put("restrictSearchToOrganisms", organismTerms);
  }

  @Override
  protected String getRequestedDocumentType(PluginRequest request) throws PluginModelException {
    return convertDocumentType(request);
  }

  @Override
  protected Optional<String> getProjectIdForFilter(String projectId) {
    return isPortal(projectId) ? Optional.empty() : Optional.of(projectId);
  }

  public static String convertDocumentType(PluginRequest request) throws PluginModelException {
    RecordClass recordClass = SiteSearchUtil.getRecordClass(request);
    return (isTranscriptRecordClass(recordClass) ?
      getGeneRecordClass(SiteSearchUtil.getWdkModel(request.getProjectId())) :
      recordClass
    ).getUrlSegment();
  }

  public static boolean isPortal(String projectId) {
    return projectId.equals("EuPathDB");
  }
}
