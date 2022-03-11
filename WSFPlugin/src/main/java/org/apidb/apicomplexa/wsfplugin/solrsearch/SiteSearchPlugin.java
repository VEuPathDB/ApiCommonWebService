package org.apidb.apicomplexa.wsfplugin.solrsearch;

import static org.apidb.apicommon.model.TranscriptUtil.getGeneRecordClass;
import static org.apidb.apicommon.model.TranscriptUtil.isTranscriptRecordClass;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

import org.apidb.apicommon.model.TranscriptUtil;
import org.eupathdb.websvccommon.wsfplugin.PluginUtilities;
import org.eupathdb.websvccommon.wsfplugin.solrsearch.EuPathSiteSearchPlugin;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.db.runner.SQLRunnerException;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.record.RecordClass;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.json.JSONArray;
import org.json.JSONObject;

public class SiteSearchPlugin extends EuPathSiteSearchPlugin {

  private static final String ORGANISM_PARAM_NAME = "text_search_organism";
  private static final String ORGANISM_DOC_TYPE = "organism";

  // cache of map from organism source_id to project ID
  private static Map<String,String> _projectByOrganismSourceIdMap = null;

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
      TranscriptUtil.isProjectIdInPks(recordClass.getWdkModel()) ?
          new String[]{ primaryKey.getString(0), "", recordProjectId, "Y", score } :
          new String[]{ primaryKey.getString(0), "", "Y", score };
  }

  @Override
  protected JSONObject supplementSearchParams(PluginRequest request, JSONObject baseSolrRequestJson) {
    String projectId = request.getProjectId();
    Map<String,String> internalValues = request.getParams();

    // if the parameter set includes an organism parameter, add organisms to solr query, else don't
    List<String> organismTerms = internalValues.containsKey(ORGANISM_PARAM_NAME) ?
        getTermsFromInternal(internalValues.get(ORGANISM_PARAM_NAME), false) : null;
    return baseSolrRequestJson
      // only add project ID filter for non-portal sites; for portal get back all records
      .put("restrictToProject", isPortal(projectId) ? "VEuPathDB" : projectId)
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
    RecordClass recordClass = PluginUtilities.getRecordClass(request);
    return (isTranscriptRecordClass(recordClass) ?
      getGeneRecordClass(PluginUtilities.getWdkModel(request.getProjectId())) :
      recordClass
    ).getUrlSegment();
  }

  public static boolean isPortal(String projectId) {
    return projectId.equals("EuPathDB");
  }

  @Override
  protected String computeRecordProjectId(Optional<String> solrRecordProjectId, JSONArray primaryKey, PluginRequest request) throws PluginModelException {
    // special case of organism record in the portal
    if (isPortal(request.getProjectId()) && getRequestedDocumentType(request).equals(ORGANISM_DOC_TYPE)) {
      if (_projectByOrganismSourceIdMap == null) {
        _projectByOrganismSourceIdMap = getProjectByOrganismSourceIdMap(PluginUtilities.getWdkModel(request));
      }
      String organismSourceId = primaryKey.getString(0);
      String projectId = _projectByOrganismSourceIdMap.get(organismSourceId);
      if (projectId == null) {
        throw new PluginModelException("No row exists in table 'apidbtuning.organismattributes' for organism returned by SOLR '" + organismSourceId + "'.");
      }
      return projectId;
    }
    return super.computeRecordProjectId(solrRecordProjectId, primaryKey, request);
  }

  private static synchronized Map<String, String> getProjectByOrganismSourceIdMap(WdkModel wdkModel) throws PluginModelException {
    try {
      DataSource appDs = wdkModel.getAppDb().getDataSource();
      String sql = "select distinct source_id, project_id from apidbtuning.organismattributes";
      return new SQLRunner(appDs, sql).executeQuery(rs -> {
        Map<String, String> orgProjectMap = new HashMap<>();
        while (rs.next()) {
          orgProjectMap.put(rs.getString(1), rs.getString(2));
        }
        return orgProjectMap;
      });
    }
    catch (SQLRunnerException e) {
      throw new PluginModelException("Unable to generate project ID map for organism doc type", e.getCause());
    }
  }
}
