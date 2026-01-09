package org.apidb.apicomplexa.wsfplugin.eda;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;

import org.apache.log4j.Logger;
import org.eupathdb.common.service.PostValidationUserException;
import org.gusdb.fgputil.Tuples.TwoTuple;
import org.gusdb.fgputil.client.ClientUtil;
import org.gusdb.wsf.plugin.PluginModelException;
import org.json.JSONArray;
import org.json.JSONObject;

public class GeneEdaSubsetPlugin extends AbstractEdaGenesPlugin {

  private static final Logger LOG = Logger.getLogger(GeneEdaSubsetPlugin.class);

  // Hard-coded variable ID which is expected to contain gene IDs
  private static final String GENE_ID_VARIABLE_ID = "VAR_bdc8e679";

  @Override
  protected InputStream getEdaTabularDataStream(String edaBaseUrl, Map<String, String> authHeader) throws Exception {

    // dig filters out of analysis spec
    JSONArray filters = _analysisSpec.getJSONObject("descriptor").getJSONObject("subset").getJSONArray("descriptor");

    // look up study to find entity containing gene variable (tuple is <entityId,variableId>
    TwoTuple<String,String> entityIdVariableId = findGeneColumnLocation(edaBaseUrl, _studyId, authHeader);

    // use subsetting service's tabular endpoint (no need for merging or data services)
    String url = edaBaseUrl + "/studies/" + _studyId + "/entities/" + entityIdVariableId.getFirst() + "/tabular";

    JSONObject body = new JSONObject()
        .put("outputVariableIds", new JSONArray().put(entityIdVariableId.getSecond()))
        .put("filters", filters);

    LOG.info("Will make request to EDA at URL: " + url + " with auth header value " + authHeader.get(HttpHeaders.AUTHORIZATION) + " and body: " + body.toString(2));

    return ClientUtil.makeAsyncPostRequest(url, body, "text/tab-separated-values", authHeader).getInputStream();
  }

  // convenience type
  private static class VariableList extends ArrayList<TwoTuple<String, String>> {}

  private static TwoTuple<String, String> findGeneColumnLocation(String edaBaseUrl, String studyId, Map<String,String> authHeader) throws PluginModelException {

    JSONObject rootEntity = readGetRequest(edaBaseUrl + "/studies/" + studyId, authHeader,
        responseJson -> responseJson.getJSONObject("study").getJSONObject("rootEntity"));

    // find gene ID variable and ensure there is only one in this study
    VariableList foundVars = new VariableList();
    findGeneColumnLocation(rootEntity, foundVars);
    if (foundVars.size() != 1) {
      throw new PostValidationUserException("Study " + studyId + " has " + (foundVars.isEmpty() ? "0" : ">1") +
          "variables identified as gene IDs (variable ID = " + GENE_ID_VARIABLE_ID + ")");
    }
    return foundVars.get(0);
  }

  // finds variables with hard-coded gene variable name; fills foundVars with tuples of [entityId, variableId]
  private static void findGeneColumnLocation(JSONObject entity, VariableList foundVars) {

    // try to find gene variable on this entity
    String entityId = entity.getString("id");
    JSONArray vars = entity.getJSONArray("variables");
    for (int i = 0; i < vars.length(); i++) {
      JSONObject var = vars.getJSONObject(i);
      if (GENE_ID_VARIABLE_ID.equals(var.getString("id"))) {
        // found the var on this entity
        foundVars.add(new TwoTuple<>(entityId, GENE_ID_VARIABLE_ID));
      }
    }

    // now check child entities recursively through the tree
    JSONArray children = entity.getJSONArray("children");
    for (int i = 0; i < children.length(); i++) {
      JSONObject child = children.getJSONObject(i);
      findGeneColumnLocation(child, foundVars);
    }
  }

  @Override
  protected Boolean isRetainedRow(String[] edaRow) {
    return true;
  }

  @Override
  protected Object[] convertToTmpTableRow(String[] edaRow) {
    // this plugin's EDA response contains only the gene ID in a single column
    return new Object[] { edaRow[0] };
  }

}
