package org.apidb.apicomplexa.wsfplugin.solrsearch;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status.Family;

import org.apache.log4j.Logger;
import org.gusdb.fgputil.IoUtil;
import org.gusdb.fgputil.json.JsonIterators;
import org.gusdb.fgputil.runtime.GusHome;
import org.gusdb.fgputil.runtime.InstanceManager;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.record.PrimaryKeyDefinition;
import org.gusdb.wdk.model.record.RecordClass;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.json.JSONArray;
import org.json.JSONObject;

public class SiteSearchUtil {

  private static final Logger LOG = Logger.getLogger(SiteSearchUtil.class);

  public static class SearchField {

    public final String _term;
    public final String _internal;
    public final String _solrField;

    public SearchField(String name, String displayName, String term) {
      _solrField = name;
      _term = displayName;
      _internal = term;
    }

    public String getTerm() {
      return _term;
    }
    public String getInternal() {
      return _internal;
    }
    public String getSolrField() {
      return _solrField;
    }
  }

  public static WdkModel getWdkModel(String projectId) {
    return InstanceManager.getInstance(WdkModel.class, GusHome.getGusHome(), projectId);
  }

  public static String getSolrServiceUrl() {
    // TODO: use value in model.prop when it exists; for now use Dave's site
    return "https://dfalke-b.plasmodb.org/site-search";
  }

  private static RecordClass getRecordClass(PluginRequest request) throws PluginModelException {
    String questionFullName = request.getContext().get("wdk-question");
    return getWdkModel(request.getProjectId()).getQuestionByFullName(questionFullName)
      .map(question -> question.getRecordClass())
      .orElseThrow(() -> new PluginModelException("Could not find context question: " + questionFullName));
  }

  public static String getRequestedDocumentType(PluginRequest request) throws PluginModelException {
    String urlSegment = getRecordClass(request).getUrlSegment();
    return urlSegment.equals("transcript") ? "gene" : urlSegment;
  }

  public static PrimaryKeyDefinition getPrimaryKeyDefinition(PluginRequest request) throws PluginModelException {
    return getRecordClass(request).getPrimaryKeyDefinition();
  }

  public static List<SearchField> getSearchFields(String documentType) throws PluginModelException {
    Response response = null;
    try {
      Client client = ClientBuilder.newClient();
      String metadataUrl = getSolrServiceUrl() + "/categories-metadata";
      LOG.info("Querying site search service with: " + metadataUrl);
      WebTarget webTarget = client.target(metadataUrl);
      Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
      response = invocationBuilder.get();
      String responseBody = IoUtil.readAllChars(new InputStreamReader((InputStream)response.getEntity()));
      if (!response.getStatusInfo().getFamily().equals(Family.SUCCESSFUL)) {
        throw new PluginModelException("Unable to retrieve metadata from site " +
            "search service.  Request returned " + response.getStatus() +
            ". Response body:\n" + responseBody);
      }
      JSONArray docTypes = new JSONObject(responseBody).getJSONArray("documentTypes");
      List<JSONArray> fieldsJsons = JsonIterators.arrayStream(docTypes)
        .map(obj -> obj.getJSONObject())
        .filter(obj -> obj.getString("id").equals(documentType))
        .map(obj -> obj.getJSONObject("wdkRecordTypeData").getJSONArray("searchFields"))
        .collect(Collectors.toList());
      if (fieldsJsons.size() != 1) {
        throw new PluginModelException("Could not find unique document type with id " + documentType);
      }
      return JsonIterators.arrayStream(fieldsJsons.get(0))
        .map(obj -> obj.getJSONObject())
        .map(json -> new SearchField(
           json.getString("name"),
           json.getString("displayName"),
           json.getString("term")))
        .collect(Collectors.toList());
    }
    catch (IOException e) {
      throw new PluginModelException("Could not read categories-metadata response", e);
    }
    finally {
      if (response != null) response.close();
    }
  }
}
