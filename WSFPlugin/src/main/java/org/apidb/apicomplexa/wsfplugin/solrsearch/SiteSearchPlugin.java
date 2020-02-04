package org.apidb.apicomplexa.wsfplugin.solrsearch;

import static org.apidb.apicomplexa.wsfplugin.solrsearch.SiteSearchUtil.getRequestedDocumentType;
import static org.apidb.apicomplexa.wsfplugin.solrsearch.SiteSearchUtil.getSearchFields;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.apidb.apicomplexa.wsfplugin.solrsearch.SiteSearchUtil.SearchField;
import org.gusdb.fgputil.FormatUtil;
import org.gusdb.fgputil.functional.Functions;
import org.gusdb.fgputil.web.MimeTypes;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;
import org.json.JSONArray;
import org.json.JSONObject;

public class SiteSearchPlugin extends AbstractPlugin {

  private static final Logger LOG = Logger.getLogger(SiteSearchPlugin.class);

  @Override
  public String[] getRequiredParameterNames() {
    return new String[]{ "solr_search_organism", "text_expression", "solr_text_fields" };
  }

  @Override
  public String[] getColumns() {
    return new String[] { "source_id", "gene_source_id", "project_id", "matched_result", "max_score" };
  }

  @Override
  public void validateParameters(PluginRequest request) throws PluginModelException, PluginUserException {
    // parameters should already be validated by WDK
  }

  @Override
  protected int execute(PluginRequest request, PluginResponse response)
      throws PluginModelException, PluginUserException {
    LOG.info("Executing " + SiteSearchPlugin.class.getSimpleName() + "...\n" + FormatUtil.getCurrentStackTrace());
    Response solrResponse = null;
    try {
      Client client = ClientBuilder.newClient();
      String metadataUrl = SiteSearchUtil.getSolrServiceUrl();
      JSONObject requestBody = buildRequestJson(request);
      LOG.info("Querying site search service at " + metadataUrl + " with JSON body: " + requestBody.toString(2));
      WebTarget webTarget = client.target(metadataUrl);
      Invocation.Builder invocationBuilder = webTarget.request(MimeTypes.ND_JSON);
      solrResponse = invocationBuilder.post(Entity.entity(requestBody.toString(), MediaType.APPLICATION_JSON));
      BufferedReader br = new BufferedReader(new InputStreamReader((InputStream)solrResponse.getEntity()));
      while (br.ready()) {
        String line = br.readLine();
        LOG.info("Site Search Service response line: " + line);
        String[] tokens = line.split(FormatUtil.TAB);
        if (tokens.length != 2) throw new PluginModelException("Unexpected format in line: " + line);
        JSONArray primaryKey = new JSONArray(tokens[0]);
        String score = tokens[1];
        response.addRow(new String[]{
          primaryKey.getString(0),
          primaryKey.getString(1),
          primaryKey.getString(2),
          "1",
          score
        });
      }
      return 0;
    }
    catch (IOException e) {
      throw new PluginModelException("Could not read response from site search service", e);
    }
    finally {
      if (solrResponse != null) solrResponse.close();
    }
  }
  /**
   * Builds something like this:
   * 
   * {
   *   searchText: string,
   *   restrictToProject?: string,
   *   restrictSearchToOrganisms?: string[], (must be subset of metadata orgs)
   *   documentTypeFilter?: {
   *     documentType: string,
   *     foundOnlyInFields?: string[]
   *   }
   * }
   */
  private JSONObject buildRequestJson(PluginRequest request) throws PluginModelException {
    String docType = getRequestedDocumentType(request);
    Map<String,SearchField> searchFieldMap = Functions.getMapFromValues(
        getSearchFields(docType), field -> field.getTerm());
    String projectId = request.getProjectId();
    Map<String,String> internalValues = request.getParams();
    String searchTerm = unquoteString(internalValues.get("text_expression"));
    List<String> organismTerms = getTermsFromInternal(internalValues.get("text_search_organism"));
    List<String> searchFields = getTermsFromInternal(internalValues.get("solr_text_fields"))
        .stream()
        .map(term -> searchFieldMap.get(term))
        .filter(field -> field != null)
        .map(field -> field.getSolrField())
        .collect(Collectors.toList());
    return new JSONObject()
      .put("searchText", searchTerm)
      .put("restrictToProject", projectId)
      .put("restrictSearchToOrganisms", organismTerms)
      .put("documentTypeFilter", new JSONObject()
        .put("documentType", docType)
        .put("foundOnlyInFields", searchFields)
      );
  }

  private static List<String> getTermsFromInternal(String internalEnumValue) {
    return Arrays.stream(internalEnumValue.split(","))
      .map(quotedStr -> unquoteString(quotedStr))
      .collect(Collectors.toList());
  }

  private static String unquoteString(String quotedString) {
    return quotedString.substring(1, quotedString.length() - 1);
  }
}
