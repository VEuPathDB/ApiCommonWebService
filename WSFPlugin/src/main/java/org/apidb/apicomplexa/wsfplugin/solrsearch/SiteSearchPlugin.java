package org.apidb.apicomplexa.wsfplugin.solrsearch;

import static org.apidb.apicomplexa.wsfplugin.solrsearch.SiteSearchUtil.getRequestedDocumentType;
import static org.apidb.apicomplexa.wsfplugin.solrsearch.SiteSearchUtil.getSearchFields;
import static org.apidb.apicomplexa.wsfplugin.solrsearch.SiteSearchUtil.getSiteSearchServiceUrl;

import java.io.BufferedReader;
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
import org.gusdb.fgputil.FormatUtil.Style;
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

  private static final String ORGANISM_PARAM_NAME = "solr_search_organism";
  private static final String SEARCH_TEXT_PARAM_NAME = "text_expression";
  private static final String SEARCH_FIELDS_PARAM_NAME = "solr_text_fields";

  @Override
  public String[] getRequiredParameterNames() {
    return new String[]{ ORGANISM_PARAM_NAME, SEARCH_TEXT_PARAM_NAME, SEARCH_FIELDS_PARAM_NAME };
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
    LOG.info("Executing " + SiteSearchPlugin.class.getSimpleName() +
        " with params " + FormatUtil.prettyPrint(request.getParams(), Style.MULTI_LINE));
    Response searchResponse = null;
    try {
      // build request elements
      String searchUrl = getSiteSearchServiceUrl(request);
      JSONObject requestBody = buildRequestJson(request);
      LOG.info("Querying site search service at " + searchUrl + " with JSON body: " + requestBody.toString(2));

      // make request
      Client client = ClientBuilder.newClient();
      WebTarget webTarget = client.target(searchUrl);
      Invocation.Builder invocationBuilder = webTarget.request(MimeTypes.ND_JSON);
      searchResponse = invocationBuilder.post(Entity.entity(requestBody.toString(), MediaType.APPLICATION_JSON));

      LOG.info("Received response from site search service with status: " + searchResponse.getStatus());

      BufferedReader br = new BufferedReader(new InputStreamReader((InputStream)searchResponse.getEntity()));
      String line;
      while ((line = br.readLine()) != null) {
        LOG.debug("Site Search Service response line: " + line);
        String[] tokens = line.split(FormatUtil.TAB);
        if (tokens.length != 2) throw new PluginModelException("Unexpected format in line: " + line);
        JSONArray primaryKey = new JSONArray(tokens[0]);
        String score = tokens[1];
        // FIXME: currently hard-coded for transcripts
        String[] row = new String[]{
          "",
          primaryKey.getString(0),
          request.getProjectId(),
          "Y",
          score
        };
        LOG.debug("Returning row: " + new JSONArray(row).toString());
        response.addRow(row);
      }
      return 0;
    }
    catch (Exception e) {
      throw new PluginModelException("Could not read response from site search service", e);
    }
    finally {
      if (searchResponse != null) searchResponse.close();
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
  private static JSONObject buildRequestJson(PluginRequest request) throws PluginModelException {
    String docType = getRequestedDocumentType(request);
    Map<String,SearchField> searchFieldMap = Functions.getMapFromValues(
        getSearchFields(getSiteSearchServiceUrl(request), docType, request.getProjectId()), field -> field.getTerm());
    String projectId = request.getProjectId();
    Map<String,String> internalValues = request.getParams();
    String searchTerm = unquoteString(internalValues.get(SEARCH_TEXT_PARAM_NAME));
    List<String> organismTerms = getTermsFromInternal(internalValues.get(ORGANISM_PARAM_NAME), false);
    List<String> searchFieldTerms = getTermsFromInternal(internalValues.get(SEARCH_FIELDS_PARAM_NAME), true);
    List<String> searchFieldSolrNames =
      (searchFieldTerms.isEmpty() ?
        searchFieldMap.values().stream() :
        searchFieldTerms.stream()
          .map(term -> searchFieldMap.get(term))
          .filter(field -> field != null)
      )
      .map(field -> field.getSolrField())
      .collect(Collectors.toList());
    return new JSONObject()
      .put("searchText", searchTerm)
      // only add project ID filter for non-portal sites; for portal get back all records
      .put("restrictToProject", projectId.equals("EuPathDB") ? null : projectId)
      .put("restrictSearchToOrganisms", organismTerms)
      .put("documentTypeFilter", new JSONObject()
        .put("documentType", docType)
        .put("foundOnlyInFields", searchFieldSolrNames)
      );
  }

  private static List<String> getTermsFromInternal(String internalEnumValue, boolean performDequote) {
    return Arrays.stream(internalEnumValue.split(","))
      .map(string -> performDequote ? unquoteString(string) : string)
      .collect(Collectors.toList());
  }

  private static String unquoteString(String quotedString) {
    return quotedString.substring(1, quotedString.length() - 1);
  }
}
