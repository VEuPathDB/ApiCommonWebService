package org.apidb.apicomplexa.wsfplugin.solrsearch;

import static org.apidb.apicomplexa.wsfplugin.solrsearch.SiteSearchUtil.getRecordClass;
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
import org.apidb.apicommon.model.TranscriptUtil;
import org.apidb.apicomplexa.wsfplugin.solrsearch.SiteSearchUtil.SearchField;
import org.gusdb.fgputil.ArrayUtil;
import org.gusdb.fgputil.FormatUtil;
import org.gusdb.fgputil.FormatUtil.Style;
import org.gusdb.fgputil.functional.Functions;
import org.gusdb.fgputil.json.JsonUtil;
import org.gusdb.fgputil.web.MimeTypes;
import org.gusdb.wdk.model.record.PrimaryKeyDefinition;
import org.gusdb.wdk.model.record.RecordClass;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;
import org.json.JSONArray;
import org.json.JSONObject;

public class SiteSearchPlugin extends AbstractPlugin {

  private static final Logger LOG = Logger.getLogger(SiteSearchPlugin.class);

  private static final String ORGANISM_PARAM_NAME = "text_search_organism";
  private static final String SEARCH_TEXT_PARAM_NAME = "text_expression";
  private static final String SEARCH_DOC_TYPE = "document_type";
  private static final String SEARCH_FIELDS_PARAM_NAME = "text_fields";

  @Override
  public String[] getRequiredParameterNames() {
    return new String[]{ SEARCH_TEXT_PARAM_NAME, SEARCH_FIELDS_PARAM_NAME };
  }

  @Override
  public String[] getColumns(PluginRequest request) throws PluginModelException {
    RecordClass recordClass = getRecordClass(request);
    PrimaryKeyDefinition pkDef = recordClass.getPrimaryKeyDefinition();
    String[] dynamicColumns = TranscriptUtil.isTranscriptRecordClass(recordClass) ?
        new String[]{ "matched_result", "max_score" } : new String[]{ "max_score" };
    String[] columns = ArrayUtil.concatenate(pkDef.getColumnRefs(), dynamicColumns);
    LOG.info("SiteSearchPlugin instance will return the following columns: " + FormatUtil.join(columns, ", "));
    return columns;
  }

  @Override
  public void validateParameters(PluginRequest request) throws PluginModelException, PluginUserException {
    // most validation already performed by WDK; make sure passed doc type
    //   matches urlSegment of requested record class
    if (!getRequestedDocumentType(request).equals(request.getParams().get(SEARCH_DOC_TYPE))) {
      throw new PluginUserException("Invalid param value '" +
          request.getParams().get(SEARCH_DOC_TYPE) + "' for " + SEARCH_DOC_TYPE +
          ".  Value for this recordclass must be " + getRequestedDocumentType(request));
    }
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
      RecordClass recordClass = getRecordClass(request);
      boolean pkHasProjectId = recordClass.getPrimaryKeyDefinition().hasColumn("project_id");
      while ((line = br.readLine()) != null) {
        LOG.debug("Site Search Service response line: " + line);
        String[] tokens = line.split(FormatUtil.TAB);
        if (tokens.length < 2 || tokens.length > 3) {
          throw new PluginModelException("Unexpected format in line: " + line);
        }
        JSONArray primaryKey = new JSONArray(tokens[0]);
        String score = tokens[1];
        String projectId = tokens.length == 3 && !tokens[2].isBlank() ? tokens[2].trim() : request.getProjectId();

        // build WSF plugin result row from parsed site search row
        String[] row = TranscriptUtil.isTranscriptRecordClass(recordClass) ?
          // transcript requests only return gene ID; return it + empty transcript ID (will be filled in later)
          new String[]{ primaryKey.getString(0), "", projectId, "Y", score } :
          ArrayUtil.concatenate(JsonUtil.toStringArray(primaryKey),
            // only include projectId if it is a primary key field
            pkHasProjectId ? new String[] { projectId, score } : new String[] { score });

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
    List<String> organismTerms = internalValues.containsKey(ORGANISM_PARAM_NAME) ?
        getTermsFromInternal(internalValues.get(ORGANISM_PARAM_NAME), false) : null;
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
