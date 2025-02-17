package org.apidb.apicomplexa.wsfplugin.eda;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.gusdb.fgputil.functional.Functions.wrapException;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Types;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.core.HttpHeaders;

import org.apache.log4j.Logger;
import org.eupathdb.websvccommon.wsfplugin.PluginUtilities;
import org.gusdb.fgputil.ArrayUtil;
import org.gusdb.fgputil.FormatUtil;
import org.gusdb.fgputil.IoUtil;
import org.gusdb.fgputil.Tuples.TwoTuple;
import org.gusdb.fgputil.client.ClientUtil;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.db.runner.SQLRunner.ArgumentBatch;
import org.gusdb.fgputil.db.runner.SQLRunnerException;
import org.gusdb.fgputil.functional.FunctionalInterfaces.FunctionWithException;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.dbms.TemporaryTable;
import org.gusdb.wdk.model.query.SqlQuery;
import org.gusdb.wdk.model.record.PrimaryKeyDefinition;
import org.gusdb.wdk.model.record.RecordClass;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.DelayedResultException;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class GeneEdaSubsetPlugin extends AbstractPlugin {

  private static final Logger LOG = Logger.getLogger(GeneEdaSubsetPlugin.class);

  /** Expect to see JSON similar to the following:
{
  "displayName":"Unnamed Analysis",
  "description":"",
  "studyId":"DS_b044a3a170",
  "studyVersion":"",
  "apiVersion":"",
  "isPublic":false,
  "analysisId":"eMqpWrK",
  "creationTime":"2025-01-24T12:42:07",
  "modificationTime":"2025-01-24T12:59:40",
  "numFilters":3,
  "numComputations":0,
  "numVisualizations":0,
  "descriptor":{
    "subset":{
      "descriptor":[
        {"variableId":"VAR_ccdd8654","entityId":"genePhenotypeData","type":"numberRange","min":-8.536320000000002,"max":-4.8442200000000035},
        {"variableId":"VAR_e105c28b","entityId":"genePhenotypeData","type":"numberRange","min":1190.286,"max":1587.048},
        {"variableId":"VAR_c33fe1b0","entityId":"genePhenotypeData","type":"numberRange","min":-6,"max":3.9164499999999998}
      ],
      "uiSettings":{
        "genePhenotypeData/VAR_bdc8e679":{
          "sort":{"columnKey":"value","direction":"asc","groupBySelected":false},
          "searchTerm":"",
          "currentPage":1,
          "rowsPerPage":50
        }
      }
    },
    "computations":[],
    "starredVariables":[],
    "dataTableConfig":{},
    "derivedVariables":[]
  }
}
   */

  private static final String EDA_ANALYSIS_SPEC_PARAM_NAME = "eda_analysis_spec";

  @Override
  public String[] getRequiredParameterNames() {
    return new String[] { EDA_ANALYSIS_SPEC_PARAM_NAME };
  }

  @Override
  public String[] getColumns(PluginRequest request) throws PluginModelException {
    RecordClass recordClass = PluginUtilities.getRecordClass(request);
    PrimaryKeyDefinition pkDef = recordClass.getPrimaryKeyDefinition();
    // PK column order is: gene_source_id, source_id, project_id
    String[] columns = ArrayUtil.concatenate(pkDef.getColumnRefs(), new String[] { "matched_result" });
    LOG.info(GeneEdaSubsetPlugin.class.getName() + " instance will return the following columns: " + FormatUtil.join(columns, ", "));
    return columns;
  }

  private JSONObject getAnalysisSpec(PluginRequest request) throws PluginUserException {
    String value = request.getParams().get(EDA_ANALYSIS_SPEC_PARAM_NAME);
    try {
      if (value == null || value.isBlank()) {
        throw new PluginUserException("Request does not include required parameter: " + EDA_ANALYSIS_SPEC_PARAM_NAME);
      }
      return new JSONObject(value);
    }
    catch (JSONException e) {
      LOG.error("Bad request: " + value);
      throw new PluginUserException("Parameter " + EDA_ANALYSIS_SPEC_PARAM_NAME + " must contain a EDA analysis JSON object.", e);
    }
  }

  @Override
  public void validateParameters(PluginRequest request) throws PluginModelException, PluginUserException {
    // only a convenience to validate here; same thing happens if we validate in execute()
  }

  @Override
  protected int execute(PluginRequest request, PluginResponse response)
      throws PluginModelException, PluginUserException, DelayedResultException {

    // read request body as JSON and pick out important pieces
    JSONObject analysisSpec = getAnalysisSpec(request);
    String datasetId = analysisSpec.getString("studyId"); // misnamed; still need to look up study ID
    JSONArray filters = analysisSpec.getJSONObject("descriptor").getJSONObject("subset").getJSONArray("descriptor");

    // get auth header to pass with EDA requests
    Map<String,String> authHeader = Map.of(HttpHeaders.AUTHORIZATION, "Bearer " +
        Optional.ofNullable(request.getContext().get(Utilities.CONTEXT_KEY_BEARER_TOKEN_STRING))
            .orElseThrow(() -> new PluginUserException("No user bearer token supplied to plugin.")));

    WdkModel wdkModel = PluginUtilities.getWdkModel(request);
    String edaBaseUrl = wdkModel.getProperties().get("LOCALHOST") + "/eda";

    // validate dataset ID and convert to study ID for call to EDA
    String studyId = findStudyId(edaBaseUrl, datasetId, authHeader)
        .orElseThrow(() -> new PluginUserException("Dataset with ID '" + datasetId + "' could not be found for this user."));

    // look up study to find entity containing gene variable (tuple is <entityId,variableId>
    TwoTuple<String,String> entityIdVariableId = findGeneColumnLocation(edaBaseUrl, studyId, authHeader)
        .orElseThrow(() -> new PluginUserException("Study '" + studyId + "' does not have a variable designated 'gene'."));

    try (
        // create temporary cache table to hold our gene result
        TemporaryTable tmpTable = new TemporaryTable(wdkModel, tableName -> "CREATE TABLE " + tableName + " ( gene_source_id VARCHAR(20) )");

        // make request to EDA; if fails, then don't need to be inside SQLRunner
        InputStream tabularStream = getEdaGeneResult(edaBaseUrl, studyId, entityIdVariableId.getFirst(), entityIdVariableId.getSecond(), filters, authHeader)
    ) {

      BufferedReader reader = new BufferedReader(new InputStreamReader(tabularStream));
      if (reader.ready()) reader.readLine(); // get rid of header

      // insert gene rows into temporary table
      String insertSql = "INSERT INTO " + tmpTable.getTableName() + "( ? )";
      new SQLRunner(wdkModel.getAppDb().getDataSource(), insertSql, "insert-tmp-gene-vals").executeStatementBatch(new ArgumentBatch() {

        @Override
        public Iterator<Object[]> iterator() {
          return new Iterator<Object[]>() {

            @Override
            public boolean hasNext() {
              return wrapException(() -> reader.ready());
            }

            @Override
            public Object[] next() {
              return new Object[] { wrapException(() -> reader.readLine()) };
            }
          };
        }

        @Override
        public int getBatchSize() {
          return 20;
        }

        @Override
        public Integer[] getParameterTypes() {
          return new Integer[] { Types.VARCHAR };
        }
      });

      // once temporary table is written, join with transcripts to create transcript result
      String rawSql = ((SqlQuery)wdkModel.getQuerySet("GeneId").getQuery("GeneByLocusTag")).getSql();
      String sql = Utilities.replaceMacros(rawSql, Map.of("ds_gene_ids", "select gene_source_id, rownum as dataset_value_order from " + tmpTable.getTableName()));

      new SQLRunner(wdkModel.getAppDb().getDataSource(), sql, "eda-gene-to-transcript").executeQuery(rs -> {
        /* SQL query will return:
          <column name="source_id"/>
          <column name="matched_result"/>
          <column name="gene_source_id"/>
          <column name="project_id"/>
          <column name="input_id"/>
          <column name="dataset_order"/> */
        /* Need to supply columns in this order:
             gene_source_id, source_id, project_id, matched_result */
        try {
          while (rs.next()) {
            response.addRow(new String[] {
                rs.getString("gene_source_id"),
                rs.getString("source_id"),
                rs.getString("project_id"),
                rs.getString("matched_result")
            });
          }
          return null;
        }
        catch (PluginUserException | PluginModelException e) {
          throw new SQLRunnerException("Could not parse transcript transform result", e);
        }
      });

      return 0;
    }
    catch (Exception e) {
      throw new PluginModelException("Could not insert filtered genes into temporary table", e);
    }
  }

  protected InputStream getEdaGeneResult(String edaBaseUrl, String studyId, String entityId, String variableId, JSONArray filters, Map<String, String> authHeader) throws Exception {
    String url = edaBaseUrl + "/eda/studies/" + studyId + "/entities/" + entityId + "/tabular";
    JSONObject body = new JSONObject()
        .put("outputVariableIds", new JSONArray().put(variableId))
        .put("filters", filters);
    LOG.info("Will make request to EDA at URL: " + url + " with auth header value " + authHeader.get(HttpHeaders.AUTHORIZATION) + " and body: " + body.toString(2));
    return ClientUtil.makeAsyncPostRequest(url, body.toString(), "text/tab-separated-values", authHeader).getInputStream();
  }

  private <T> T readGetRequest(String url, Map<String,String> authHeader, FunctionWithException<JSONObject, T> mapper) throws PluginModelException {
    try (InputStream response = ClientUtil.makeAsyncGetRequest(url, APPLICATION_JSON, authHeader).getInputStream()) {
      return mapper.apply(new JSONObject(IoUtil.readAllChars(new InputStreamReader(response))));
    }
    catch (Exception e) {
      throw new PluginModelException("Unable to fetch, read, or parse response from " + url, e);
    }
  }

  private Optional<String> findStudyId(String edaBaseUrl, String datasetId, Map<String,String> authHeader) throws PluginModelException {
    return readGetRequest(edaBaseUrl + "/permissions", authHeader, responseJson -> {
      JSONObject datasets = responseJson.getJSONObject("perDataset");
      if (!datasets.has(datasetId)) {
        return Optional.empty();
      }
      return Optional.of(datasets.getJSONObject(datasetId).getString("studyId"));
    });
  }

  private Optional<TwoTuple<String, String>> findGeneColumnLocation(String edaBaseUrl, String studyId, Map<String,String> authHeader) throws PluginModelException {
    return readGetRequest(edaBaseUrl + "/studies/" + studyId, authHeader, responseJson -> {
      JSONObject rootEntity = responseJson.getJSONObject("study").getJSONObject("rootEntity");
      return findGeneColumnLocation(rootEntity);
    });
  }

  // if finds variable with provider label "gene", returns tuple of [entityId, variableId]
  private Optional<TwoTuple<String, String>> findGeneColumnLocation(JSONObject entity) {

    // try to find gene variable on this entity
    String entityId = entity.getString("id");
    JSONArray vars = entity.getJSONArray("variables");
    for (int i = 0; i < vars.length(); i++) {
      JSONObject var = vars.getJSONObject(i);
      String variableId = var.getString("id");
      if (var.has("providerLabel")) {
        String labelArrayStr = var.getString("providerLabel");
        JSONArray labels = new JSONArray(labelArrayStr);
        for (int j = 0; j < labels.length(); j++) {
          if (labels.getString(j).equals("gene")) {
            // found it on this entity
            return Optional.of(new TwoTuple<>(entityId, variableId));
          }
        }
      }
    }

    // no luck; now check child entities recursively through the tree
    JSONArray children = entity.getJSONArray("children");
    for (int i = 0; i < children.length(); i++) {
      JSONObject child = children.getJSONObject(i);
      Optional<TwoTuple<String, String>> result = findGeneColumnLocation(child);
      if (result.isPresent()) {
        // found it in the entity tree
        return result;
      }
    }

    // could not find anywhere
    return Optional.empty();
  }

}
