package org.apidb.apicomplexa.wsfplugin.eda;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.gusdb.fgputil.FormatUtil.TAB;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Types;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.ws.rs.core.HttpHeaders;

import org.apache.log4j.Logger;
import org.eupathdb.common.service.PostValidationUserException;
import org.eupathdb.websvccommon.wsfplugin.PluginUtilities;
import org.gusdb.fgputil.ArrayUtil;
import org.gusdb.fgputil.FormatUtil;
import org.gusdb.fgputil.IoUtil;
import org.gusdb.fgputil.ListBuilder;
import org.gusdb.fgputil.client.ClientUtil;
import org.gusdb.fgputil.db.runner.ArgumentBatch;
import org.gusdb.fgputil.db.runner.SQLRunner;
import org.gusdb.fgputil.db.runner.SQLRunnerException;
import org.gusdb.fgputil.functional.FunctionalInterfaces.FunctionWithException;
import org.gusdb.fgputil.functional.Functions;
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

public abstract class AbstractEdaGenesPlugin extends AbstractPlugin {

  private static final Logger LOG = Logger.getLogger(AbstractEdaGenesPlugin.class);

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

  private static final String EDA_DATASET_ID_PARAM_NAME = "eda_dataset_id";
  private static final String EDA_ANALYSIS_SPEC_PARAM_NAME = "eda_analysis_spec";

  private static final String[] EMPTY_ARRAY = new String[0];

  protected WdkModel _wdkModel;
  protected String[] _responseColumnNames;
  protected List<String> _filteredDynamicAttributeNames; // dyn attrs not including wdk_weight
  protected String _datasetId;
  protected JSONObject _analysisSpec;
  protected Map<String,String> _additionalParamValues;
  protected String _studyId;

  @Override
  public String[] getRequiredParameterNames() {
    // note: dataset ID param is optional; value will be pulled off analysis spec if omitted
    return ArrayUtil.concatenate(new String[] { EDA_ANALYSIS_SPEC_PARAM_NAME }, getAdditionalRequiredParamNames());
  }

  // override to support query parameters beyond dataset ID and analysis spec
  protected String[] getAdditionalRequiredParamNames() {
    return EMPTY_ARRAY;
  }

  // override to support query parameters beyond dataset ID and analysis spec
  protected void setAdditionalParamValues(Map<String, String> paramMap) {
    // no additional params by default
  }

  protected abstract InputStream getEdaTabularDataStream(String edaBaseUrl, Map<String, String> authHeader) throws Exception;

  protected abstract Boolean isRetainedRow(String[] edaRow);

  protected abstract Object[] convertToTmpTableRow(String[] edaRow);

  @Override
  public String[] getColumns(PluginRequest request) throws PluginModelException {
    RecordClass recordClass = PluginUtilities.getRecordClass(request);
    PrimaryKeyDefinition pkDef = recordClass.getPrimaryKeyDefinition();
    _filteredDynamicAttributeNames = PluginUtilities
        .getContextQuestion(request)
        .getDynamicAttributeFieldMap()
        .keySet().stream()
        .filter(s -> !s.equals(Utilities.COLUMN_WEIGHT))
        .collect(Collectors.toList());
    // PK column order is: gene_source_id, source_id, project_id
    _responseColumnNames = ArrayUtil.concatenate(pkDef.getColumnRefs(), new String[] { "matched_result" }, _filteredDynamicAttributeNames.toArray(new String[0]));
    LOG.info(GeneEdaSubsetPlugin.class.getName() + " instance will return the following columns: " + FormatUtil.join(_responseColumnNames, ", "));
    return _responseColumnNames;
  }

  @Override
  public void validateParameters(PluginRequest request) throws PluginModelException, PluginUserException {
    // only a convenience to validate here; same thing happens if we validate in execute()
  }

  private static JSONObject getAnalysisSpec(PluginRequest request) {
    String datasetParamValue = request.getParams().get(EDA_DATASET_ID_PARAM_NAME);
    if (datasetParamValue == null || datasetParamValue.isBlank()) {
      throw new PostValidationUserException("Parameter '" + EDA_DATASET_ID_PARAM_NAME + "' is required.");
    }
    String value = request.getParams().get(EDA_ANALYSIS_SPEC_PARAM_NAME);
    try {
      if (value == null || value.isBlank()) {
        // This can happen if the user has not interacted with the EDA UI at all (i.e. no filters have been applied)
        // Thus, generate an "empty" descriptor for use
        String creationDate = FormatUtil.formatDateTime(new Date());
        return new JSONObject()
          .put("studyId", datasetParamValue)
          .put("displayName", "Unnamed Analysis")
          .put("description", "")
          .put("studyVersion", "")
          .put("apiVersion", "")
          .put("isPublic", false)
          .put("analysisId", "abcdefg")
          .put("creationTime", creationDate)
          .put("modificationTime", creationDate)
          .put("numFilters", 0)
          .put("numComputations", 0)
          .put("numVisualizations", 0)
          .put("descriptor", new JSONObject()
            .put("subset", new JSONObject()
              .put("descriptor", new JSONArray())
              .put("uiSettings", new JSONObject()))
            .put("computations", new JSONArray())
            .put("starredVariables", new JSONArray())
            .put("dataTableConfig", new JSONObject())
            .put("derivedVariables", new JSONArray()));

      }
      return new JSONObject(value);
    }
    catch (JSONException e) {
      LOG.error("Bad request: " + value);
      throw new PostValidationUserException("Parameter " + EDA_ANALYSIS_SPEC_PARAM_NAME +
          " must contain a EDA analysis JSON object. " + e.getMessage());
    }
  }

  @Override
  protected int execute(PluginRequest request, PluginResponse response)
      throws PluginModelException, PluginUserException, DelayedResultException {

    // read request body as JSON and pick out important pieces
    _analysisSpec = getAnalysisSpec(request);
    _datasetId = _analysisSpec.getString("studyId"); // misnamed; still need to look up study ID

    // check to make sure dataset ID param matches dataset declared in the analysis spec
    String datasetParamValue = request.getParams().get(EDA_DATASET_ID_PARAM_NAME);
    if (datasetParamValue != null && !_datasetId.equals(datasetParamValue)) {
      throw new PostValidationUserException("Value of dataset parameter '" + EDA_DATASET_ID_PARAM_NAME +
          "' ('" + datasetParamValue + "') must match 'studyId' property declared in the passed analysis spec ('" +
          _datasetId + "').  Note both values should be dataset IDs, not study IDs (old API).");
    }

    // set additional params on subclass (may want to process or convert to other types)
    _additionalParamValues = Arrays.stream(getAdditionalRequiredParamNames())
        .collect(Collectors.toMap(p -> p, p -> request.getParams().get(p)));

    // get auth header to pass with EDA requests
    Map<String,String> authHeader = Map.of(HttpHeaders.AUTHORIZATION, "Bearer " +
        Optional.ofNullable(request.getContext().get(Utilities.CONTEXT_KEY_BEARER_TOKEN_STRING))
            .orElseThrow(() -> new PluginModelException("No user bearer token supplied to plugin.")));

    _wdkModel = PluginUtilities.getWdkModel(request);
    Map<String,String> props = _wdkModel.getProperties();
    String edaBaseUrl = props.get("LOCALHOST") + props.get("EDA_SERVICE_URL");

    // validate dataset ID and convert to study ID for calls to EDA
    _studyId = findStudyId(edaBaseUrl, _datasetId, authHeader)
        .orElseThrow(() -> new PostValidationUserException("Dataset with ID '" + _datasetId + "' could not be found for this user."));

    // gather columns for temporary table
    List<String> tmpTableColumns = new ListBuilder<String>("gene_source_id").addAll(_filteredDynamicAttributeNames).toList();

    // create SQL for table to store temporary gene IDs and dynamic columns
    String createTmpTableSql = tmpTableColumns.stream()
        .map(col -> col +" VARCHAR(30)")
        .collect(Collectors.joining(", ", "CREATE TABLE %s%s ( ", " )"));

    LOG.info("Creating temporary table with SQL: " + createTmpTableSql);

    try (
        // create temporary cache table to hold our gene result
        TemporaryTable tmpTable = new TemporaryTable(_wdkModel, (schema, tableName) -> String.format(createTmpTableSql, schema, tableName));
        // make request to EDA and do any conversion to get a tabular stream
        InputStream tabularStream = getEdaTabularDataStream(edaBaseUrl, authHeader)
    ) {

      BufferedReader reader = new BufferedReader(new InputStreamReader(tabularStream));
      if (!reader.ready()) {
        throw new PluginModelException("EDA tabular response did not contain a header line (or any rows");
      }

      // skip header (assume subclasses know which columns are returned by the EDA request and handle them appropriately)
      reader.readLine();

      // insert gene rows into temporary table
      String placeholders = tmpTableColumns.stream().map(c -> "?").collect(Collectors.joining(", "));
      String insertSql = "INSERT INTO " + tmpTable.getTableNameWithSchema() + " values ( " + placeholders + " )";

      LOG.info("Will insert rows into temporary table with SQL: " + insertSql);

      FilteredArgumentBatch rowsProvider = new FilteredArgumentBatch(
          reader, this::isRetainedRow, this::convertToTmpTableRow, tmpTableColumns.size());

      new SQLRunner(_wdkModel.getAppDb().getDataSource(), insertSql, "insert-tmp-gene-vals")
          .executeStatementBatch(rowsProvider);

      LOG.info(rowsProvider.getNumRowsProvided() + " rows successfully written to temporary table (" + rowsProvider.getNumRowsSkipped() + " rows skipped).");

      // RRD: leaving test code here but commented; uncommenting allows us to view the temporary table after the fact (real temporary table is deleted)
      //String copySql = "create table " + tmpTable.getTableNameWithSchema() + "a as (select * from " + tmpTable.getTableNameWithSchema() + ")";
      //new SQLRunner(_wdkModel.getAppDb().getDataSource(), copySql).executeStatement();

      // once temporary table is written, join with transcripts to create transcript result
      String rawSql = ((SqlQuery)_wdkModel.getQuerySet("GeneId").getQuery("GeneByLocusTag")).getSql();
      String rownumCol = _wdkModel.getAppDb().getPlatform().getRowNumberColumn();
      String geneTranscriptsSql = Utilities.replaceMacros(rawSql, Map.of("ds_gene_ids", "select gene_source_id, " + rownumCol + " as dataset_value_order from " + tmpTable.getTableNameWithSchema()));

      // join back to temp table to pick up dynamic cols, but only if necessary
      if (!_filteredDynamicAttributeNames.isEmpty()) {
        geneTranscriptsSql = "select gt.*, " +
            _filteredDynamicAttributeNames.stream().map(col -> "tmp." + col).collect(Collectors.joining(", ")) +
            " from (" + geneTranscriptsSql + ") gt, " + tmpTable.getTableNameWithSchema() + " tmp" +
            " where gt.gene_source_id = tmp.gene_source_id";
      }

      LOG.info("Joining EDA genes to transcripts to deliver transcript rows to WDK with this SQL: " + geneTranscriptsSql);
      new SQLRunner(_wdkModel.getAppDb().getDataSource(), geneTranscriptsSql, "eda-gene-to-transcript").executeQuery(rs -> {
        /*
          SQL query will return:
            <column name="source_id"/>
            <column name="matched_result"/>
            <column name="gene_source_id"/>
            <column name="project_id"/>
            <column name="input_id"/>
            <column name="dataset_order"/>
            + any dynamic columns
          Need to supply columns in this order:
            gene_source_id, source_id, project_id, matched_result, then dynamic columns
        */
        try {
          int numTranscriptsDelivered = 0;
          while (rs.next()) {
            // loop over claimed response columns and pull directly off the result set to write WSF response row
            numTranscriptsDelivered++;
            response.addRow(
              Arrays.stream(_responseColumnNames)
                .map(Functions.fSwallow(name -> rs.getString(name)))
                .collect(Collectors.toList())
                .toArray(new String[_responseColumnNames.length]));
          }
          LOG.info("Wrote " + numTranscriptsDelivered + " to WSF response.");
          return null;
        }
        catch (PluginUserException | PluginModelException e) {
          throw new SQLRunnerException("Could not parse transcript transform result", e);
        }
      });

      return 0;
    }
    catch (DelayedResultException e) {
      throw e;
    }
    catch (Exception e) {
      throw new PluginModelException("Could not insert filtered genes into temporary table", e);
    }
  }

  private static class FilteredArgumentBatch implements ArgumentBatch {

    private final BufferedReader _reader;
    private final Predicate<String[]> _rowFilter;
    private final Function<String[],Object[]> _rowConverter;
    private final int _expectedDbRowLength;
    private int _numRowsProvided = 0;
    private int _numRowsSkipped = 0;
    private String[] _nextRow;

    public FilteredArgumentBatch(
        BufferedReader reader,
        Predicate<String[]> rowFilter,
        Function<String[],Object[]> rowConverter,
        int expectedDbRowLength) {
      _reader = reader;
      _rowFilter = rowFilter;
      _rowConverter = rowConverter;
      _expectedDbRowLength = expectedDbRowLength;
      setNextRow();
    }

    private void setNextRow() {
      try {
        _nextRow = null;
        while(_reader.ready() && _nextRow == null) {
          String[] tokens = _reader.readLine().split(TAB);
          if (_rowFilter.test(tokens)) {
            _nextRow = tokens;
          }
          else {
            _numRowsSkipped++;
          }
        }
      }
      catch (IOException e) {
        throw new RuntimeException("Failed to read input from reader", e);
      }
    }

    @Override
    public Iterator<Object[]> iterator() {
      return new Iterator<Object[]>() {

        @Override
        public boolean hasNext() {
          return _nextRow != null;
        }

        @Override
        public Object[] next() {
          if (!hasNext()) throw new NoSuchElementException();
          String[] oldNextRow = _nextRow;
          setNextRow();
          Object[] dbRow = _rowConverter.apply(oldNextRow);
          if (dbRow.length != _expectedDbRowLength) {
            throw new RuntimeException("DB row returned by rowConverter [" +
                Arrays.stream(dbRow).map(String::valueOf).collect(Collectors.joining(", ")) +
                "] must have length " + _expectedDbRowLength);
          }
          _numRowsProvided++;
          return dbRow;
        }
      };
    }

    public int getNumRowsProvided() {
      return _numRowsProvided;
    }

    public int getNumRowsSkipped() {
      return _numRowsSkipped;
    }

    @Override
    public int getBatchSize() {
      return 50;
    }

    @Override
    public Integer[] getParameterTypes() {
      Integer[] types = new Integer[_expectedDbRowLength];
      for (int i = 0; i < _expectedDbRowLength; i++) {
        types[i] = Types.VARCHAR;
      }
      return types;
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

  protected static <T> T readGetRequest(String url, Map<String,String> authHeader, FunctionWithException<JSONObject, T> mapper) throws PluginModelException {
    try (InputStream response = ClientUtil.makeAsyncGetRequest(url, APPLICATION_JSON, authHeader).getInputStream()) {
      return mapper.apply(new JSONObject(IoUtil.readAllChars(new InputStreamReader(response))));
    }
    catch (Exception e) {
      throw new PluginModelException("Unable to fetch, read, or parse response from " + url, e);
    }
  }
}
