package org.apidb.apicomplexa.wsfplugin.blast;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.apidb.apicommon.model.TranscriptUtil;
import org.gusdb.fgputil.FormatUtil;
import org.gusdb.fgputil.IoUtil;
import org.gusdb.fgputil.runtime.GusHome;
import org.gusdb.fgputil.runtime.InstanceManager;
import org.gusdb.fgputil.runtime.ThreadUtil;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.question.Question;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.DelayedResultException;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;
import org.json.JSONObject;

public class MultiblastServicePlugin extends AbstractPlugin {

  private static final Logger LOG = Logger.getLogger(MultiblastServicePlugin.class);

  // required properties in model.prop
  private static final String LOCALHOST_PROP_KEY = "LOCALHOST";
  private static final String SERVICE_URL_PROP_KEY = "MULTI_BLAST_SERVICE_URL";

  @Override
  public String[] getRequiredParameterNames() {
    return new String[] {
      "BlastDatabaseType",
      "BlastAlgorithm",
      "BlastDatabaseOrganism",
      "BlastQuerySequence",
      "BlastRecordClass", // TODO: Do we need this param for multi-blast or can we get from BlastDatabaseType?
      "ExpectationValue",
      "NumQueryResults",
      "MaxMatchesQueryRange",
      "WordSize",
      "ScoringMatrix",
      "MatchMismatchScore",
      "GapCosts",
      "CompAdjust",
      "FilterLowComplex",
      "SoftMask",
      "LowerCaseMask"
    };
  }

  @Override
  public String[] getColumns(PluginRequest request) throws PluginModelException {
    List<String> columns = new ArrayList<>();
    String questionName = request.getContext().get(Utilities.QUERY_CTX_QUESTION);
    Question q = getWdkModel(request).getQuestionByFullName(questionName).get();
    columns.addAll(Arrays.asList(q.getRecordClass().getPrimaryKeyDefinition().getColumnRefs()));
    if (TranscriptUtil.isTranscriptQuestion(q)) {
      columns.add("matched_result");
    }
    columns.addAll(Arrays.asList(new String[] {
      "summary",
      "alignment",
      "evalue_mant",
      "evalue_exp",
      "score"
    }));
    return columns.toArray(new String[0]);
  }

  @Override
  public void validateParameters(PluginRequest request) throws PluginModelException, PluginUserException {
    // let WDK handle validation for now
  }

  @Override
  protected int execute(PluginRequest request, PluginResponse response)
      throws PluginModelException, PluginUserException, DelayedResultException {

    // find base URL for multi-blast service
    String multiBlastServiceUrl = getMultiBlastServiceUrl(request);

    // use passed params to POST new job request to blast service
    String jobId = createJob(request, multiBlastServiceUrl);

    // wait 1 second
    ThreadUtil.sleep(1000);

    // query the job status (if results in cache, should return complete immediately)
    if (!jobIsComplete(multiBlastServiceUrl, jobId)) {
      throw new DelayedResultException();
    }

    // job is complete; write results to plugin response
    // TODO: read and stream results

    return 0;
  }

  private boolean jobIsComplete(String multiBlastServiceUrl, String jobId) throws PluginModelException {
    String jobIdEndpointUrl = multiBlastServiceUrl + "/jobs" + jobId;
    LOG.info("Requesting multi-blast job status at " + jobIdEndpointUrl);

    // make new job request
    Client client = ClientBuilder.newClient();
    WebTarget webTarget = client.target(jobIdEndpointUrl);
    Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
    Response jobStatusResponse = invocationBuilder.get();

    String responseBody = getResponseBody(jobStatusResponse);
    if (jobStatusResponse.getStatus() != 200) {
      throw new PluginModelException("Unexpected response from multi-blast " +
          "service while checking job status (jobId=" + jobId + "): " + jobStatusResponse.getStatus() +
          FormatUtil.NL + responseBody);
    }

    // parse response and analyze
    JSONObject responseObj = new JSONObject(responseBody);
    switch(responseObj.getString("status")) {
      case "queued":
      case "in-progress":
        return false;
      case "completed":
        return true;
      case "errored":
        throw new PluginModelException(
          "Multi-blast service job failed: " + responseObj.getString("description"));
      default:
        throw new PluginModelException(
          "Multi-blast service job status endpoint returned unrecognized status value: " + responseObj.getString("status"));
    }
  }

  private String createJob(PluginRequest request, String multiBlastServiceUrl) throws PluginModelException {
    String jobsEndpointUrl = multiBlastServiceUrl + "/jobs";
    JSONObject newJobRequestBody = buildNewJobRequestJson(request);
    LOG.info("Requesting new multi-blast job at " + jobsEndpointUrl + " with JSON body: " + newJobRequestBody.toString(2));

    // make new job request
    Client client = ClientBuilder.newClient();
    WebTarget webTarget = client.target(jobsEndpointUrl);
    Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON);
    Response newJobResponse = invocationBuilder.post(Entity.entity(newJobRequestBody.toString(), MediaType.APPLICATION_JSON));

    String responseBody = getResponseBody(newJobResponse);
    if (newJobResponse.getStatus() != 200) {
      throw new PluginModelException("Unexpected response from multi-blast " +
          "service while requesting new job: " + newJobResponse.getStatus() +
          FormatUtil.NL + responseBody);
    }

    // return job ID
    return new JSONObject(responseBody).getString("jobId");
  }

  private String getResponseBody(Response newJobResponse) throws PluginModelException {
    String responseBody = "";
    if (newJobResponse.hasEntity()) {
      try (InputStream body = (InputStream)newJobResponse.getEntity();
           ByteArrayOutputStream str = new ByteArrayOutputStream()) {
        IoUtil.transferStream(str, body);
        responseBody = str.toString();
      }
      catch (IOException e) {
        throw new PluginModelException("Unable to read response body from service response.", e);
      }
    }
    return responseBody;
  }

  private JSONObject buildNewJobRequestJson(PluginRequest request) {
    return new JSONObject();
  }

  private static WdkModel getWdkModel(PluginRequest request) {
    return InstanceManager.getInstance(WdkModel.class, GusHome.getGusHome(), request.getProjectId());
  }

  public static String getMultiBlastServiceUrl(PluginRequest request) throws PluginModelException {
    Map<String,String> modelProps = getWdkModel(request).getProperties();
    String localhost = modelProps.get(LOCALHOST_PROP_KEY);
    String siteSearchServiceUrl = modelProps.get(SERVICE_URL_PROP_KEY);
    if (localhost == null || siteSearchServiceUrl == null) {
      throw new PluginModelException("model.prop must contain the properties: " +
          LOCALHOST_PROP_KEY + ", " + SERVICE_URL_PROP_KEY);
    }
    return localhost + siteSearchServiceUrl;
  }
}
