package org.apidb.apicomplexa.wsfplugin.apifed;

import java.net.URI;
import java.util.UUID;

import org.apache.log4j.Logger;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apidb.apicomplexa.wsfplugin.wdkquery.WdkQueryPlugin;
import org.gusdb.wdk.model.ServiceResolver;
import org.gusdb.wsf.client.ClientModelException;
import org.gusdb.wsf.client.ClientRequest;
import org.gusdb.wsf.client.ClientUserException;
import org.gusdb.wsf.client.WsfClient;
import org.gusdb.wsf.client.WsfClientFactory;
import org.gusdb.wsf.client.WsfResponseListener;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginUserException;

public class ComponentQuery extends Thread implements WsfResponseListener {

  private static final long REQUEST_TOKEN_INTERVAL = 500;

  private static final Logger logger = Logger.getLogger(ComponentResult.class);

  private static WsfClientFactory _wsfClientFactory = ServiceResolver.resolve(WsfClientFactory.class);

  private final String projectId;
  private final String url;
  private final ClientRequest request;
  private final ComponentResult result;

  private boolean running;
  private boolean stopRequested;

  private int rowCount = 0;
  private int attachmentCount = 0;

  public ComponentQuery(String projectId, String url, PluginRequest pluginRequest, ComponentResult result) {
    this.projectId = projectId;
    this.url = url;

    this.request = new ClientRequest();
    this.request.setPluginClass(WdkQueryPlugin.class.getName());
    this.request.setProjectId(projectId);
    this.request.setParams(pluginRequest.getParams());
    this.request.setOrderedColumns(pluginRequest.getOrderedColumns());
    this.request.setContext(pluginRequest.getContext());

    this.result = result;
    this.running = false;
    this.stopRequested = false;
  }

  public void requestStop() {
    stopRequested = true;
  }

  public boolean isRunning() {
    return running;
  }

  @Override
  public void run() {
    running = true;
    String errorMessage = "Thread ran and exited Correctly";
    logger.info("The Thread is running for project " + projectId + ", querying URL " + url);

    try (CloseableThreadContext.Instance componentQueryContext = CloseableThreadContext
        .put("ProjectId", projectId)
        .put("QueryId", UUID.randomUUID().toString())) {
      long start = System.currentTimeMillis();

      WsfClient client = _wsfClientFactory.newClient(this, new URI(url));

      // invoke the web service, and get response
      int signal = client.invoke(request);
      if (!stopRequested) {
        while (!result.addSignal(projectId, signal)) {
          Thread.sleep(REQUEST_TOKEN_INTERVAL);
        }
      }

      long end = System.currentTimeMillis();
      // TODO: misleading.. always 0, check why, then reset level to info
      logger.trace("Thread (" + url + ") has returned " + rowCount + " results, " + attachmentCount +
          " attachments, in " + ((end - start) / 1000.0) + " seconds.");
    }
    catch (Exception ex) {
      logger.error("Error occurred related to " + url, ex);
      errorMessage = ex.getMessage() + " Occured : Thread exited" + ex.getCause();
      try {
        result.addMessage(projectId, Integer.toString(WdkQueryPlugin.STATUS_ERROR_SERVICE_UNAVAILABLE));
      }
      catch (Exception ex1) {
        throw new RuntimeException(ex1);
      }
    }
    finally {
      result.releaseToken(projectId);
      logger.debug("The Thread is stopped(" + url + ").................. : by request: " + stopRequested +
          "  Error Message = " + errorMessage);
      running = false;
    }
  }

  @Override
  public void onRowReceived(String[] row) throws ClientModelException, ClientUserException {
    if (stopRequested)
      return;
    try {
      while (!result.addRow(projectId, row)) { // cannot add row, wait for token;
        try {
          Thread.sleep(REQUEST_TOKEN_INTERVAL);
        }
        catch (InterruptedException ex) {}
        rowCount++;
      }
    }
    catch (PluginModelException ex) {
      throw new ClientModelException(ex);
    }
    catch (PluginUserException ex) {
      throw new ClientUserException(ex);
    }
  }

  @Override
  public void onAttachmentReceived(String key, String content) throws ClientModelException,
      ClientUserException {
    if (stopRequested)
      return;
    try {
      while (!result.addAttachment(projectId, key, content)) { // cannot add attachment, wait for token
        try {
          Thread.sleep(REQUEST_TOKEN_INTERVAL);
        }
        catch (InterruptedException ex) {}
        attachmentCount++;
      }
    }
    catch (PluginModelException ex) {
      throw new ClientModelException(ex);
    }
    catch (PluginUserException ex) {
      throw new ClientUserException(ex);
    }
  }

  @Override
  public void onMessageReceived(String message) throws ClientModelException, ClientUserException {
    if (stopRequested)
      return;
    try {
      while (!result.addMessage(projectId, message)) { // cannot add message, wait for token
        try {
          Thread.sleep(REQUEST_TOKEN_INTERVAL);
        }
        catch (InterruptedException ex) {}
      }
    }
    catch (PluginModelException ex) {
      throw new ClientModelException(ex);
    }
    catch (PluginUserException ex) {
      throw new ClientUserException(ex);
    }
  }
}
