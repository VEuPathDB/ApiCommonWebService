package org.apidb.apicomplexa.wsfplugin.apifed;

import java.net.URI;

import org.apache.log4j.Logger;
import org.apidb.apicomplexa.wsfplugin.wdkquery.WdkQueryPlugin;
import org.gusdb.wsf.client.WsfClient;
import org.gusdb.wsf.client.WsfClientBuilder;
import org.gusdb.wsf.client.WsfResponseListener;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.WsfException;
import org.gusdb.wsf.service.WsfRequest;

public class ComponentQuery extends Thread implements WsfResponseListener {

  private static final long REQUEST_TOKEN_INTERVAL = 500;

  private static final Logger logger = Logger.getLogger(ComponentResult.class);

  private final String projectId;
  private final String url;
  private final WsfRequest request;
  private final ComponentResult result;

  private boolean running;
  private boolean stopRequested;

  private int rowCount = 0;
  private int attachmentCount = 0;

  public ComponentQuery(String projectId, String url, PluginRequest pluginRequest, ComponentResult result) {
    this.projectId = projectId;
    this.url = url;
    this.request = new WsfRequest(pluginRequest);
    this.request.setPluginClass(WdkQueryPlugin.class.getName());
    this.request.setProjectId(projectId);
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
    logger.info("The Thread is running.................." + url);

    try {
      long start = System.currentTimeMillis();

      WsfClient client = WsfClientBuilder.newClient(this, new URI(url));

      // invoke the web service, and get response
      int signal = client.invoke(request);
      if (!stopRequested) {
        while (!result.addSignal(projectId, signal)) {
          Thread.sleep(REQUEST_TOKEN_INTERVAL);
        }
      }

      long end = System.currentTimeMillis();
      logger.info("Thread (" + url + ") has returned " + rowCount + " results, " + attachmentCount +
          " attachments, in " + ((end - start) / 1000.0) + " seconds.");
    }
    catch (Exception ex) {
      logger.error("Error occurred related to " + url, ex);
      errorMessage = ex.getMessage() + " Occured : Thread exited" + ex.getCause();
      try {
        result.addMessage(projectId, Integer.toString(WdkQueryPlugin.STATUS_ERROR_SERVICE_UNAVAILABLE));
      }
      catch (WsfException ex1) {
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
  public void onRowReceived(String[] row) throws WsfException {
    if (stopRequested)
      return;
    while (!result.addRow(projectId, row)) { // cannot add row, wait for token;
      try {
        Thread.sleep(REQUEST_TOKEN_INTERVAL);
      }
      catch (InterruptedException ex) {}
      rowCount++;
    }
  }

  @Override
  public void onAttachmentReceived(String key, String content) throws WsfException {
    if (stopRequested)
      return;
    while (!result.addAttachment(projectId, key, content)) { // cannot add attachment, wait for token
      try {
        Thread.sleep(REQUEST_TOKEN_INTERVAL);
      }
      catch (InterruptedException ex) {}
      attachmentCount++;
    }
  }

  @Override
  public void onMessageReceived(String message) throws WsfException {
    if (stopRequested)
      return;
    while (!result.addMessage(projectId, message)) { // cannot add message, wait for token
      try {
        Thread.sleep(REQUEST_TOKEN_INTERVAL);
      }
      catch (InterruptedException ex) {}
    }
  }
}
