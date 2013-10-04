package org.apidb.apicomplexa.wsfplugin.apifed;

import java.net.URL;

import org.apache.log4j.Logger;
import org.apidb.apicomplexa.wsfplugin.wdkquery.WdkQueryPlugin;
import org.gusdb.wsf.client.WsfResponse;
import org.gusdb.wsf.client.WsfService;
import org.gusdb.wsf.client.WsfServiceServiceLocator;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.service.WsfRequest;

public class ComponentQuery extends Thread {

  private static final Logger logger = Logger.getLogger(ComponentResult.class);

  private final String projectId;
  private final String url;
  private final WsfRequest request;
  private final ComponentResult result;

  private boolean running;
  private boolean stopRequested;

  public ComponentQuery(String projectId, String url,
      PluginRequest pluginRequest, ComponentResult result) {
    this.projectId = projectId;
    this.url = url;
    this.request = new WsfRequest(pluginRequest);
    this.request.setPluginClass(WdkQueryPlugin.class.getName());
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

  @SuppressWarnings("unchecked")
  @Override
  public void run() {
    running = true;
    String errorMessage = "Thread ran and exited Correctly";
    logger.info("The Thread is running.................." + url);
    WsfServiceServiceLocator locator = new WsfServiceServiceLocator();

    try {
      WsfService service = locator.getWsfService(new URL(url));
      long start = System.currentTimeMillis();
      
      

      // invoke the web service, and get response
      WsfResponse wsfResponse = service.invoke(request.toString());
      int invokeId = wsfResponse.getInvokeId();
      int pages = wsfResponse.getPageCount();

      // set the additional info from the first response; those info might not
      // be present in the subsequent responses.
      result.setMessage(projectId, wsfResponse.getMessage());
      result.setSignal(wsfResponse.getSignal());
      result.addAttachments(wsfResponse.getAttachments());

      int pageId = 0;
      while (!stopRequested) {
        String[][] resultArray = wsfResponse.getResult();

        logger.debug("caching page " + pageId + "/" + pages + ", " + resultArray.length + " rows...");

        for (int i = 0; i < resultArray.length; i++) {
          result.addRow(resultArray[i]);
        }
        // advance to the next page.
        pageId++;
        if (pageId >= pages)
          break;
        wsfResponse = service.requestResult(invokeId, pageId);
      }

      long end = System.currentTimeMillis();
      logger.info("Thread (" + url + ") has returned results in "
          + ((end - start) / 1000.0) + " seconds.");
    } catch (Exception ex) {
      logger.error("Error occurred.", ex);
      errorMessage = ex.getMessage() + " Occured : Thread exited"
          + ex.getCause();
      result.setMessage(projectId,
          Integer.toString(WdkQueryPlugin.STATUS_ERROR_SERVICE_UNAVAILABLE));
    } finally {
      logger.debug("The Thread is stopped(" + url
          + ").................. : by request: " + stopRequested
          + "  Error Message = " + errorMessage);
      running = false;
    }
  }
}
