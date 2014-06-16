package org.apidb.apicomplexa.wsfplugin.apifed;

import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.WsfException;

public class ComponentResult {
  
  public static final String SIGNAL_SUFFIX = "-signal";

  protected final PluginResponse response;

  private String message = "";
  private int signal = 0;
  private String token = null;
  private int rowCount = 0;

  /**
   * create a result container without tokens.
   * 
   * @param response
   */
  protected ComponentResult(PluginResponse response) {
    this.response = response;
  }
  
  public int getSignal() {
    return signal;
  }

  private synchronized boolean requestToken(String projectId) {
    if (this.token == null) { // no token set, set the token
      this.token = projectId;
      return true;
    }
    else
      // toke already set, check if the token is the same as the stored one.
      return this.token.equals(projectId);
  }

  public synchronized void releaseToken(String projectId) {
    // only release it if it's the same token
    if (this.token != null && this.token.equals(projectId))
      this.token = null;
  }

  public boolean addRow(String projectId, String[] row) throws WsfException {
    if (requestToken(projectId)) {
      response.addRow(row);
      rowCount++;
      return true;
    }
    else
      return false;
  }

  public int getRowCount() {
    return rowCount;
  }

  public boolean addMessage(String projectId, String message) throws WsfException {
    if (requestToken(projectId)) {
      if (this.message.length() > 0)
        this.message += ",";
      this.message += projectId + ":" + message;
      response.setMessage(this.message);
      return true;
    }
    else
      return false;
  }

  public boolean addAttachment(String projectId, String key, String content) throws WsfException {
    if (requestToken(projectId)) {
      response.addAttachment(key, content);
      return true;
    } else return false;
  }
  
  public boolean addSignal(String projectId, int signal) throws WsfException {
    if (requestToken(projectId)) {
      response.addAttachment(projectId + SIGNAL_SUFFIX, Integer.toString(signal));
      if (signal != 0) this.signal = signal;
      return true;
    } else return false;
  }
}
