package org.apidb.apicomplexa.wsfplugin.apifed;

import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;

public class ComponentResult {
  
  public static final String SIGNAL_SUFFIX = "-signal";

  protected final PluginResponse response;

  private String _message = "";
  private int _signal = 0;
  private String _token = null;
  private int _rowCount = 0;

  /**
   * create a result container without tokens.
   * 
   * @param response
   */
  protected ComponentResult(PluginResponse response) {
    this.response = response;
  }
  
  public int getSignal() {
    return _signal;
  }

  private synchronized boolean requestToken(String projectId) {
    if (_token == null) { // no token set, set the token
      _token = projectId;
      return true;
    }
    else
      // toke already set, check if the token is the same as the stored one.
      return _token.equals(projectId);
  }

  public synchronized void releaseToken(String projectId) {
    // only release it if it's the same token
    if (_token != null && _token.equals(projectId))
      _token = null;
  }

  public boolean addRow(String projectId, String[] row) throws PluginModelException, PluginUserException {
    if (requestToken(projectId)) {
      response.addRow(row);
      _rowCount++;
      return true;
    }
    else
      return false;
  }

  public int getRowCount() {
    return _rowCount;
  }

  public boolean addMessage(String projectId, String message) throws PluginModelException, PluginUserException  {
    if (requestToken(projectId)) {
      if (_message.length() > 0)
        _message += ",";
      _message += projectId + ":" + message;
      response.setMessage(_message);
      return true;
    }
    else
      return false;
  }

  public boolean addAttachment(String projectId, String key, String content) throws PluginModelException, PluginUserException  {
    if (requestToken(projectId)) {
      response.addAttachment(key, content);
      return true;
    } else return false;
  }
  
  public boolean addSignal(String projectId, int signal) throws PluginModelException, PluginUserException  {
    if (requestToken(projectId)) {
      response.addAttachment(projectId + SIGNAL_SUFFIX, Integer.toString(signal));
      if (signal != 0) _signal = signal;
      return true;
    } else return false;
  }
}
