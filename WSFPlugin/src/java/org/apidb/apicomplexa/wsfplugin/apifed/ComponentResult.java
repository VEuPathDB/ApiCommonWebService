package org.apidb.apicomplexa.wsfplugin.apifed;

import java.util.Map;

import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.WsfServiceException;

public class ComponentResult {

  protected final PluginResponse response;

  protected ComponentResult(PluginResponse response) {
    this.response = response;
  }

  public void addRow(String[] row) throws WsfServiceException {
    response.addRow(row);
  }

  public synchronized void setMessage(String projectId, String message) {
    String previous = response.getMessage();
    if (previous != null && previous.length() > 0) {
      message = previous + ",";
    }
    message = projectId + ":" + message;
    response.setMessage(message);
  }

  public void addAttachments(Map<String, String> attachments) {
    response.addAttachments(attachments);
  }

  public void setSignal(int signal) {
    response.setSignal(signal);
  }
}
