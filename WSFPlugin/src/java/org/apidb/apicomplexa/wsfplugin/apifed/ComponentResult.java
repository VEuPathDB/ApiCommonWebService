package org.apidb.apicomplexa.wsfplugin.apifed;

import java.util.Map;

import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.WsfPluginException;

public class ComponentResult {

  protected final PluginResponse response;
  private final String[] tokens;

  private int tokenIndex = 0;

  /**
   * create a result container without tokens.
   * 
   * @param response
   */
  protected ComponentResult(PluginResponse response) {
    this(response, null);
  }

  /**
   * Create a result container with tokens. the ComponentQuery has to obtain the
   * correct token in order to write rows. After a ComponentQuery is done, it
   * should call releaseToekn() in order for other ComponentQueries to write.
   * 
   * @param response
   * @param tokens
   */
  protected ComponentResult(PluginResponse response, String[] tokens) {
    this.response = response;
    this.tokens = tokens;
    this.tokenIndex = 0;
  }

  public boolean requestToken(String token) {
    if (tokens == null) return true;
    if (tokenIndex >= tokens.length) return false;
    return tokens[tokenIndex].equals(token);
  }

  public void releaseToken(String token) {
    if (tokens != null && tokenIndex < tokens.length) {
      if (tokens[tokenIndex].equals(token)) tokenIndex++;
    }
  }

  public boolean addRow(String token, String[] row) throws WsfPluginException {
    if (requestToken(token)) {
      response.addRow(row);
      return true;
    } else return false;
  }

  public synchronized void setMessage(String projectId, String message) {
    String previous = response.getMessage();
    if (previous != null && previous.length() > 0) {
      previous = previous + ",";
    } else previous = "";
    message = previous + projectId + ":" + message;
    response.setMessage(message);
  }

  public void addAttachments(Map<String, String> attachments) {
    response.addAttachments(attachments);
  }

  public void setSignal(int signal) {
    response.setSignal(signal);
  }
}
