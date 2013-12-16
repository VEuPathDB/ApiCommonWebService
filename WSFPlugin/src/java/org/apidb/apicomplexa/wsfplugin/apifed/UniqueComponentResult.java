package org.apidb.apicomplexa.wsfplugin.apifed;

import java.util.HashSet;
import java.util.Set;

import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.WsfPluginException;
import org.json.JSONArray;

public class UniqueComponentResult extends ComponentResult {

  private final Set<String> rows = new HashSet<>();

  public UniqueComponentResult(PluginResponse response) {
    super(response);
  }

  public UniqueComponentResult(PluginResponse response, String[] tokens) {
    super(response, tokens);
  }
  
  @Override
  public synchronized boolean addRow(String token, String[] row) throws WsfPluginException {
    // only store unique rows, and ignore the duplicated ones
    String key = getKey(row);
    if (!rows.contains(key)) {
      super.addRow(token, row);
      return rows.add(key);
    }
    return true;
  }

  private String getKey(String[] row) {
    JSONArray jsArray = new JSONArray();
    for (String value : row) {
      jsArray.put(value);
    }
    return jsArray.toString();
  }

}
