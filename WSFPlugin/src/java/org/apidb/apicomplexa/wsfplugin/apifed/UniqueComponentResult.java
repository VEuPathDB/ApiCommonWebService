package org.apidb.apicomplexa.wsfplugin.apifed;

import java.util.HashSet;
import java.util.Set;

import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.WsfException;
import org.json.JSONArray;

public class UniqueComponentResult extends ComponentResult {

  private final Set<String> rows = new HashSet<>();

  public UniqueComponentResult(PluginResponse response) {
    super(response);
  }

  @Override
  public synchronized boolean addRow(String token, String[] row) throws WsfException {
    // only store unique rows, and ignore the duplicated ones
    String key = getKey(row);
    if (!rows.contains(key)) {
      if (super.addRow(token, row)) {
        rows.add(key);
        return true;
      } else {
        return false;
      }
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
