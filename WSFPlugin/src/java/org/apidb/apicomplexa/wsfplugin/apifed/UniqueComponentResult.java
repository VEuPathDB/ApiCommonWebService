package org.apidb.apicomplexa.wsfplugin.apifed;

import java.util.HashSet;
import java.util.Set;

import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.WsfServiceException;
import org.json.JSONArray;

public class UniqueComponentResult extends ComponentResult {

  private final Set<String> rows = new HashSet<>();

  public UniqueComponentResult(PluginResponse response) {
    super(response);
  }

  @Override
  public synchronized void addRow(String[] row) throws WsfServiceException {
    // only store unique rows, and ignore the duplicated ones
    String key = getKey(row);
    if (!rows.contains(key)) {
      super.addRow(row);
      rows.add(key);
    }
  }

  private String getKey(String[] row) {
    JSONArray jsArray = new JSONArray();
    for (String value : row) {
      jsArray.put(value);
    }
    return jsArray.toString();
  }

}
