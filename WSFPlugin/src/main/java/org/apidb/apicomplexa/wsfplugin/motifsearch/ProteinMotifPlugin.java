package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.util.Map;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;

/**
 * @author Jerric, modified by Cristina 2010 to add DNA motif
 * @created Jan 31, 2006
 */
public class ProteinMotifPlugin extends AAMotifPlugin {

  private static final String COLUMN_GENE_SOURCE_ID = "gene_source_id";
  private static final String COLUMN_MATCHED_RESULT = "matched_result";

  private static final String CUSTOM_FIELD_REGEX = "ProteinDeflineRegex";

  public ProteinMotifPlugin() {
    super(CUSTOM_FIELD_REGEX);
  }

  @Override
  public String[] getColumns(PluginRequest request) {
    return new String[] {
        COLUMN_SOURCE_ID,
        COLUMN_GENE_SOURCE_ID,
        COLUMN_PROJECT_ID,
        COLUMN_MATCHED_RESULT,
        COLUMN_LOCATIONS,
        COLUMN_MATCH_COUNT,
        COLUMN_SEQUENCE,
        COLUMN_MATCH_SEQUENCES
    };
  }

  @Override
  protected void addMatch(PluginMatch match, PluginResponse response,
                          Map<String, Integer> pluginOrders) throws PluginModelException, PluginUserException  {
    String[] result = new String[pluginOrders.size()];
    result[pluginOrders.get(COLUMN_PROJECT_ID)] = match.projectId;
    result[pluginOrders.get(COLUMN_SOURCE_ID)] = match.sourceId;
    result[pluginOrders.get(COLUMN_GENE_SOURCE_ID)] = null;
    result[pluginOrders.get(COLUMN_MATCHED_RESULT)] = "Y";
    result[pluginOrders.get(COLUMN_LOCATIONS)] = match.locations;
    result[pluginOrders.get(COLUMN_MATCH_COUNT)] = Integer.toString(match.matchCount);
    result[pluginOrders.get(COLUMN_SEQUENCE)] = match.sequence;
    result[pluginOrders.get(COLUMN_MATCH_SEQUENCES)] = String.join(", ", match.matchSequences);
    response.addRow(result);
  }

}
