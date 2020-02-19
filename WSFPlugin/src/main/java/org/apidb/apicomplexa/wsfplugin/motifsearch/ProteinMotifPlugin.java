/**
 * 
 */
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

// geneID could be an ORF or a genomic sequence deending on who uses the plugin
public class ProteinMotifPlugin extends AAMotifPlugin {

  public static final String COLUMN_GENE_SOURCE_ID = "gene_source_id";
  public static final String COLUMN_MATCHED_RESULT = "matched_result";

  public ProteinMotifPlugin() {
    super(FIELD_REGEX, DEFAULT_REGEX);
  }

  /**
   * This constructor is provided to support children extension for motif search
   * of other protein related types, such as ORF, etc.
   * 
   * @param regexField
   * @param defaultRegex
   */
  public ProteinMotifPlugin(String regexField, String defaultRegex) {
    super(regexField, defaultRegex);
  }

  @Override
  public String[] getColumns(PluginRequest request) {
    return new String[] { COLUMN_SOURCE_ID, COLUMN_GENE_SOURCE_ID, COLUMN_PROJECT_ID, COLUMN_MATCHED_RESULT,
        COLUMN_LOCATIONS, COLUMN_MATCH_COUNT, COLUMN_SEQUENCE };
  }

  @Override
  protected void addMatch(PluginResponse response, Match match,
      Map<String, Integer> orders) throws PluginModelException, PluginUserException  {
    String[] result = new String[orders.size()];
    result[orders.get(COLUMN_PROJECT_ID)] = match.projectId;
    result[orders.get(COLUMN_SOURCE_ID)] = match.sourceId;
    result[orders.get(COLUMN_GENE_SOURCE_ID)] = null;
    result[orders.get(COLUMN_MATCHED_RESULT)] = "Y";
    result[orders.get(COLUMN_LOCATIONS)] = match.locations;
    result[orders.get(COLUMN_MATCH_COUNT)] = Integer.toString(match.matchCount);
    result[orders.get(COLUMN_SEQUENCE)] = match.sequence;
    response.addRow(result);
  }

}
