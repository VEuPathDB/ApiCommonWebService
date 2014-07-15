/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.eupathdb.common.model.InstanceManager;
import org.eupathdb.common.model.ProjectMapper;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wsf.common.PluginRequest;
import org.gusdb.wsf.common.WsfException;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.WsfPluginException;

/**
 * @author Jerric, modified by Cristina 2010 to add DNA motif
 * @created Jan 31, 2006
 */

// geneID could be an ORF or a genomic sequence deending on who uses the plugin
public abstract class AbstractMotifPlugin extends AbstractPlugin {

  private static final Logger logger = Logger.getLogger(AbstractMotifPlugin.class);
  
  protected class Match {

    public String sourceId;
    public String projectId;

    /**
     * 
     * locations contains (xxx-yyy), (xxx-yyyy), ... sequence contains sequences
     * from matches, separated by a space (so it wraps in summary page)
     */
    public String locations;
    public int matchCount = 0;
    public String sequence;

    public String getKey() {
      return sourceId + projectId;
    }

    @Override
    public int hashCode() {
      return getKey().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj != null && obj instanceof Match) {
        Match match = (Match) obj;
        return getKey().equals(match.getKey());
      } else return false;
    }
  }

  // required parameter definition
  public static final String PROPERTY_FILE = "motifSearch-config.xml";

  public static final String PARAM_DATASET = "motif_organism";
  public static final String PARAM_EXPRESSION = "motif_expression";

  // column definitions for returnd results
  public static final String COLUMN_SOURCE_ID = "SourceID";
  public static final String COLUMN_PROJECT_ID = "ProjectId";
  public static final String COLUMN_LOCATIONS = "Locations";
  public static final String COLUMN_MATCH_COUNT = "MatchCount";
  public static final String COLUMN_SEQUENCE = "Sequence";

  protected static final String MOTIF_STYLE_CLASS = "motif";

  private String regexField;
  private String defaultRegex;

  private MotifConfig config;
  private ProjectMapper projectMapper;

  protected abstract Map<Character, String> getSymbols();

  protected abstract void findMatches(PluginResponse response,
      Map<String, Integer> orders, String headline, Pattern searchPattern,
      String sequence) throws WsfException;

  protected AbstractMotifPlugin(String regexField, String defaultRegex) {
    super(PROPERTY_FILE);
    this.regexField = regexField;
    this.defaultRegex = defaultRegex;
  }

  public MotifConfig getConfig() {
    return config;
  }

  public void setConfig(MotifConfig config) {
    this.config = config;
  }

  public ProjectMapper getProjectMapper() {
    return projectMapper;
  }

  public void setProjectMapper(ProjectMapper projectMapper) {
    this.projectMapper = projectMapper;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.AbstractPlugin#initialize(java.util.Map)
   */
  @Override
  public void initialize()
      throws WsfPluginException {
    super.initialize();

    config = new MotifConfig(properties, regexField, defaultRegex);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.WsfPlugin#getRequiredParameters()
   */
  @Override
  public String[] getRequiredParameterNames() {
    return new String[] { PARAM_EXPRESSION, PARAM_DATASET };
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.WsfPlugin#getColumns()
   */
  @Override
  public String[] getColumns() {
    return new String[] { COLUMN_SOURCE_ID, COLUMN_PROJECT_ID,
        COLUMN_LOCATIONS, COLUMN_MATCH_COUNT, COLUMN_SEQUENCE };
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#validateParameters(java.util.Map)
   */
  @Override
  public void validateParameters(PluginRequest request)
       {
    // do nothing in this plugin
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
   */
  @Override
  public int execute(PluginRequest request, PluginResponse response)
      throws WsfPluginException {
    logger.info("Invoking MotifSearchPlugin...");

    String projectId = request.getProjectId();
    try {
      WdkModel wdkModel = InstanceManager.getInstance(WdkModel.class, projectId);
      projectMapper = ProjectMapper.getMapper(wdkModel);
    }
    catch (WdkModelException ex) {
      throw new WsfPluginException(ex);
    }
    
    Map<String, String> params = request.getParams();
    // create a column order map
    String[] orderedColumns = request.getOrderedColumns();
    Map<String, Integer> orders = new HashMap<String, Integer>();
    for (int i = 0; i < orderedColumns.length; i++)
      orders.put(orderedColumns[i], i);

    // get required parameters
    String datasetIDs = params.get(PARAM_DATASET);

    // translate the expression
    String expression = params.get(PARAM_EXPRESSION);
    Pattern searchPattern = translateExpression(expression);

    logger.debug("datasetIDs: " + datasetIDs);
    logger.debug("expression: " + expression);

    // open the flatfile database assigned by the user
    try {
      String[] dsIds = datasetIDs.split(",");

      // scan on each dataset, and add matched motifs in the result
      for (String dsId : dsIds) {
        logger.debug("execute(): dsId: " + dsId);
        // parent organisms in a treeParam, we only need the leave nodes
        if (dsId.equals("-1") || dsId.length() <= 3) {
          logger.debug("organism value: (" + dsId
              + ") not included, we only care for leave nodes\n");
          continue;
        }

        findMatches(response, dsId.trim(), searchPattern,
            config.getContextLength(), orders);
      }
      return 0;
    } catch (Exception ex) {
      throw new WsfPluginException(ex);
    }
  }

  /**
   * @param fileID
   * @return
   */
  private File openDataFile(String datasetID) throws IOException {
    logger.info("openDataFile() - datasetID: " + datasetID + "\n");

    File dataFile = new File(datasetID);
    if (!dataFile.exists()) throw new IOException("The dataset \""
        + dataFile.toString() + "\" cannot be found.");
    else return dataFile;
  }

  private Pattern translateExpression(String expression) {
    Map<Character, String> codes = getSymbols();

    boolean inSquareBraces = false, inCurlyBraces = false;
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < expression.length(); i++) {
      char ch = Character.toUpperCase(expression.charAt(i));
      boolean skipChar = false;
      if (ch == '{') inCurlyBraces = true;
      else if (ch == '}') inCurlyBraces = false;
      else if (ch == '[') inSquareBraces = true;
      else if (ch == ']') inSquareBraces = false;
      else if (!inCurlyBraces && codes.containsKey(ch)) {
        // the char is not in any curly braces, and is a known code;
        // replace the char with the actual string.
        String replace = codes.get(ch);
        if (!inSquareBraces) replace = "[" + replace + "]";
        builder.append(replace);
        skipChar = true;
      }
      if (!skipChar) builder.append(ch);
    }
    logger.debug("translated expression: " + builder);

    int option = Pattern.CASE_INSENSITIVE;
    return Pattern.compile(builder.toString(), option);
  }

  private void findMatches(PluginResponse response, String datasetID,
      Pattern searchPattern, int contextLength, 
      Map<String, Integer> orders) throws IOException, WsfException {
    File datasetFile = openDataFile(datasetID);
    BufferedReader in = new BufferedReader(new FileReader(datasetFile));

    // read header of the first sequence
    String headline = null, line;
    StringBuilder sequence = new StringBuilder();
    try {
      while ((line = in.readLine()) != null) {
        line = line.trim();
        if (line.length() == 0) continue;

        if (line.charAt(0) == '>') {
          // starting of a new sequence, process the previous sequence if
          // have any;
          if (sequence.length() > 0) {
            findMatches(response, orders, headline, searchPattern,
                sequence.toString());

            // clear the sequence buffer to be ready for the next one
            sequence = new StringBuilder();
          }
          headline = line;
        } else {
          sequence.append(line);
        }
      }
    } finally {
      in.close();
    }

    // process the last sequence, if it hasn't been processed
    if (headline != null && sequence.length() > 0) {
      findMatches(response, orders, headline, searchPattern,
          sequence.toString());
    }
  }

  protected void addMatch(PluginResponse response, Match match,
      Map<String, Integer> orders) throws WsfException {
    String[] result = new String[orders.size()];
    result[orders.get(COLUMN_PROJECT_ID)] = match.projectId;
    result[orders.get(COLUMN_SOURCE_ID)] = match.sourceId;
    result[orders.get(COLUMN_LOCATIONS)] = match.locations;
    result[orders.get(COLUMN_MATCH_COUNT)] = Integer.toString(match.matchCount);
    result[orders.get(COLUMN_SEQUENCE)] = match.sequence;
    // logger.debug("result " + resultToString(result) + "\n");
    response.addRow(result);
  }

  protected String getProjectId(String organism) throws SQLException {
    return projectMapper.getProjectByOrganism(organism);
  }

  protected String getLocation(int length, int start, int stop, boolean reversed) {
    // show the location at base 1.
    if (reversed) {
      int newStart = length - stop + 1;
      stop = length - start + 1;
      start = newStart;
    } else {
      start += 1;
      stop += 1;
    }
    String location = Integer.toString(start);
    if (start != stop) location += "-" + stop;
    return location;
  }

}
