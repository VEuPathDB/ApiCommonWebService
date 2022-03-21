package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.eupathdb.common.model.ProjectMapper;
import org.gusdb.fgputil.functional.FunctionalInterfaces.ConsumerWithException;
import org.gusdb.fgputil.functional.FunctionalInterfaces.FunctionWithException;
import org.gusdb.fgputil.runtime.InstanceManager;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;

/**
 * @author Jerric, modified by Cristina 2010 to add DNA motif
 * @created Jan 31, 2006
 */
public abstract class AbstractMotifPlugin extends AbstractPlugin {

  private static final Logger LOG = Logger.getLogger(AbstractMotifPlugin.class);

  public interface MatchFinder {
    /**
     * Finds matches of the passed pattern in the given file and submits them to the consumer
     *
     * @param fastaFile file to read
     * @param searchPattern pattern to search for
     * @param consumer consumes the matches, writing them to the plugin response
     * @param orgToProjectId function that looks up projectId by organism
     */
    void findMatches(
        File fastaFile,
        Pattern searchPattern,
        ConsumerWithException<PluginMatch> consumer,
        FunctionWithException<String, String> orgToProjectId) throws Exception;
  }

  // motif search property file
  public static final String PROPERTY_FILE = "motifSearch-config.xml";

  // required parameter definition
  public static final String PARAM_DATASET = "motif_organism";
  public static final String PARAM_EXPRESSION = "motif_expression";
  private static final String[] REQUIRED_PARAMETER_NAMES = new String[] {
      PARAM_EXPRESSION,
      PARAM_DATASET
  };

  // column definitions for returned results
  public static final String COLUMN_SOURCE_ID = "SourceID";
  public static final String COLUMN_PROJECT_ID = "ProjectId";
  public static final String COLUMN_LOCATIONS = "Locations";
  public static final String COLUMN_MATCH_COUNT = "MatchCount";
  public static final String COLUMN_SEQUENCE = "Sequence";
  public static final String COLUMN_MATCH_SEQUENCES = "MatchSequences";
  private static final String[] OUTPUT_COLUMNS = new String[] {
      COLUMN_SOURCE_ID,
      COLUMN_PROJECT_ID,
      COLUMN_LOCATIONS,
      COLUMN_MATCH_COUNT,
      COLUMN_SEQUENCE,
      COLUMN_MATCH_SEQUENCES
  };

  // CSS style to color matches within context
  protected static final String MOTIF_STYLE_CLASS = "motif";

  // provides record type specific symbol translation in the submitted pattern
  protected abstract Map<Character, String> getSymbols();

  // provides record type specific match finder
  protected abstract MatchFinder getMatchFinder(MotifConfig config);

  // fields initialized in constructor
  private final String _regexField;
  private final String _defaultRegex;

  // fields initialized in initialize()
  private MotifConfig _config;
  private ProjectMapper _projectMapper;
  private Map<String, Integer> _columnOrders;
  
  protected AbstractMotifPlugin(String regexField, String defaultRegex) {
    super(PROPERTY_FILE);
    _regexField = regexField;
    _defaultRegex = defaultRegex;
  }

  @Override
  public void initialize(PluginRequest request)
      throws PluginModelException {
    super.initialize(request);

    // create motif-specific config
    _config = new MotifConfig(properties, _regexField, _defaultRegex);

    // create project mapper
    String projectId = request.getProjectId();
    try {
      WdkModel wdkModel = InstanceManager.getInstance(WdkModel.class, projectId);
      _projectMapper = ProjectMapper.getMapper(wdkModel);
    }
    catch (WdkModelException ex) {
      throw new PluginModelException(ex);
    }

    // create a column order map
    String[] orderedColumnArray = request.getOrderedColumns();
    _columnOrders = new HashMap<String, Integer>();
    for (int i = 0; i < orderedColumnArray.length; i++)
      _columnOrders.put(orderedColumnArray[i], i);
  }

  @Override
  public String[] getRequiredParameterNames() {
    return REQUIRED_PARAMETER_NAMES;
  }

  @Override
  public String[] getColumns(PluginRequest request) {
    return OUTPUT_COLUMNS;
  }

  @Override
  public void validateParameters(PluginRequest request) {
    // do nothing in this plugin
  }

  @Override
  public int execute(PluginRequest request, PluginResponse response)
      throws PluginModelException {
    LOG.info("Invoking MotifSearchPlugin...");

    Map<String, String> params = request.getParams();

    // get required parameters
    String datasetIDs = params.get(PARAM_DATASET);

    // get and translate the expression
    String expression = params.get(PARAM_EXPRESSION);
    Pattern searchPattern = translateExpression(expression, getSymbols());

    LOG.debug("datasetIDs: " + datasetIDs);
    LOG.debug("expression: " + expression);

    // open the flatfile database assigned by the user
    try {
      String[] dsIds = datasetIDs.split(",");

      // scan on each dataset, and add matched motifs in the result
      for (String dsId : dsIds) {
        LOG.debug("execute(): dsId: " + dsId);
        // parent organisms in a treeParam, we only need the leave nodes
        if (dsId.equals("-1") || dsId.length() <= 3) {
          LOG.debug("organism value: (" + dsId
              + ") not included; we only care about leaf nodes\n");
          continue;
        }

        getMatchFinder(_config).findMatches(
            openDataFile(dsId.trim()),
            searchPattern,
            match -> addMatch(match, response, _columnOrders),
            _projectMapper::getProjectByOrganism);
      }
      return 0;
    }
    catch (Exception e) {
      // wrap with PluginModelException only if needed
      throw e instanceof PluginModelException
        ? (PluginModelException)e
        : new PluginModelException(e);
    }
  }

  private static File openDataFile(String datasetID) throws IOException {
    LOG.info("openDataFile() - datasetID: " + datasetID + "\n");

    File dataFile = new File(datasetID);
    if (!dataFile.exists()) throw new IOException("The dataset \""
        + dataFile.toString() + "\" cannot be found.");
    else return dataFile;
  }

  public static Pattern translateExpression(String expression, Map<Character, String> codes) {
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
    LOG.debug("translated expression: " + builder);

    int option = Pattern.CASE_INSENSITIVE;
    return Pattern.compile(builder.toString(), option);
  }

  protected void addMatch(PluginMatch match, PluginResponse response,
                          Map<String, Integer> columnOrders) throws PluginModelException, PluginUserException  {
    String[] result = new String[columnOrders.size()];
    result[columnOrders.get(COLUMN_PROJECT_ID)] = match.projectId;
    result[columnOrders.get(COLUMN_SOURCE_ID)] = match.sourceId;
    result[columnOrders.get(COLUMN_LOCATIONS)] = match.locations;
    result[columnOrders.get(COLUMN_MATCH_COUNT)] = Integer.toString(match.matchCount);
    result[columnOrders.get(COLUMN_SEQUENCE)] = match.sequence;
    result[columnOrders.get(COLUMN_MATCH_SEQUENCES)] = String.join(", ", match.matchSequences);
    // logger.debug("result " + resultToString(result) + "\n");
    response.addRow(result);
  }

  public static String formatLocation(int length, int start, int stop, boolean reversed) {
    // show the location at base 1.
    if (reversed) {
      int newStart = length - stop + 1;
      stop = length - start + 1;
      start = newStart;
    }
    else {
      start += 1;
      stop += 1;
    }
    String location = Integer.toString(start);
    if (start != stop) {
      location += "-" + stop;
    }
    return location;
  }

}
