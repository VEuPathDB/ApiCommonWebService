/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.plasmoap;

import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;

/**
 * @author Jerric
 * @created Jan 13, 2006
 */
public class PlasmoAPPlugin extends AbstractPlugin {

  private static final Logger logger = Logger.getLogger(PlasmoAPPlugin.class);

  private static final String PROPERTY_FILE = "plasmoAP-config.xml";

  public static final String PARAM_SEQUENCE = "Sequence";

  public static final String COLUMN_REPORT = "Report";
  public static final String COLUMN_SIGNAL = "Signal";

  private static final String FIELD_PERL_EXE = "PerlExe";
  private static final String FIELD_SCRIPT = "PlasmoAPScript";

  private String perlExe;
  private String plasmoapScript;

  public PlasmoAPPlugin() {
    super(PROPERTY_FILE);
  }

  // load properties

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.AbstractPlugin#initialize(java.util.Map)
   */
  @Override
  public void initialize() throws PluginModelException {
    super.initialize();

    // load properties
    perlExe = getProperty(FIELD_PERL_EXE);
    plasmoapScript = getProperty(FIELD_SCRIPT);
    if (perlExe == null || plasmoapScript == null)
      throw new PluginModelException("The required fields in property file are missing: " + FIELD_PERL_EXE +
          ", " + FIELD_SCRIPT);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.WsfPlugin#getRequiredParameters()
   */
  @Override
  public String[] getRequiredParameterNames() {
    return new String[] { PARAM_SEQUENCE };
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.WsfPlugin#getColumns()
   */
  @Override
  public String[] getColumns(PluginRequest request) {
    return new String[] { COLUMN_REPORT, COLUMN_SIGNAL };
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#validateParameters(java.util.Map)
   */
  @Override
  public void validateParameters(PluginRequest request) {
    // do nothing in this plugin
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
   */
  @Override
  public int execute(PluginRequest request, PluginResponse response) throws PluginModelException,
      PluginUserException {
    logger.info("Invoking PlasmoAPPlugin...");

    // invoke the PlasmoAP application
    String[] command = prepareCommand(request.getParams());
    StringBuffer buffer = new StringBuffer();
    // wait for infinite time
    int signal = invokeCommand(command, buffer, 0);
    String output = buffer.toString();

    if (signal != 0)
      throw new PluginModelException("The invocation of PlasmoApPlugin is failed: " + output);

    // parse the signal out of the report
    String match = parseResult(output);

    // construct the result
    String[] orderedColumns = request.getOrderedColumns();
    String[] row = new String[orderedColumns.length];
    for (int i = 0; i < orderedColumns.length; i++) {
      if (orderedColumns[i].equalsIgnoreCase(COLUMN_REPORT)) {
        row[i] = output;
      }
      else if (orderedColumns[i].equalsIgnoreCase(COLUMN_SIGNAL)) {
        row[i] = match;
      }
    }
    response.addRow(row);
    response.setMessage(output);
    return signal;
  }

  private String[] prepareCommand(Map<String, String> params) {
    String sequence = params.get(PARAM_SEQUENCE);
    Vector<String> cmds = new Vector<String>();
    cmds.add(perlExe);
    cmds.add(plasmoapScript);
    cmds.add(sequence);
    String[] cmdArray = new String[cmds.size()];
    cmds.toArray(cmdArray);
    return cmdArray;
  }

  private String parseResult(String result) {
    String temp = result.toLowerCase();
    String flag = "the submitted sequence";
    // the flag must exist in the result
    int pos = temp.indexOf(flag.toLowerCase());
    assert pos >= 0;

    pos += flag.length();
    temp = result.substring(pos).trim();
    // find the second space
    pos = temp.indexOf(' ');
    pos = temp.indexOf(' ', pos + 1);
    temp = temp.substring(0, pos).trim();

    String signal;
    if (temp.equalsIgnoreCase("does not"))
      signal = "false";
    else
      signal = "true";
    return signal;
  }
}
