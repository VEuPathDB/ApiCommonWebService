/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.plasmoap;

import java.io.IOException;
import java.util.Map;
import java.util.Vector;

import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author Jerric
 * @created Jan 13, 2006
 */
public class PlasmoAPPlugin extends WsfPlugin {

    private static final String PROPERTY_FILE = "plasmoAP-config.xml";

    public static final String PARAM_SEQUENCE = "Sequence";

    public static final String COLUMN_REPORT = "Report";
    public static final String COLUMN_SIGNAL = "Signal";

    private static final String FIELD_PERL_EXE = "PerlExe";
    private static final String FIELD_SCRIPT = "PlasmoAPScript";

    private static String perlExe;
    private static String plasmoapScript;

    /**
     * @throws WsfServiceException
     * 
     */
    public PlasmoAPPlugin() throws WsfServiceException {
        super(PROPERTY_FILE);
        // load properties
        perlExe = getProperty(FIELD_PERL_EXE);
        plasmoapScript = getProperty(FIELD_SCRIPT);
        if (perlExe == null || plasmoapScript == null)
            throw new WsfServiceException(
                    "The required fields in property file are missing: "
                            + FIELD_PERL_EXE + ", " + FIELD_SCRIPT);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getRequiredParameters()
     */
    @Override
    protected String[] getRequiredParameterNames() {
        return new String[] { PARAM_SEQUENCE };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getColumns()
     */
    @Override
    protected String[] getColumns() {
        return new String[] { COLUMN_REPORT, COLUMN_SIGNAL };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#validateParameters(java.util.Map)
     */
    @Override
    protected void validateParameters(Map<String, String> params)
            throws WsfServiceException {
    // do nothing in this plugin
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
     */
    @Override
    protected String[][] execute(String invokeKey, Map<String, String> params,
            String[] orderedColumns) throws WsfServiceException {
        logger.info("Invoking PlasmoAPPlugin...");

        try {
            // invoke the PlasmoAP application
            String[] command = prepareCommand(params);
            String report = invokeCommand(command, 0); // wait for infinite
            // time

            if (exitValue != 0)
                throw new WsfServiceException(
                        "The invocation of PlasmoApPlugin is failed: " + report);

            // parse the signal out of the report
            String signal = parseResult(report);

            // construct the result
            String[][] result = new String[1][orderedColumns.length];
            for (int i = 0; i < orderedColumns.length; i++) {
                if (orderedColumns[i].equalsIgnoreCase(COLUMN_REPORT)) {
                    result[0][i] = report;
                } else if (orderedColumns[i].equalsIgnoreCase(COLUMN_SIGNAL)) {
                    result[0][i] = signal;
                }
            }
            return result;
        } catch (IOException ex) {
            logger.error(ex);
            throw new WsfServiceException(ex);
        }
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
        if (temp.equalsIgnoreCase("does not")) signal = "false";
        else signal = "true";
        return signal;
    }
}
