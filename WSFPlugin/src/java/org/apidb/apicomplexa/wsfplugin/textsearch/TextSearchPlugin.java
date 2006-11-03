/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.textsearch;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author John I
 * @created Aug 23, 2006
 */
public class TextSearchPlugin extends WsfPlugin {

    private class Match {
        public String geneID;
    }

    private String scriptDir;

    private static final String PROPERTY_FILE = "textSearch-config.xml";

    // required parameter definition
    public static final String PARAM_TEXT_EXPRESSION = "text_expression";
    public static final String PARAM_CASE_INDEPENDENT = "case_independent";
    public static final String PARAM_DATASETS = "datasets";
    public static final String PARAM_MAX_PVALUE = "max_pvalue";
    public static final String PARAM_SPECIES_NAME = "species_name";
    public static final String PARAM_WHOLE_WORDS = "whole_words";

    public static final String COLUMN_GENE_ID = "GeneID";

    // field definition
    private static final String FIELD_DATA_DIR = "DataDir";
    private static final String FIELD_SCRIPT_DIR = "ScriptDir";

    private File dataDir;
    private String sourceIdRegex;
    private int maxLen;

    /**
     * @throws WsfServiceException
     * 
     */
    public TextSearchPlugin() throws WsfServiceException {
	super(PROPERTY_FILE);
	// load properties
	String dir = getProperty(FIELD_DATA_DIR);
	if (dir == null)
	    throw new WsfServiceException(
					  "The required field in property file is missing: "
					  + FIELD_DATA_DIR);
        dataDir = new File(dir);
        logger.info("constructor(): dataDir: " + dataDir.getName() + "\n");

	scriptDir = getProperty(FIELD_SCRIPT_DIR);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getRequiredParameters()
     */
    @Override
    protected String[] getRequiredParameterNames() {
        return new String[] { PARAM_TEXT_EXPRESSION, PARAM_DATASETS };
    }


    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getColumns()
     */
    @Override
    protected String[] getColumns() {
        return new String[] { COLUMN_GENE_ID };
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
    protected String[][] execute(Map<String, String> params,
            String[] orderedColumns) throws WsfServiceException {
        logger.info("Invoking TextSearchPlugin...");

        // get parameters
        String datasets = params.get(PARAM_DATASETS);
	String whole_words = params.get(PARAM_WHOLE_WORDS);
        String textExpression = rewriteExpression(params.get(PARAM_TEXT_EXPRESSION), whole_words);
        String caseIndependent = params.get(PARAM_CASE_INDEPENDENT);
	String maxPvalue = params.get(PARAM_MAX_PVALUE);
	String species_name = params.get(PARAM_SPECIES_NAME);

	if (species_name == null) {
	    species_name = "";
	}

	StringBuffer cmd = new StringBuffer("(");

	// iterate through datasets
	String[] ds = datasets.split(",");
	for (String dataset : ds) {
	    cmd.append(scriptDir + "/filterByValue -n " + maxPvalue + " < " + dataDir
		       + "/" + dataset + " | " + scriptDir + "/filterByValue -s '"
		       + species_name + "' | egrep " + caseIndependent + " '" + textExpression
		       + "';");
	}

       	cmd.append(") | cut -f1 | sort -u");
	//	cmd.append(")");

	logger.info("\ncommand line = \"" + cmd + "\"\n\n");

	// make it a string array to fool exec() into working
	//	String[] cmds = new String[] {cmd.toString(), "|",  "cut", "-f1",  "|", "sort", "-u"};
	String[] cmds = new String[] {"bash", "-c", cmd.toString()};

	// run it
	try {
            String output = invokeCommand(cmds, 10 * 60);
	    System.out.println("output is " + output);
	    logger.info("output is " + output);
	    //            long end = System.currentTimeMillis();
	    //            logger.info("Invocation takes: " + ((end - start) / 1000.0)
	    //                    + " seconds");

            if (exitValue != 0)
                throw new WsfServiceException("The invocation is failed: "
                        + output);

	    List<Match> matches = new ArrayList<Match>();

	    BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(output.getBytes())));
	    String nameIn;
	    while ((nameIn = reader.readLine()) != null) {

		logger.info("match GeneID: " + nameIn);

		Match match = new Match();
		match.geneID = nameIn;
		matches.add(match);
	    }

	    // construct results
            return prepareResult(matches, orderedColumns);
	} catch (Exception ex) {
            throw new WsfServiceException(ex);
        }

    }

    private String rewriteExpression(String expression, String whole_words) {
	String newExpression;

	if (expression.substring(0, 1).equals("^")) {
	    if (whole_words.equals("true")){
                newExpression = "	" + expression.substring(1) + "[^[:alnum:]]";
	    } else {
		newExpression = "	" + expression.substring(1);
	    }
	} else {
	    if (whole_words.equals("true")){
		newExpression = "[^[:alnum:]]" + expression + "[^[:alnum:]]";
	    } else {
		newExpression = "	.*" + expression;
	    }
	}

        logger.info("rewrote \"" + expression + "\" to \"" + newExpression + "\"");

	return newExpression;

    }

    private String[][] prepareResult(List<Match> matches, String[] cols) {
        String[][] result = new String[matches.size()][cols.length];
        // create an column order map
        Map<String, Integer> orders = new HashMap<String, Integer>();
        for (int i = 0; i < cols.length; i++)
            orders.put(cols[i], i);

        for (int i = 0; i < matches.size(); i++) {
            Match match = matches.get(i);

            result[i][orders.get(COLUMN_GENE_ID)] = match.geneID;
        }
        return result;
    }

    private String[][] prepareDummyResult(List<Match> matches, String[] cols) {
        String[][] result = new String[1][cols.length];
        // create an column order map
        Map<String, Integer> orders = new HashMap<String, Integer>();
        for (int i = 0; i < cols.length; i++)
            orders.put(cols[i], i);


        result[0][orders.get(COLUMN_GENE_ID)] = "PF13_0021";

        return result;
    }

    private String extractField(String defline, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(defline);
        if (matcher.find()) {
            // the match is located at group 1
            return matcher.group(1);
        } else return null;
    }
}
