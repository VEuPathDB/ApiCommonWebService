package org.apidb.apicomplexa.wsfplugin.listcomparison;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;

/**
 * @author JP
 * 
 */
public class ListComparisonPlugin extends AbstractPlugin {

    private static final Logger logger = Logger.getLogger(ListComparisonPlugin.class);

    private static final String PROPERTY_FILE = "fisherExactTest-config.xml";

    // required parameter definition
    public static final String PARAM_DS_GENE_IDS = "ds_gene_ids";
    public static final String PARAM_ORTHOLOGYFLAG = "orthologyFlag";
    public static final String PARAM_DS_CUTOFF_TYPE = "datasetCutoffType";
    public static final String PARAM_DS_CUTOFF = "datasetCutoff";
    public static final String PARAM_THRESHOLD = "threshold";

    // required result column definition
    public static final String COLUMN_DATASET_ID = "dataset_id";
    public static final String COLUMN_ObserOverlap = "obser_ovelap";
    public static final String COLUMN_ExpOverlap = "exp_overlap";
    public static final String COLUMN_FE = "fold_enrichment";
    public static final String COLUMN_PercentUL = "percent_ul";
    public static final String COLUMN_PercentDS = "percent_ds";
    public static final String COLUMN_Pvalue = "p_value";
    
    //    public static final String COLUMN_PROJECT_ID = "ProjectId";

    // field definition
    private static final String FIELD_PERL_EXECUTABLE = "perlExecutable";
    private static final String FIELD_PERL_SCRIPT = "perlScript";
    private static final String FIELD_DB_CONNECTION = "dbConnection";
    private static final String FIELD_DB_LOGIN = "dbLogin";
    private static final String FIELD_DB_PASSWORD = "dbPassword";
    //private static final String FIELD_PROJECT_ID = "projectId";

    private String perlExec;
    private String perlScript;
    private String dbConnection;
    private String dbLogin;
    private String dbPassword;
    //    private String projectId;
    public ListComparisonPlugin() {
      super(PROPERTY_FILE);
    }

    // load properties
    @Override
    public void initialize()
            throws PluginModelException {
        super.initialize();

        // load properties
        perlExec = getProperty(FIELD_PERL_EXECUTABLE);
        perlScript = getProperty(FIELD_PERL_SCRIPT);
        dbConnection = getProperty(FIELD_DB_CONNECTION);
        dbLogin = getProperty(FIELD_DB_LOGIN);
        dbPassword = getProperty(FIELD_DB_PASSWORD);
	//	projectId = getProperty(FIELD_PROJECT_ID);

        if (perlExec == null)
            throw new PluginModelException("The " + FIELD_PERL_EXECUTABLE
                    + " field is missing from the configuration file.");
        if (perlScript == null)
            throw new PluginModelException("The " + FIELD_PERL_SCRIPT
                    + "field is missing from the configuration file");
        if (dbConnection == null)
            throw new PluginModelException("The " + FIELD_DB_CONNECTION
                    + "field is missing from the configuration file");
        if (dbLogin == null)
            throw new PluginModelException("The " + FIELD_DB_LOGIN
                    + "field is missing from the configuration file");
        if (dbPassword == null)
            throw new PluginModelException("The " + FIELD_DB_PASSWORD
                    + "field is missing from the configuration file");
	//	  if (projectId == null)
	//  throw new PluginModelException("The " + FIELD_PROJECT_ID
	//	          + "field is missing from the configuration file");
    }

    @Override
    public String[] getRequiredParameterNames() {
        return new String[] { PARAM_DS_GENE_IDS, PARAM_DS_CUTOFF_TYPE, PARAM_DS_CUTOFF, PARAM_THRESHOLD, PARAM_ORTHOLOGYFLAG };
    }

    @Override
    public String[] getColumns(PluginRequest request) {
        return new String[] { COLUMN_DATASET_ID, COLUMN_ObserOverlap, COLUMN_ExpOverlap, COLUMN_FE, COLUMN_PercentUL, COLUMN_PercentDS, COLUMN_Pvalue};
    }

    @Override
    public void validateParameters(PluginRequest request)
            throws PluginUserException {
        // validate orthologs JP THIS ISNT WORKING AS PARAM IS COMING THROUGH AS 'no' not no  
        //Map<String, String> params = request.getParams();
        /** String orthologs = params.get(PARAM_ORTHOLOGYFLAG);
        if (!orthologs.equalsIgnoreCase("yes")
                && !orthologs.equalsIgnoreCase("no"))
            throw new PluginUserException("Invalid use orthlog selection: "
                    + orthologs + ". Should be either "
                    + "\"yes\" or \"no\"");
        params.put(PARAM_ORTHOLOGYFLAG, orthologs.toLowerCase());
	**/
        // validdate the size of result
	/**
        int numReturn = 0;
        try {
            numReturn = Integer.parseInt(params.get(PARAM_NUM_RETURN));
        } catch (NumberFormatException e) {
            throw new PluginUserException("Invalid size of the result: "
                    + numReturn + ", a number is expected.");
        }
	**/
	// validate search goal
        
	/**String searchGoal = params.get(PARAM_SEARCH_GOAL);
        if (!searchGoal.equalsIgnoreCase("similar")
                && !searchGoal.equalsIgnoreCase("dissimilar"))
            throw new PluginUserException("Invalid search goal: " + searchGoal
                    + ". Should be either \"similar\" or \"dissimilar\"");
        params.put(PARAM_SEARCH_GOAL, searchGoal.toLowerCase());
	**/
       
        // validate scale data - NOT IMPLEMENTED
        /**  String scaleData = params.get(PARAM_SCALE_DATA);
        if (scaleData.equalsIgnoreCase("true") || scaleData.equals("1")) {
            scaleData = "1";
        } else scaleData = "0";
	params.put(PARAM_SCALE_DATA, scaleData);  **/

        // validate fdr_cutoff - CANT GET IT TO WORK 
        /** int fc = 0;
        try {
            fc = Integer.parseInt(params.get(PARAM_FC));
            if (fc < 0)
                throw new PluginUserException(
                        "The FC cutoff should be greater than 0");
        } //catch (NumberFormatException ex) {
	// throw new PluginUserException("Invalid FC value: " + fc
	//          + ", a number is expected.");
	//}
	**/
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#execute(java.util.Map,
     * java.lang.String[])
     */
    @Override
    public int execute(PluginRequest request, PluginResponse response) throws PluginModelException, PluginUserException {
        logger.debug("Invoking ListComparison Plugin...");

        // prepare the command
        Map<String, String> params = request.getParams();
        String[] cmds = prepareCommand(params);

        // measure the time used for invocation
        long start = System.currentTimeMillis();
        try {
            // invoke the command, and set default 10 min as timeout limit
            StringBuffer output = new StringBuffer();

            int signal = invokeCommand(cmds, output, 10 * 60);
            long end = System.currentTimeMillis();
            logger.debug("Invocation takes: " + ((end - start) / 1000.0)
                    + " seconds");

            if (signal != 0)
                throw new PluginModelException("The invocation is failed: "
                        + output);

            // prepare the result
            //String queryDatasetName = params.get(PARAM_DS_GENE_IDS);
            prepareResult(response, output.toString(), request.getOrderedColumns());

            return signal;
        } catch (IOException ex) {
            long end = System.currentTimeMillis();
            logger.debug("Invocation takes: " + ((end - start) / 1000.0)
                    + " seconds");

            throw new PluginModelException(ex);
        }

    }

    private String[] prepareCommand(Map<String, String> params) {
        List<String> cmds = new ArrayList<String>();

        cmds.add(perlExec);
        cmds.add(perlScript);
        cmds.add(params.get(PARAM_DS_GENE_IDS));
        cmds.add(params.get(PARAM_ORTHOLOGYFLAG));

        cmds.add(params.get(PARAM_DS_CUTOFF_TYPE));
        cmds.add(params.get(PARAM_DS_CUTOFF));

        cmds.add(dbConnection);
        cmds.add(dbLogin);
        cmds.add(dbPassword);
        cmds.add(params.get(PARAM_THRESHOLD));

	cmds.add("| sort -k6,6");
       
        //cmds.add(params.get(PARAM_WEIGHTS_STRING));


        String[] array = new String[cmds.size()];
        cmds.toArray(array);
	
        return array;
    }

    private void prepareResult(PluginResponse response, String content, String[] orderedColumns)
            throws IOException, PluginModelException, PluginUserException {
        // create a map of <column/position>
        Map<String, Integer> columns = new HashMap<String, Integer>(
                orderedColumns.length);
        for (int i = 0; i < orderedColumns.length; i++) {
            columns.put(orderedColumns[i], i);
        }

        // check if the output contains error message
        if (content.indexOf("ERROR:") >= 0)
            throw new PluginModelException(content);

        // read from the buffered stream
        BufferedReader in = new BufferedReader(new InputStreamReader(
                new ByteArrayInputStream(content.getBytes())));

        //NumberFormat format = NumberFormat.getNumberInstance();
        //format.setMaximumFractionDigits(4);
        //format.setMinimumFractionDigits(4);

        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.length() == 0) continue;
            String[] parts = line.split("\t");

            if (parts.length != 7)
                throw new PluginModelException("Invalid output format -- split into " + parts.length + " parts. Content:\n"
                        + content + "\n<<END OF CONTENT\n");

            String datasetId = parts[0].trim();
            String num_ObserOverlap = parts[1].trim();
            String num_ExpecOverlap = parts[2].trim();
	    String num_fe = parts[3].trim();
            String num_percentUL = parts[4].trim();
            String num_percentDS = parts[5].trim();
            String p = parts[6].trim();

            // do not skip the query gene, and include it in the result list
            // if (geneId.equalsIgnoreCase(queryGeneId)) continue;

            String[] row = new String[7];
            row[columns.get(COLUMN_DATASET_ID)] = datasetId;
            row[columns.get(COLUMN_ObserOverlap)] = num_ObserOverlap;
            row[columns.get(COLUMN_ExpOverlap)] = num_ExpecOverlap;
	    row[columns.get(COLUMN_FE)] = num_fe;
	    row[columns.get(COLUMN_PercentUL)] = num_percentUL;
	    row[columns.get(COLUMN_PercentDS)] = num_percentDS;
	    row[columns.get(COLUMN_Pvalue)] = p;
            response.addRow(row);
        }
        in.close();
    }
}
