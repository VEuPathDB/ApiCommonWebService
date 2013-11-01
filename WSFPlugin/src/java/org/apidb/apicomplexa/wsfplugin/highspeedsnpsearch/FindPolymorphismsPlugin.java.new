/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Date;

import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.WsfPluginException;

/**
 * @author steve
 * 
 */
public class FindPolymorphismsPlugin extends AbstractPlugin {

  private static final String PROPERTY_FILE = "highSpeedSnpSearch-config.xml";

  // required parameter definition
  public static final String PARAM_ORGANISM = "Organism";
  public static final String PARAM_STRAIN_LIST = "StrainList";
  public static final String PARAM_MAX_PERCENT_UNKNOWNS = "MaxPercentUnknowns";
  public static final String PARAM_MIN_PERCENT_POLYMORPHISMS = "MinPercentPolymorphisms";

  // required result column definition
  public static final String COLUMN_PROJECT_ID = "ProjectId";
  public static final String COLUMN_SNP_SOURCE_ID = "SnpSourceId";
  public static final String COLUMN_PERCENT_OF_POLYMORPHISMS = "PercentOfPolymorphisms";
  public static final String COLUMN_PERCENT_OF_UNKNOWNS = "PercentOfUnknowns";
  public static final String COLUMN_IS_SYNONYMOUS = "IsSynonymous";

  // property definition
  private static final String PROPERTY_JOBS_DIR = "jobsDir";
  private static final String PROPERTY_DATA_DIR = "dataDir";
  private static final String PROPERTY_PROJECT_ID = "projectId";

  private File jobsDir;
  private File dataDir;
  private String projectId;

  public FindPolymorphismsPlugin() {
    super(PROPERTY_FILE);
  }

  // load properties

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.AbstractPlugin#initialize(java.util.Map)
   */
  @Override  public void initialize(Map<String, Object> context)     throws WsfPluginException {
    super.initialize(context);

    // load properties

    // project id
    projectId = getProperty(PROPERTY_PROJECT_ID);
    if (projectId == null)
      throw new WsfPluginException(PROPERTY_PROJECT_ID
				   + " is missing from the configuration file");

    // jobs dir
    String jobsDirName  = getProperty(PROPERTY_JOBS_DIR);
    if (jobsDirName == null)
      throw new WsfPluginException(PROPERTY_JOBS_DIR 
				   + " is missing from the configuration file");
    jobsDir = new File(jobsDirName);
    if (!jobsDir.exists()) throw new WsfPluginException(PROPERTY_JOBS_DIR
							+ " " + jobsDirName + " does not exist");

    // data dir
    String dataDirName  = getProperty(PROPERTY_DATA_DIR);
    if (dataDirName == null)
      throw new WsfPluginException(PROPERTY_DATA_DIR
				   + " is missing from the configuration file");
    dataDir = new File(dataDirName);
    if (!dataDir.exists()) 
      throw new WsfPluginException(PROPERTY_DATA_DIR
				   + " " + dataDirName + " does not exist");
  }

  synchronized String getTimeStamp() {
    return new Long(new Date().getTime()).toString();
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#getRequiredParameterNames()
   */
  @Override
    public String[] getRequiredParameterNames() {
    return new String[] { PARAM_ORGANISM, PARAM_STRAIN_LIST,
			  PARAM_MAX_PERCENT_UNKNOWNS, PARAM_MIN_PERCENT_POLYMORPHISMS};
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#getColumns()
   */
  @Override
    public String[] getColumns() {
    return new String[] { COLUMN_SNP_SOURCE_ID, COLUMN_PROJECT_ID,
			  COLUMN_PERCENT_OF_POLYMORPHISMS, COLUMN_PERCENT_OF_UNKNOWNS, COLUMN_IS_SYNONYMOUS };
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#validateParameters(java.util.Map)
   */
  @Override
    public void validateParameters(PluginRequest request)
    throws WsfPluginException {
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#execute(java.util.Map,
   * java.lang.String[])
   */
  @Override
    public void execute(PluginRequest request, PluginResponse response) throws WsfPluginException {
    logger.info("Invoking FindPolymorphisms Plugin...");

    // make job dir
    File jobDir = new File(jobsDir, getTimeStamp());
    jobDir.mkdirs();

    // find organism's strain dir
    Map<String, String> params = request.getParams();
    String organism = params.get(PARAM_ORGANISM);
    File organismDir = new File(dataDir, organism);
    if (!organismDir.exists()) throw new WsfPluginException("Strains dir for organism '" + organism
							    + "'does not exist:\n" + organismDir);

    // write strain IDs to file
    File strainsFile = new File(jobDir, "strains");
    String strains = params.get(PARAM_STRAIN_LIST);
    int strainsCount = writeStrainsFile(strainsFile, strains);

    // create bash script
    int percentPolymorphisms = Integer.parseInt(params.get(PARAM_MIN_PERCENT_POLYMORPHISMS));
    int polymorphismsThreshold = (int)Math.ceil(strainsCount * percentPolymorphisms / 100);  // round up
    int percentUnknowns = Integer.parseInt(params.get(PARAM_MAX_PERCENT_UNKNOWNS));
    int unknownsThreshold = (int)Math.floor(strainsCount * percentUnknowns / 100);  // round down
    runCommandToCreateBashScript(organismDir, jobDir, polymorphismsThreshold, unknownsThreshold, "strains", "findPolymorphisms");

    // invoke the command, and set default 2 min as timeout limit
    long start = System.currentTimeMillis();
    try {
      StringBuffer output = new StringBuffer();

      String[] cmds = {"findPolymorphisms"};
      int signal = invokeCommand(cmds, output, 2 * 60);
      long end = System.currentTimeMillis();
      logger.info("Invocation takes: " + ((end - start) / 1000.0)
		  + " seconds");

      if (signal != 0)
	throw new WsfPluginException("The findPolymorphisms job in jobDir " + jobDir + " failed: "
				     + output);

      // prepare the result
      prepareResult(response, output.toString(),
                    request.getOrderedColumns());

      response.setSignal(signal);
    } catch (IOException ex) {
      long end = System.currentTimeMillis();
      logger.info("Invocation takes: " + ((end - start) / 1000.0)
		  + " seconds");

      throw new WsfPluginException(ex);
    }
  }

  private int writeStrainsFile(File strainsFile, String strains) throws WsfPluginException {
    String[] strainsArray = strains.split(",");
    BufferedWriter bw = null;
    try {
      if (!strainsFile.exists()) strainsFile.createNewFile();
      FileWriter w = new FileWriter(strainsFile);
      bw = new BufferedWriter(w);
      for (String strain : strainsArray) {
	bw.write(strain);
	bw.newLine();
      }
    } catch (IOException e) {
      throw new WsfPluginException("Failed writing to strains file", e);
    } finally {
      try {
	if (bw != null) bw.close();
      } catch (IOException e) {
	throw new WsfPluginException("Failed closing strains file", e);
      }
    }
    return strainsArray.length;
  }
    
  private void runCommandToCreateBashScript(File organismDir, File jobDir, int polymorphismsThreshold, int unknownsThreshold, String strainsFileName, String bashScriptFileName) {
    List<String> cmds = new ArrayList<String>();

    //  hsssGeneratePolymorphismScript strain_files_dir tmp_dir polymorphism_threshold unknown_threshold strains_list_file output_file
    cmds.add("hsssGeneratePolymorphismScript");
    cmds.add(organismDir.getPath());
    cmds.add(jobDir.getPath());
    cmds.add(new Integer(polymorphismsThreshold).toString());
    cmds.add(new Integer(unknownsThreshold).toString());
    cmds.add(strainsFileName);
    cmds.add(bashScriptFileName);

    String[] array = new String[cmds.size()];
    cmds.toArray(array);
  }

  private void prepareResult(PluginResponse response, String content, String[] orderedColumns) throws WsfPluginException, IOException {
    // create a map of <column/position>
    Map<String, Integer> columns = new HashMap<String, Integer>(
								orderedColumns.length);
    for (int i = 0; i < orderedColumns.length; i++) {
      columns.put(orderedColumns[i], i);
    }

    // check if the output contains error message
    if (content.indexOf("ERROR:") >= 0)
      throw new WsfPluginException(content);

    // read from the buffered stream
    BufferedReader in = new BufferedReader(new InputStreamReader(
								 new ByteArrayInputStream(content.getBytes())));

    String line;
    while ((line = in.readLine()) != null) {
      line = line.trim();
      String[] parts = line.split("\t");

      if (parts.length != 5)
	throw new WsfPluginException("Invalid output format:\n"
				     + content);

      String[] row = new String[5];
      row[columns.get(COLUMN_SNP_SOURCE_ID)] = parts[0];
      row[columns.get(COLUMN_PROJECT_ID)] = projectId;
      row[columns.get(COLUMN_PERCENT_OF_POLYMORPHISMS)] = parts[1];
      row[columns.get(COLUMN_PERCENT_OF_UNKNOWNS)] = parts[2];
      row[columns.get(COLUMN_IS_SYNONYMOUS)] = parts[3];
      response.addRow(row);
    }
    in.close();
  }

  @Override
    protected String[] defineContextKeys() {
    return null;
  }
}
