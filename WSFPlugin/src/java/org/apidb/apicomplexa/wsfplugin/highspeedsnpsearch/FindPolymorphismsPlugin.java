package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import javax.xml.parsers.ParserConfigurationException;

import org.eupathdb.common.model.ProjectMapper;
import org.gusdb.fgputil.FormatUtil;
import org.gusdb.wdk.controller.CConstants;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.jspwrap.WdkModelBean;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.WsfPluginException;
import org.xml.sax.SAXException;

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
  public static final String PARAM_READ_FREQ_PERCENT = "ReadFrequencyPercent";

  // required result column definition
  public static final String COLUMN_PROJECT_ID = "ProjectId";
  public static final String COLUMN_SNP_SOURCE_ID = "SourceId";
  public static final String COLUMN_PERCENT_OF_POLYMORPHISMS = "PercentOfPolymorphisms";
  public static final String COLUMN_PERCENT_OF_UNKNOWNS = "PercentOfUnknowns";
  public static final String COLUMN_IS_SYNONYMOUS = "IsSynonymous";

  // property definition
  public static final String PROPERTY_JOBS_DIR = "jobsDir";
  public static final String PROPERTY_DATA_DIR = "dataDir";

  private File jobsDir;
  private File dataDir;
  private String projectId;
  private ProjectMapper projectMapper;

  public FindPolymorphismsPlugin() {
    super(PROPERTY_FILE);
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
  @Override  public void initialize(Map<String, Object> context)     throws WsfPluginException {
    super.initialize(context);

    System.err.println("inside init");

    // jobs dir
    System.err.println(properties);
    String jobsDirName  = getProperty(PROPERTY_JOBS_DIR);
    if (jobsDirName == null)
      throw new WsfPluginException(PROPERTY_JOBS_DIR 
				   + " is missing from the configuration file");
    jobsDir = new File(jobsDirName);
    if (!jobsDir.exists()) throw new WsfPluginException(PROPERTY_JOBS_DIR
							+ " " + jobsDirName + " does not exist");

    // data dir
    String dataDirName  = getProperty(PROPERTY_DATA_DIR);
    System.err.println("datadir: " + dataDirName);
    if (dataDirName == null)
      throw new WsfPluginException(PROPERTY_DATA_DIR
				   + " is missing from the configuration file");
    dataDir = new File(dataDirName);
    if (!dataDir.exists()) 
      throw new WsfPluginException(PROPERTY_DATA_DIR
				   + " " + dataDirName + " does not exist");
    // create project mapper
    WdkModelBean wdkModel = (WdkModelBean) context.get(CConstants.WDK_MODEL_KEY);
    try {
      projectMapper = ProjectMapper.getMapper(wdkModel.getModel());
    } catch (WdkModelException | SAXException | IOException
        | ParserConfigurationException ex) {
      throw new WsfPluginException(ex);
    }
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
			  PARAM_MAX_PERCENT_UNKNOWNS, PARAM_MIN_PERCENT_POLYMORPHISMS, PARAM_READ_FREQ_PERCENT};
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
    File jobDir = new File(jobsDir, "findPolymorphisms." + getTimeStamp());
    jobDir.mkdirs();

    logger.info("  jobDir: " + jobDir.getPath());

    // find organism's strain dir
    Map<String, String> params = request.getParams();
    String organism = params.get(PARAM_ORGANISM);
    String projectId = getProjectId(organism);
    File projectDir = new File(dataDir, projectId);
    if (!projectDir.exists()) throw new WsfPluginException("Strains dir for project '" + projectId
							    + "'does not exist:\n" + projectDir);

    File organismDir = new File(projectDir, organism);
    if (!organismDir.exists()) throw new WsfPluginException("Strains dir for organism '" + organism
							    + "'does not exist:\n" + organismDir);

    String readFreqPercent = params.get(PARAM_READ_FREQ_PERCENT);
    File readFreqDir = new File(organismDir, "readFreq" + readFreqPercent);
    if (!readFreqDir.exists()) throw new WsfPluginException("Strains dir for readFreq '" + readFreqPercent
							    + "'does not exist:\n" + readFreqDir);
    

    // write strain IDs to file
    File strainsFile = new File(jobDir, "strains");
    String strains = params.get(PARAM_STRAIN_LIST);
    int strainsCount = writeStrainsFile(strainsFile, strains);

    // create bash script
    int percentPolymorphisms = Integer.parseInt(params.get(PARAM_MIN_PERCENT_POLYMORPHISMS));
    int polymorphismsThreshold = (int)Math.ceil(strainsCount * percentPolymorphisms / 100);  // round up
    int percentUnknowns = Integer.parseInt(params.get(PARAM_MAX_PERCENT_UNKNOWNS));
    int unknownsThreshold = (int)Math.floor(strainsCount * percentUnknowns / 100);  // round down
    runCommandToCreateBashScript(readFreqDir, jobDir, polymorphismsThreshold, unknownsThreshold, "strains", "findPolymorphisms", "result");

    // invoke the command, and set default 2 min as timeout limit
    long start = System.currentTimeMillis();
    try {
      StringBuffer output = new StringBuffer();

      String[] cmds = {jobDir.getPath() + "/findPolymorphisms"};
      int signal = invokeCommand(cmds, output, 2 * 60);
      long end = System.currentTimeMillis();
      logger.info("Invocation takes: " + ((end - start) / 1000.0)
		  + " seconds");

      if (signal != 0)
	throw new WsfPluginException("The findPolymorphisms job in jobDir " + jobDir + " failed: "
				     + output);

      // prepare the result
      prepareResult(response, jobDir.getPath() + "/result",
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
    
  private void runCommandToCreateBashScript(File strainsDir, File jobDir, int polymorphismsThreshold, int unknownsThreshold, String strainsFileName, String bashScriptFileName, String resultFileName) throws WsfPluginException {
    List<String> command = new ArrayList<String>();

    //  hsssGeneratePolymorphismScript strain_files_dir tmp_dir polymorphism_threshold unknown_threshold strains_list_file output_file
    command.add("hsssGeneratePolymorphismScript");
    command.add(strainsDir.getPath());
    command.add(jobDir.getPath());
    command.add(new Integer(polymorphismsThreshold).toString());
    command.add(new Integer(unknownsThreshold).toString());
    command.add(jobDir.getPath() + "/" + strainsFileName);
    command.add(jobDir.getPath() + "/" + bashScriptFileName);
    command.add(jobDir.getPath() + "/" + resultFileName);

    String[] array = new String[command.size()];
    command.toArray(array);
    try {
      Process process = new ProcessBuilder(command).start();
        process.waitFor();
        if (process.exitValue() != 0) {
            Scanner s = new Scanner(process.getErrorStream()).useDelimiter("\\A");
            String errMsg = (s.hasNext() ? s.next() : "");
            throw new WsfPluginException("Failed running " + FormatUtil.arrayToString(array, " ") + ": " + errMsg);
        }
    } catch (IOException|InterruptedException e) {
      throw new WsfPluginException("Failed running " + FormatUtil.arrayToString(array, " "), e);
    }
    /*
    try {
      Process process = Runtime.getRuntime().exec(array);
      process.wait();
      if (process.exitValue() != 0) {
	BufferedReader stdError = 
	  new BufferedReader(new InputStreamReader(process.getErrorStream()));
	StringBuffer errMsg = new StringBuffer();
	String s;
	while ((s = stdError.readLine()) != null) {
	  errMsg.append(s);
	}
	throw new WsfPluginException("Failed running " + FormatUtil.arrayToString(array) + ": " + errMsg);
      }
    } catch (IOException|InterruptedException e) {
      throw new WsfPluginException("Failed running " + FormatUtil.arrayToString(array), e);
    }
    */
  }

  private void prepareResult(PluginResponse response, String resultFileName, String[] orderedColumns) throws WsfPluginException, IOException {
    // create a map of <column/position>
    Map<String, Integer> columns = new HashMap<String, Integer>(
								orderedColumns.length);
    for (int i = 0; i < orderedColumns.length; i++) {
      columns.put(orderedColumns[i], i);
    }

    // read from the buffered stream
    BufferedReader in = new BufferedReader(new FileReader(resultFileName));

    String line;
    while ((line = in.readLine()) != null) {
      line = line.trim();
      String[] parts = line.split("\t");

      if (parts.length != 4)
	throw new WsfPluginException("Invalid output format in results file");

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

  protected String getProjectId(String organism) throws WsfPluginException {
    try {
      return projectMapper.getProjectByOrganism(organism);
    } catch (SQLException e) {
      throw new WsfPluginException("Failed getting projectId for organism " + organism, e);
    }
  }

  @Override
    protected String[] defineContextKeys() {
    return new String[] { CConstants.WDK_MODEL_KEY };
  }

  /*
  private void cleanup() {
    long todayLong = new Date().getTime();
    // remove files older than a week (500000000)
    for (File tempFile : jobsDir.listFiles()) {
      if (tempFile.isFile() && tempFile.canWrite()
          && (todayLong - (tempFile.lastModified())) > 500000000) {
        logger.info("Temp file to be deleted: " + tempFile.getAbsolutePath()
            + "\n");
        tempFile.delete();
      }
    }
  }
  */

}
