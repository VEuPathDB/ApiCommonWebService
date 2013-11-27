package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.eupathdb.common.model.ProjectMapper;
import org.gusdb.fgputil.FormatUtil;
import org.gusdb.fgputil.runtime.GusHome;
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
 */
public class FindPolymorphismsPlugin extends AbstractPlugin {

  private static final Logger LOG = Logger.getLogger(FindPolymorphismsPlugin.class);

  private static final String PROPERTY_FILE = "highSpeedSnpSearch-config.xml";

  // required parameter definition
  public static final String PARAM_ORGANISM = "organism";
  public static final String PARAM_META = "ontology_type";
  public static final String PARAM_STRAIN_LIST = "htsSnp_strain_meta";
  public static final String PARAM_MIN_PERCENT_KNOWNS = "MinPercentKnowns";
  public static final String PARAM_MIN_PERCENT_POLYMORPHISMS = "MinPercentPolymorphisms";
  public static final String PARAM_READ_FREQ_PERCENT = "ReadFrequencyPercent";

  // required result column definition
  public static final String COLUMN_PROJECT_ID = "ProjectId";
  public static final String COLUMN_SNP_SOURCE_ID = "SourceId";
  public static final String COLUMN_PERCENT_OF_POLYMORPHISMS = "PercentOfPolymorphisms";
  public static final String COLUMN_PERCENT_OF_KNOWNS = "PercentOfKnowns";
  public static final String COLUMN_IS_NONSYNONYMOUS = "IsNonSynonymous";

  // property definition
  public static final String PROPERTY_JOBS_DIR = "jobsDir";
  public static final String PROPERTY_DATA_DIR = "dataDir";

  private File jobsDir;
  private File dataDir;
  private ProjectMapper projectMapper;

  private static final String JOBS_DIR_PREFIX = "hsssFindPolymorphisms.";

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
  @Override
  public void initialize(Map<String, Object> context) throws WsfPluginException {
    super.initialize(context);

    // jobs dir
    LOG.debug(properties);
    String jobsDirName = getProperty(PROPERTY_JOBS_DIR);
    if (jobsDirName == null)
        throw new WsfPluginException(PROPERTY_JOBS_DIR
                + " is missing from the configuration file");
    jobsDir = new File(jobsDirName);
    if (!jobsDir.exists())
        throw new WsfPluginException(PROPERTY_JOBS_DIR
                + " " + jobsDirName + " does not exist");

    // data dir
    String dataDirName = getProperty(PROPERTY_DATA_DIR);
    LOG.debug("datadir: " + dataDirName);
    if (dataDirName == null)
        throw new WsfPluginException(PROPERTY_DATA_DIR
                + " is missing from the configuration file");
    dataDir = new File(dataDirName);
    if (!dataDir.exists()) 
        throw new WsfPluginException(PROPERTY_DATA_DIR
                + " " + dataDirName + " does not exist");

    // BEWARE:  this try THROWS an exception in the unit testing context, which is ignored.
    // don't put any code after it
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
    return new String[] { PARAM_ORGANISM, PARAM_STRAIN_LIST, PARAM_META,
              PARAM_MIN_PERCENT_KNOWNS, PARAM_MIN_PERCENT_POLYMORPHISMS, PARAM_READ_FREQ_PERCENT};
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#getColumns()
   */
  @Override
    public String[] getColumns() {
    return new String[] { COLUMN_SNP_SOURCE_ID, COLUMN_PROJECT_ID,
              COLUMN_PERCENT_OF_POLYMORPHISMS, COLUMN_PERCENT_OF_KNOWNS, COLUMN_IS_NONSYNONYMOUS };
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

    // make job dir
    File jobDir = new File(jobsDir, JOBS_DIR_PREFIX + getTimeStamp());
    jobDir.mkdirs();

    logger.info("Invoking FindPolymorphismsPlugin execute for job " + jobDir.getPath());

    // find organism's strain dir
    Map<String, String> params = request.getParams();
    String organism = params.get(PARAM_ORGANISM);
    if (organism.startsWith("'")) {  // remove single quotes possibly supplied by wdk
      organism = organism.substring(1, organism.length());
      organism = organism.substring(0, organism.length() - 1);
    }
    String projectId = getProjectId(organism);
    File projectDir = new File(dataDir, projectId);
    if (!projectDir.exists()) throw new WsfPluginException("Strains dir for project '" + projectId
                                + "'does not exist:\n" + projectDir);

    String organismNoSpaces = organism.replaceAll(" ","");
    File organismDir = new File(projectDir, organismNoSpaces);
    if (!organismDir.exists()) throw new WsfPluginException("Strains dir for organism '" + organismNoSpaces
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
    int polymorphismsThreshold = (int)Math.ceil(strainsCount * percentPolymorphisms / 100.0);  // round up
    if (polymorphismsThreshold == 0) polymorphismsThreshold++;
    logger.debug("strainsCount: " + strainsCount + "pp: " + percentPolymorphisms + "thresh: " + polymorphismsThreshold);
    int percentUnknowns = 100 - Integer.parseInt(params.get(PARAM_MIN_PERCENT_KNOWNS));
    int unknownsThreshold = (int)Math.floor(strainsCount * percentUnknowns / 100.0);  // round down
    if (unknownsThreshold > (strainsCount - 2)) unknownsThreshold = strainsCount - 2;  // must be at least 2 known
    runCommandToCreateBashScript(readFreqDir, jobDir, polymorphismsThreshold, unknownsThreshold, "strains", "findPolymorphisms", "result");

    // invoke the command, and set default 2 min as timeout limit
    long start = System.currentTimeMillis();
    try {
      StringBuffer output = new StringBuffer();

      String[] cmds = { jobDir.getPath() + "/findPolymorphisms" };
      String[] env = { "PATH=" + GusHome.getGusHome() + "/bin:" + System.getenv("PATH") };
      int signal = invokeCommand(cmds, output, 2 * 60, env);
      long invoke_end = System.currentTimeMillis();
      logger.info("Running findPolymorphisms took: " + ((invoke_end - start) / 1000.0)
          + " seconds");

      if (signal != 0)
          throw new WsfPluginException("The findPolymorphisms job in jobDir " +
                  jobDir + " failed: " + output);

      // prepare the result
      prepareResult(response, projectId, jobDir.getPath() + "/result",
                    request.getOrderedColumns());

      response.setSignal(signal);
    } catch (IOException ex) {
      long end = System.currentTimeMillis();
      logger.info("Invocation takes: " + ((end - start) / 1000.0)
          + " seconds");

      throw new WsfPluginException(ex);
    } finally {
      cleanup();
    }
    logger.info("Done FindPolymorphisms Plugin execute...");
  }

  private int writeStrainsFile(File strainsFile, String strains) throws WsfPluginException {
    String[] strainsArray = strains.split(",");
    BufferedWriter bw = null;
    int count = 0;
    try {
      if (!strainsFile.exists()) strainsFile.createNewFile();
      FileWriter w = new FileWriter(strainsFile);
      bw = new BufferedWriter(w);
      for (String strain : strainsArray) {
	if (strain.equals("-1")) continue;  // workaround: -1 is the internal value passed for non-leaf nodes of a wdk tree param, until wdk stops passing those through.
	String t = strain.trim();
	bw.write(t);
	bw.newLine();
	count++;
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
    return count;
  }
    
  private void runCommandToCreateBashScript(File strainsDir, File jobDir, int polymorphismsThreshold, int unknownsThreshold, String strainsFileName, String bashScriptFileName, String resultFileName) throws WsfPluginException {
    List<String> command = new ArrayList<String>();
    String gusBin = GusHome.getGusHome() + "/bin";

    //  hsssGeneratePolymorphismScript strain_files_dir tmp_dir polymorphism_threshold unknown_threshold strains_list_file 1 output_file result_file
    command.add(gusBin + "/hsssGeneratePolymorphismScript");
    command.add(strainsDir.getPath());
    command.add(jobDir.getPath());
    command.add(new Integer(polymorphismsThreshold).toString());
    command.add(new Integer(unknownsThreshold).toString());
    command.add(jobDir.getPath() + "/" + strainsFileName);
    command.add("1");
    command.add(jobDir.getPath() + "/" + bashScriptFileName);
    command.add(jobDir.getPath() + "/" + resultFileName);

    String[] array = new String[command.size()];
    command.toArray(array);
    try {
      ProcessBuilder builder = new ProcessBuilder(command);
      
      // set path on command
      Map<String,String> env = builder.environment();
      env.put("PATH", gusBin + ":" + env.get("PATH"));
      logger.debug("Path sent to subprocesses: " + env.get("PATH"));
      Process process = builder.start();
      process.waitFor();
      if (process.exitValue() != 0) {
        Scanner s = new Scanner(process.getErrorStream()).useDelimiter("\\A");
        String errMsg = (s.hasNext() ? s.next() : "");
        throw new WsfPluginException("Failed running " + FormatUtil.arrayToString(array, " ") + ": " + errMsg);
      }
    } catch (IOException|InterruptedException e) {
      throw new WsfPluginException("Exception running " + FormatUtil.arrayToString(array, " ") + e, e);
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

  private void prepareResult(PluginResponse response, String projectId, String resultFileName, String[] orderedColumns) throws WsfPluginException, IOException {
    // create a map of <column/position>
    Map<String, Integer> columns = new HashMap<String, Integer>(orderedColumns.length);
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
      row[columns.get(COLUMN_PERCENT_OF_KNOWNS)] = parts[1];
      row[columns.get(COLUMN_PERCENT_OF_POLYMORPHISMS)] = parts[2];
      row[columns.get(COLUMN_IS_NONSYNONYMOUS)] = parts[3];
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

  private void cleanup() {
    long todayLong = new Date().getTime();
    // remove files older than a week (500000000)
    for (File jobDir : jobsDir.listFiles()) {
      if (jobDir.isDirectory() && jobsDir.canWrite() 
          && jobDir.getPath().contains("findPolym")
          && (todayLong - (jobDir.lastModified())) > 500000000) {
        if (jobDir.listFiles() == null) LOG.warn("was null: " + jobDir.getPath());
        for (File tmpFile : jobDir.listFiles()) tmpFile.delete();
        logger.info("Job dir to be deleted: " + jobDir.getAbsolutePath() + "\n");
        jobDir.delete();
      }
    }
  }
}
