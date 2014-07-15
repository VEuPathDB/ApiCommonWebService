package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.eupathdb.common.model.InstanceManager;
import org.eupathdb.common.model.ProjectMapper;
import org.gusdb.fgputil.FormatUtil;
import org.gusdb.fgputil.db.SqlUtils;
import org.gusdb.fgputil.runtime.GusHome;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wsf.common.PluginRequest;
import org.gusdb.wsf.common.WsfException;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.WsfPluginException;

/**
 * @author steve
 */
public abstract class HighSpeedSnpSearchAbstractPlugin extends AbstractPlugin {

  private static final Logger logger = Logger.getLogger(HighSpeedSnpSearchAbstractPlugin.class);

  private static final String PROPERTY_FILE = "highSpeedSnpSearch-config.xml";

  public static final String PARAM_ORGANISM = "organism";
  public static final String PARAM_WEBSVCPATH = "WebServicesPath";

  public static final String COLUMN_PROJECT_ID = "ProjectId";
  public static final String COLUMN_SNP_SOURCE_ID = "SourceId";

  // property definition
  public static final String PROPERTY_JOBS_DIR = "jobsDir";

  private File jobsDir;
  protected WdkModel wdkModel;
  private ProjectMapper projectMapper;
  private String organismNameForFiles_forTesting = null;

  public HighSpeedSnpSearchAbstractPlugin() {
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
  public void initialize() throws WsfPluginException {
    super.initialize();

    // jobs dir
    logger.debug(properties);
    String jobsDirName = getProperty(PROPERTY_JOBS_DIR);
    if (jobsDirName == null)
        throw new WsfPluginException(PROPERTY_JOBS_DIR
                + " is missing from the configuration file");
    jobsDir = new File(jobsDirName);
    if (!jobsDir.exists())
        throw new WsfPluginException(PROPERTY_JOBS_DIR
                + " " + jobsDirName + " does not exist");
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#execute(java.util.Map,
   * java.lang.String[])
   */
  @Override
    public int execute(PluginRequest request, PluginResponse response) throws WsfException {
    String projectId = request.getProjectId();
    try {
      this.wdkModel = InstanceManager.getInstance(WdkModel.class, projectId);
      this.projectMapper = ProjectMapper.getMapper(wdkModel);
    }
    catch (WdkModelException ex) {
      throw new WsfPluginException(ex);
    }
    
    String jobsDirPrefix = getJobsDirPrefix();

    Map<String, String> params = request.getParams();

    // make job dir
    File jobDir = new File(jobsDir, jobsDirPrefix + getTimeStamp());
    jobDir.mkdirs();

    String commandName = getCommandName();

    logger.info("Invoking " + commandName + " plugin execute() for job " + jobDir.getPath());

    File organismDir = findOrganismDir(params, projectId);

    // create bash script
    List<String> command = makeCommandToCreateBashScript(jobDir, params, organismDir);
    runCommandToCreateBashScript(command, commandName);

    // invoke the command, and set default 2 min as timeout limit
    long start = System.currentTimeMillis();
    int signal = 0;
    try {
      StringBuffer output = new StringBuffer();

      String[] cmds = { jobDir.getPath() + "/" + commandName };
      String[] env = { "PATH=" + GusHome.getGusHome() + "/bin:" + System.getenv("PATH"), "GUS_HOME=" + GusHome.getGusHome() };
      signal = invokeCommand(cmds, output, 2 * 60, env);
      long invoke_end = System.currentTimeMillis();
      logger.info("Running " + commandName + " bash took: " + ((invoke_end - start) / 1000.0)
          + " seconds");

      if (signal != 0)
          throw new WsfPluginException("The " + commandName+ " job in jobDir " +
                  jobDir + " failed: " + output);

      // prepare the result
      prepareResult(response, projectId, jobDir.getPath() + "/" + getResultsFileBaseName(),
                    request.getOrderedColumns());
    } catch (IOException ex) {
      long end = System.currentTimeMillis();
      logger.info("Invocation took: " + ((end - start) / 1000.0)
          + " seconds");

      throw new WsfPluginException(ex);
    } finally {
      cleanup(jobsDirPrefix);
    }
    logger.info("Done " + commandName + " plugin execute...");
    return signal;
  }

  protected abstract String getJobsDirPrefix();

  synchronized String getTimeStamp() {
    return new Long(new Date().getTime()).toString();
  }

  protected abstract String getCommandName();

  protected abstract String getResultsFileBaseName();

  protected String getProjectId(Map<String, String> params) throws WsfPluginException {
    String organism = removeSingleQuotes(params.get(PARAM_ORGANISM));
    
    try {
      return projectMapper.getProjectByOrganism(organism);
    } catch (SQLException e) {
      throw new WsfPluginException("Failed getting projectId for organism " + organism, e);
    }
  }

  String removeSingleQuotes(String inputText) {
    String text = inputText;
    if (text.startsWith("'")) {  // remove single quotes possibly supplied by wdk
      text = text.substring(1, text.length());
      text = text.substring(0, text.length() - 1);
    }
    return text;
  }

  File findOrganismDir(Map<String, String> params, String projectId) throws WsfPluginException {

    // find organism's strain dir
    String organism = removeSingleQuotes(params.get(PARAM_ORGANISM));

    String organismNameForFiles = getOrganismNameForFiles(organism);

    String webSvcPathRaw = params.get(PARAM_WEBSVCPATH);
    String organismDirStr = webSvcPathRaw.replaceAll("PROJECT_GOES_HERE", projectId) + "/" + organismNameForFiles + "/highSpeedSnpSearch";

    File organismDir = new File(organismDirStr);
    if (!organismDir.exists()) throw new WsfPluginException("Organism dir does not exist:\n" + organismDirStr); 
    return organismDir;
  }

  /**
   * Write the strains user provided in a parameter to a strains file
   */
  protected int writeStrainsFile(File jobDir, String strains, String strainsFileName) throws WsfPluginException {

    File strainsFile = new File(jobDir, strainsFileName);

    String[] strainsArray = strains.split(",");
    BufferedWriter bw = null;
    int count = 0;
    try {
      if (!strainsFile.exists()) strainsFile.createNewFile();
      FileWriter w = new FileWriter(strainsFile);
      bw = new BufferedWriter(w);
      for (String strain : strainsArray) {
	String t = strain.trim();
	if (t.equals("-1")) continue;  // workaround: -1 is the internal value passed for non-leaf nodes of a wdk tree param, until wdk stops passing those through.
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
    
  protected abstract List<String> makeCommandToCreateBashScript(File jobDir, Map<String, String> params, File organismDir) throws WsfPluginException;

  protected void runCommandToCreateBashScript( List<String> command, String bashScriptFileName) throws WsfPluginException {
    String gusBin = GusHome.getGusHome() + "/bin";

    String[] array = new String[command.size()];
    command.toArray(array);
    try {
      ProcessBuilder builder = new ProcessBuilder(command);
      System.err.println(command.toString());
      
      // set path on command
      Map<String,String> env = builder.environment();
      env.put("PATH", gusBin + ":" + env.get("PATH"));
      env.put("GUS_HOME", GusHome.getGusHome());
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
  }

  /**
   * unpack result from result file and pack it into rows needed by wsf framework
   * @throws WsfException 
   */
  protected void prepareResult(PluginResponse response, String projectId, String resultFileName, String[] orderedColumns) throws IOException, WsfException {
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

      response.addRow(makeResultRow(parts, columns, projectId));
    }
    in.close();
  }

  protected abstract String[] makeResultRow(String [] parts, Map<String, Integer> columns, String projectId) throws WsfPluginException;

  public void setOrganismNameForFiles(String name) {
    organismNameForFiles_forTesting = name;
  }

  private String getOrganismNameForFiles(String organism) throws WsfPluginException {
    if (organismNameForFiles_forTesting != null) return organismNameForFiles_forTesting;

    String sql = "select distinct o.name_for_filenames from apidb.organism o, apidbtuning.snpstrains s where s.organism = ? and s.taxon_id = o.taxon_id";
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      conn = getDbConnection();
      stmt = conn.prepareStatement(sql);
      stmt.setString(1, organism);
      rs = stmt.executeQuery();
      if (rs.next()) {
        return rs.getString(1);
      }
      throw new WsfPluginException("Unable to find file organism name for param '" + organism + "'.");
    }
    catch (SQLException | WdkModelException e) {
      logger.error("caught SQLException or WdkModelException " + e.getMessage());
      throw new WsfPluginException(e);
    }
    finally {
      SqlUtils.closeQuietly(rs, stmt, conn);
    }
  }

  private Connection getDbConnection()
      throws SQLException, WdkModelException {
    return wdkModel.getConnection(WdkModel.CONNECTION_APP);
  }

  private void cleanup(String jobsDirPrefix) {
    long todayLong = new Date().getTime();
    // remove files older than a week (500000000)
    for (File jobDir : jobsDir.listFiles()) {
      if (jobDir.isDirectory() && jobsDir.canWrite() 
          && jobDir.getPath().contains(jobsDirPrefix)
          && (todayLong - (jobDir.lastModified())) > 500000000) {
        if (jobDir.listFiles() == null) logger.warn("was null: " + jobDir.getPath());
        for (File tmpFile : jobDir.listFiles()) tmpFile.delete();
        logger.info("Job dir to be deleted: " + jobDir.getAbsolutePath() + "\n");
        jobDir.delete();
      }
    }
  }
}
