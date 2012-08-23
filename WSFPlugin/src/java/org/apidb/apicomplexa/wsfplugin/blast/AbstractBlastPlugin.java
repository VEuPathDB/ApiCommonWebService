/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.blast;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.apidb.apicommon.model.ProjectMapper;
import org.gusdb.wdk.controller.CConstants;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.jspwrap.WdkModelBean;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.Plugin;
import org.gusdb.wsf.plugin.WsfRequest;
import org.gusdb.wsf.plugin.WsfResponse;
import org.gusdb.wsf.plugin.WsfServiceException;
import org.gusdb.wsf.util.Formatter;
import org.xml.sax.SAXException;

/**
 * @author xingao
 * 
 */
public abstract class AbstractBlastPlugin extends AbstractPlugin implements
    Plugin {

  // field definitions in the config file
  public static final String FILE_CONFIG = "blast-config.xml";

  // column definitions
  public static final String COLUMN_ID = "Identifier";
  public static final String COLUMN_HEADER = "Header";
  public static final String COLUMN_FOOTER = "Footer";
  public static final String COLUMN_SUMMARY = "TabularRow";
  public static final String COLUMN_ALIGNMENT = "Alignment";
  public static final String COLUMN_PROJECT_ID = "ProjectId";
  public static final String COLUMN_COUNTER = "Counter";

  // required parameter definitions
  public static final String PARAM_ALGORITHM = "BlastAlgorithm";
  public static final String PARAM_DATABASE_TYPE = "BlastDatabaseType";
  public static final String PARAM_DATABASE_ORGANISM = "BlastDatabaseOrganism";
  public static final String PARAM_SEQUENCE = "BlastQuerySequence";
  public static final String PARAM_RECORD_CLASS = "BlastRecordClass";
  
  // optional parameters
  public static final String PARAM_MAX_ALIGNMENTS = "-b";
  public static final String PARAM_MAX_DESCRIPTION = "-v";

  /**
   * remove files older than a week (500000000), in milliseconds.
   */
  private static final long MAX_FILE_LIFE = 500000000;

  /**
   * The maximum size of the blast output file that the plugin can handle.
   */
  public static final long MAX_FILE_SIZE = 30 * 1024 * 1024;

  private static Logger logger = Logger.getLogger(AbstractBlastPlugin.class);

  private final CommandFormatter commandFormatter;
  private final ResultFormatter resultFormatter;

  private BlastConfig config;

  /**
   * @param propertyFile
   * @throws WsfServiceException
   */
  public AbstractBlastPlugin(CommandFormatter commandFormatter,
      ResultFormatter resultFormatter) {
    super(FILE_CONFIG);
    this.commandFormatter = commandFormatter;
    this.resultFormatter = resultFormatter;
  }

  public BlastConfig getConfig() {
    return config;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.AbstractPlugin#initialize(java.util.Map)
   */
  @Override
  public void initialize(Map<String, Object> context)
      throws WsfServiceException {
    super.initialize(context);

    // load & set config file.
    config = new BlastConfig(properties);
    commandFormatter.setConfig(config);
    resultFormatter.setConfig(config);

    // create project mapper
    WdkModelBean wdkModel = (WdkModelBean) context.get(CConstants.WDK_MODEL_KEY);
    try {
      ProjectMapper projectMapper = ProjectMapper.getMapper(wdkModel.getModel());
      resultFormatter.setProjectMapper(projectMapper);
    } catch (WdkModelException | SAXException | IOException
        | ParserConfigurationException ex) {
      throw new WsfServiceException(ex);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#getRequiredParameterNames()
   */
  public String[] getRequiredParameterNames() {
    return new String[] { PARAM_ALGORITHM, PARAM_DATABASE_ORGANISM,
        PARAM_DATABASE_TYPE, PARAM_SEQUENCE, PARAM_RECORD_CLASS };
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#getColumns()
   */
  public String[] getColumns() {
    return new String[] { COLUMN_PROJECT_ID, COLUMN_ID, COLUMN_HEADER,
        COLUMN_FOOTER, COLUMN_SUMMARY, COLUMN_ALIGNMENT, COLUMN_COUNTER };
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#validateParameters(java.util.Map)
   */
  public void validateParameters(WsfRequest request) throws WsfServiceException {
    Map<String, String> params = request.getParams();
    for (String param : params.keySet()) {
      logger.debug("Param - name=" + param + ", value=" + params.get(param));
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#execute(java.util.Map,
   * java.lang.String[])
   */
  public WsfResponse execute(WsfRequest request) throws WsfServiceException {
    // Identifier--ProjectId--TabularRow--Alignment--Header--Footer--Counter
    String[] orderedColumns = request.getOrderedColumns();
    StringBuilder builder = new StringBuilder();
    for (String column : orderedColumns) {
      builder.append(column + " -- ");
    }
    logger.debug("BlastPlugin.java: ordered columns are:" + builder);

    // get plugin name
    String pluginName = getClass().getSimpleName();
    logger.info("Invoking " + pluginName + "...");

    File seqFile = null;
    File outFile = null;
    try {
      // create temporary files for input sequence and output report
      File dir = new File(config.getTempPath());
      seqFile = File.createTempFile(pluginName + "_", ".in", dir);
      outFile = File.createTempFile(pluginName + "_", ".out", dir);

      // get the parameters and remove them from the map
      Map<String, String> params = request.getParams();
      String dbType = params.remove(PARAM_DATABASE_TYPE);
      String seq = params.remove(PARAM_SEQUENCE);
      String recordClass = params.remove(PARAM_RECORD_CLASS);

      // write the sequence into the temporary fasta file,
      // do not reformat the sequence - easy to introduce problem
      PrintWriter out = new PrintWriter(new FileWriter(seqFile));
      if (!seq.startsWith(">"))
        out.println(">MySeq1");
      out.println(seq);
      out.flush();
      out.close();

      // prepare the arguments
      String[] command = commandFormatter.formatCommand(params, seqFile,
          outFile, dbType);
      logger.info("Command prepared: " + Formatter.printArray(command));

      // invoke the command
      StringBuffer output = new StringBuffer();
      int signal = invokeCommand(command, output, config.getTimeout());

      // signal is int, defined in WsfPlugin.java
      // we want to show the stderr to the user

      // ******this does not stop processquery from caching a 0 result and no
      // message is shown to user ******
      // the signal should be read by whoever called execute in the plugin
      // (processquery?) the message would contain the error message....
      // if we do it here, we do not show anything to the user and only results
      // 0 are seen.
      // if (signal != 0) throw new
      // WsfServiceException("The invocation is failed: " + output);
      logger.debug("BLAST output: \n------------------\n" + output.toString()
          + "\n-----------------\n");

      // check the size of the blast output, and throws error if the result is
      // too big.
//      if (outFile.length() > MAX_FILE_SIZE)
//					throw new WsfServiceException("Sorry, your Blast result is too big ("
//																				+ outFile.length()/1048576 + "MB > " 
//																				+ MAX_FILE_SIZE / 1048576 + "MB)." 
//																				+ "Please try a smaller number for the maximum descriptions (V) and maximum alignments (B) parameters.");

      // if the invocation succeeds, prepare the result; otherwise,
      // prepare results for failure scenario
      logger.info("\nPreparing the result");
      StringBuffer message = new StringBuffer();
      String[][] result = resultFormatter.formatResult(orderedColumns, outFile,
          dbType, recordClass, message);
      logger.info("\n*****************Result prepared\n\n\n");

      // logger.info(Formatter.printArray(result));

      // logger.info("\nID: " + result[0][0] + "\nID: " + result[1][0] +
      // "\nID: " + result[2][0] + "\nID: " + result[3][0]);

      // if (message.length() == 0) message.append(output);
      message.append(output);

      WsfResponse wsfResult = new WsfResponse();
      wsfResult.setMessage(message.toString());
      wsfResult.setSignal(signal);
      wsfResult.setResult(result);
      return wsfResult;
    } catch (Exception ex) {
      logger.error(ex);
      throw new WsfServiceException(ex);
    } finally {
      /*
       * if (seqFile != null) seqFile.delete(); if (outFile != null)
       * outFile.delete();
       */
      cleanup();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.AbstractPlugin#defineContextKeys()
   */
  @Override
  protected String[] defineContextKeys() {
    return new String[] { CConstants.WDK_MODEL_KEY };
  }

  protected void cleanup() {
    File dir = new File(config.getTempPath());
    String[] allFiles = dir.list();
    File tempFile = null;
    long todayLong = new Date().getTime();
    // remove old files
    for (int i = 0; i < allFiles.length; i++) {
      tempFile = new File(dir, allFiles[i]);
      if (tempFile.canWrite()
          && (todayLong - (tempFile.lastModified())) > MAX_FILE_LIFE) {
        logger.info("Temp file to be deleted: " + allFiles[i] + "\n");
        tempFile.delete();
      }
    }
    return;
  }
}
