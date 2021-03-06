package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.gusdb.fgputil.ArrayUtil;
import org.gusdb.fgputil.runtime.GusHome;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginUserException;
import org.apache.log4j.Logger;

/**
 * @author steve
 */
public abstract class FindPolymorphismsAbstractPlugin extends HighSpeedSnpSearchAbstractPlugin {

  @SuppressWarnings("unused")
  private static final Logger logger = Logger.getLogger(FindPolymorphismsPlugin.class);

  protected abstract String getStrainFilterParamName() ;
  
  public static final String PARAM_MIN_PERCENT_KNOWNS = "MinPercentIsolateCalls";
  public static final String PARAM_MIN_PERCENT_POLYMORPHISMS = "MinPercentMinorAlleles";
  public static final String PARAM_READ_FREQ_PERCENT = "ReadFrequencyPercent";

  // required result column definition
  private static final String COLUMN_PERCENT_OF_POLYMORPHISMS = "PercentMinorAlleles";
  private static final String COLUMN_PERCENT_OF_KNOWNS = "PercentIsolateCalls";
  private static final String COLUMN_PHENOTYPE = "Phenotype";

  public FindPolymorphismsAbstractPlugin(String propertyFile) {
    super(propertyFile);
  }

  @Override
  public String[] getRequiredParameterNames() {
    String[] baseParameters = { PARAM_ORGANISM, getStrainFilterParamName(), PARAM_MIN_PERCENT_KNOWNS,
        PARAM_MIN_PERCENT_POLYMORPHISMS, PARAM_READ_FREQ_PERCENT, PARAM_WEBSVCPATH };
    String[] extraParameters = getExtraParamNames();
    return ArrayUtil.concatenate(baseParameters, extraParameters);
  }

  public String[] getExtraParamNames() {
    String[] a = {};
    return a;
  }

  @Override
  public String[] getColumns(PluginRequest request) {
    return new String[] { COLUMN_SNP_SOURCE_ID, COLUMN_PROJECT_ID, COLUMN_PERCENT_OF_POLYMORPHISMS,
        COLUMN_PERCENT_OF_KNOWNS, COLUMN_PHENOTYPE };
  }

  @Override
  public void validateParameters(PluginRequest request) {}

  @Override
  protected String getCommandName() {
    return "findPolymorphisms";
  }

  @Override
   protected String getReconstructCmdName() {
     return "hsssReconstructSnpId";
  }

  @Override
  protected String getJobsDirPrefix() {
    return "hsssFindPolymorphisms.";
  }

  @Override
  protected String getResultsFileBaseName() {
    return "results";
  }

  /**
   * 
   * @param jobDir
   * @param params
   * @param organismDir
   * @throws PluginModelException
   */
  protected void initForBashScript(File jobDir, Map<String, String> params, File organismDir)
      throws PluginModelException {}

  @Override
  protected List<String> makeCommandToCreateBashScript(File jobDir, Map<String, String> params,
                                                       File organismDir) throws PluginUserException, PluginModelException {

    initForBashScript(jobDir, params, organismDir);

    List<String> command = new ArrayList<String>();
    String gusBin = GusHome.getGusHome() + "/bin";

    String strainsSql = params.get(getStrainFilterParamName());
 
    if (strainsSql == null)
      throw new PluginUserException("Strains param is empty");
    String strainsListString = getParamValueFromSql(strainsSql, "FindPolymorphismsPlugin", wdkModel.getAppDb().getDataSource()).stream().collect(Collectors.joining(", "));
    int strainsCount = writeStrainsFile(jobDir, strainsListString, "strains");
    String readFreqPercent = params.get(PARAM_READ_FREQ_PERCENT);
    File readFreqDir = new File(organismDir, "readFreq" + readFreqPercent);
    if (!readFreqDir.exists())
      throw new PluginModelException("Strains dir for readFreq ' " + readFreqPercent + "' does not exist:\n" +
          readFreqDir);
    int percentPolymorphisms = Integer.parseInt(params.get(PARAM_MIN_PERCENT_POLYMORPHISMS));
    int percentUnknowns = 100 - Integer.parseInt(params.get(PARAM_MIN_PERCENT_KNOWNS));
    int unknownsThreshold = (int) Math.floor(strainsCount * percentUnknowns / 100.0); // round down
    if (unknownsThreshold > (strainsCount - 1))
      unknownsThreshold = strainsCount - 1; // must be at least 1 known
    String reconstructNameCmd = getReconstructCmdName();
    String prefix = super.getIdPrefix();
    // hsssGeneratePolymorphismScript strain_files_dir tmp_dir polymorphism_threshold unknown_threshold
    // strains_list_file 1 output_file result_file
    command.add(gusBin + "/" + getGenerateScriptName());
    command.add(readFreqDir.getPath());
    command.add(jobDir.getPath());
    command.add("1");
    command.add(jobDir.getPath() + "/" + getCommandName());
    command.add(jobDir.getPath() + "/" + getResultsFileBaseName());
    command.add(Integer.valueOf(percentPolymorphisms).toString());
    command.add(Integer.valueOf(unknownsThreshold).toString());
    command.add(jobDir.getPath() + "/strains");
    command.add(reconstructNameCmd);
    command.add(prefix);
    return command;
  }

  protected String getGenerateScriptName() {
    return "hsssGeneratePolymorphismScript";
  }

  @Override
  protected String[] makeResultRow(String[] parts, Map<String, Integer> columns, String projectId)
      throws PluginUserException, PluginModelException {
    if (parts.length != 4)
      throw new PluginUserException("Wrong number of columns in results file.  Expected 4, found " +
          parts.length);

    String[] row = new String[5];
    row[columns.get(COLUMN_SNP_SOURCE_ID)] = parts[0];
    row[columns.get(COLUMN_PROJECT_ID)] = projectId;
    row[columns.get(COLUMN_PERCENT_OF_KNOWNS)] = parts[1];
    row[columns.get(COLUMN_PERCENT_OF_POLYMORPHISMS)] = parts[2];
    row[columns.get(COLUMN_PHENOTYPE)] = parts[3];
    return row;
  }
}
