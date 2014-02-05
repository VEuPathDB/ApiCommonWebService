package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gusdb.fgputil.runtime.GusHome;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.WsfPluginException;

/**
 * @author steve
 */
public class FindMajorAllelesPlugin extends HighSpeedSnpSearchAbstractPlugin {

  // required parameter definition
  public static final String PARAM_META = "ontology_type";
  public static final String PARAM_STRAIN_LIST_A = "htsSnp_strain_meta";
  public static final String PARAM_MIN_PERCENT_KNOWNS_A = "MinPercentIsolateCalls";
  public static final String PARAM_MIN_PERCENT_POLYMORPHISMS_A = "MinPercentMinorAlleles";
  public static final String PARAM_READ_FREQ_PERCENT_A = "ReadFrequencyPercent";
  public static final String PARAM_STRAIN_LIST_B = "htsSnp_strain_meta";
  public static final String PARAM_MIN_PERCENT_KNOWNS_B = "MinPercentIsolateCalls";
  public static final String PARAM_MIN_PERCENT_POLYMORPHISMS_B = "MinPercentMinorAlleles";
  public static final String PARAM_READ_FREQ_PERCENT_B = "ReadFrequencyPercent";

  // required result column definition
  public static final String COLUMN_PROJECT_ID = "ProjectId";
  public static final String COLUMN_SNP_SOURCE_ID = "SourceId";
  public static final String COLUMN_PERCENT_OF_POLYMORPHISMS = "PercentMinorAlleles";
  public static final String COLUMN_PERCENT_OF_KNOWNS = "PercentIsolateCalls";
  public static final String COLUMN_IS_NONSYNONYMOUS = "IsNonSynonymous";

  @SuppressWarnings("unused")
  private static final String JOBS_DIR_PREFIX = "hsssFindMajorAlleles.";

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#getRequiredParameterNames()
   */
  @Override
    public String[] getRequiredParameterNames() {
    return new String[] { PARAM_ORGANISM, PARAM_META, PARAM_WEBSVCPATH,
			  PARAM_STRAIN_LIST_A, PARAM_MIN_PERCENT_KNOWNS_A, PARAM_MIN_PERCENT_POLYMORPHISMS_A, PARAM_READ_FREQ_PERCENT_A,
			  PARAM_STRAIN_LIST_B, PARAM_MIN_PERCENT_KNOWNS_B, PARAM_MIN_PERCENT_POLYMORPHISMS_B, PARAM_READ_FREQ_PERCENT_B};
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

  @Override
  protected String getCommandName() { return "findMajorAlleles"; }

  @Override
  protected String getJobsDirPrefix() { return "hsssFindMajorAlleles."; }
    
  @Override
  protected String[] makeResultRow(String [] parts, Map<String, Integer> columns, String projectId) {
    String[] row = new String[5];
    row[columns.get(COLUMN_SNP_SOURCE_ID)] = parts[0];
    row[columns.get(COLUMN_PROJECT_ID)] = projectId;
    row[columns.get(COLUMN_PERCENT_OF_KNOWNS)] = parts[1];
    row[columns.get(COLUMN_PERCENT_OF_POLYMORPHISMS)] = parts[2];
    row[columns.get(COLUMN_IS_NONSYNONYMOUS)] = parts[3];  
    return row;
  }

  @Override
  protected List<String> makeCommandToCreateBashScript(File jobDir, Map<String, String> params, File organismDir) throws WsfPluginException {
    List<String> command = new ArrayList<String>();
    String gusBin = GusHome.getGusHome() + "/bin";

    // set A
    String strainsA = params.get(PARAM_STRAIN_LIST_A);
    if (strainsA == null) throw new WsfPluginException("Strains param is empty");
    int strainsCountA = writeStrainsFile(jobDir, strainsA, "strainsA");
    String readFreqPercentA = params.get(PARAM_READ_FREQ_PERCENT_A);
    File readFreqDirA = new File(organismDir, "readFreq" + readFreqPercentA);
    if (!readFreqDirA.exists()) throw new WsfPluginException("StrainsA dir for readFreq ' " + readFreqPercentA
                                + "' does not exist:\n" + readFreqDirA);
    int percentPolymorphismsA = Integer.parseInt(params.get(PARAM_MIN_PERCENT_POLYMORPHISMS_A));
    int percentUnknownsA = 100 - Integer.parseInt(params.get(PARAM_MIN_PERCENT_KNOWNS_A));
    int unknownsThresholdA = (int)Math.floor(strainsCountA * percentUnknownsA / 100.0);  // round down
    if (unknownsThresholdA > (strainsCountA - 2)) unknownsThresholdA = strainsCountA - 2;  // must be at least 2 known

    // set B
    String strainsB = params.get(PARAM_STRAIN_LIST_B);
    if (strainsB == null) throw new WsfPluginException("Strains param is empty");
    int strainsCountB = writeStrainsFile(jobDir, strainsB, "strainsA");
    String readFreqPercentB = params.get(PARAM_READ_FREQ_PERCENT_B);
    File readFreqDirB = new File(organismDir, "readFreq" + readFreqPercentB);
    if (!readFreqDirB.exists()) throw new WsfPluginException("StrainsB dir for readFreq ' " + readFreqPercentB
                                + "' does not exist:\n" + readFreqDirB);
    int percentPolymorphismsB = Integer.parseInt(params.get(PARAM_MIN_PERCENT_POLYMORPHISMS_B));
    int percentUnknownsB = 100 - Integer.parseInt(params.get(PARAM_MIN_PERCENT_KNOWNS_B));
    int unknownsThresholdB = (int)Math.floor(strainsCountB * percentUnknownsB / 100.0);  // round down
    if (unknownsThresholdB > (strainsCountB - 2)) unknownsThresholdB = strainsCountB - 2;  // must be at least 2 known

    // hsssGenerateMajorAllelesScript strain_files_dir tmp_dir set_a_polymorphism_threshold set_a_unknown_threshold set_a_strains_list_file set_b_polymorphism_threshold set_b_unknown_threshold set_b_strains_list_file strains_are_names output_script_file [output_data_file]
    command.add(gusBin + "/hsssGenerateMajorAllelesScript");
    command.add(jobDir.getPath());
    command.add(readFreqDirA.getPath());
    command.add(new Integer(percentPolymorphismsA).toString());
    command.add(new Integer(unknownsThresholdA).toString());
    command.add(jobDir.getPath() + "/" + "strainsA");
    command.add(readFreqDirB.getPath());
    command.add(new Integer(percentPolymorphismsB).toString());
    command.add(new Integer(unknownsThresholdB).toString());
    command.add(jobDir.getPath() + "/" + "strainsB");
    command.add("1");
    command.add(jobDir.getPath() + "/" + getCommandName());
    command.add(jobDir.getPath() + "/" + "results");
    return command;
  }

  
}
