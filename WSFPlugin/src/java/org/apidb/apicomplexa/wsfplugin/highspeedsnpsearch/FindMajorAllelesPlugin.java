package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.gusdb.fgputil.runtime.GusHome;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginUserException;

/**
 * @author steve
 */
public class FindMajorAllelesPlugin extends HighSpeedSnpSearchAbstractPlugin {

  // required parameter definition
  public static final String PARAM_STRAIN_LIST_A = "ngsSnp_strain_meta_a";
  public static final String PARAM_MIN_PERCENT_KNOWNS_A = "MinPercentIsolateCalls";
  public static final String PARAM_MIN_PERCENT_MAJOR_ALLELES_A = "MinPercentMajorAlleles";
  public static final String PARAM_READ_FREQ_PERCENT_A = "ReadFrequencyPercent";
  public static final String PARAM_STRAIN_LIST_B = "ngsSnp_strain_meta_m";
  public static final String PARAM_MIN_PERCENT_KNOWNS_B = "MinPercentIsolateCallsTwo";
  public static final String PARAM_MIN_PERCENT_MAJOR_ALLELES_B = "MinPercentMajorAllelesTwo";
  public static final String PARAM_READ_FREQ_PERCENT_B = "ReadFrequencyPercentTwo";

  // required result column definition
  public static final String COLUMN_MAJOR_ALLELE_A = "MajorAlleleA";
  public static final String COLUMN_MAJOR_ALLELE_PCT_A = "MajorAllelePctA";
  public static final String COLUMN_TRIALLELIC_A = "IsTriallelicA";
  public static final String COLUMN_MAJOR_PRODUCT_A = "MajorProductA";
  public static final String COLUMN_MAJOR_PRODUCT_VARIABLE_A = "MajorProductIsVariableA";
  public static final String COLUMN_MAJOR_ALLELE_B = "MajorAlleleB";
  public static final String COLUMN_MAJOR_ALLELE_PCT_B = "MajorAllelePctB";
  public static final String COLUMN_TRIALLELIC_B = "IsTriallelicB";
  public static final String COLUMN_MAJOR_PRODUCT_B = "MajorProductB";
  public static final String COLUMN_MAJOR_PRODUCT_VARIABLE_B = "MajorProductIsVariableB";


  @SuppressWarnings("unused")
  private static final String JOBS_DIR_PREFIX = "hsssFindMajorAlleles.";

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#getRequiredParameterNames()
   */
  @Override
    public String[] getRequiredParameterNames() {
    return new String[] { PARAM_ORGANISM,PARAM_WEBSVCPATH,
			  PARAM_STRAIN_LIST_A, PARAM_MIN_PERCENT_KNOWNS_A, PARAM_MIN_PERCENT_MAJOR_ALLELES_A, PARAM_READ_FREQ_PERCENT_A,
			  PARAM_STRAIN_LIST_B, PARAM_MIN_PERCENT_KNOWNS_B, PARAM_MIN_PERCENT_MAJOR_ALLELES_B, PARAM_READ_FREQ_PERCENT_B};
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#getColumns()
   */
  @Override
    public String[] getColumns() {
    return new String[] { COLUMN_SNP_SOURCE_ID, COLUMN_PROJECT_ID,
			  COLUMN_MAJOR_ALLELE_A, COLUMN_MAJOR_ALLELE_PCT_A, COLUMN_TRIALLELIC_A, COLUMN_MAJOR_PRODUCT_A, COLUMN_MAJOR_PRODUCT_VARIABLE_A,
			  COLUMN_MAJOR_ALLELE_B, COLUMN_MAJOR_ALLELE_PCT_B, COLUMN_TRIALLELIC_B, COLUMN_MAJOR_PRODUCT_B, COLUMN_MAJOR_PRODUCT_VARIABLE_B};
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#validateParameters(java.util.Map)
   */
  @Override
    public void validateParameters(PluginRequest request)
     {
  }

  @Override
  protected String getCommandName() { return "findMajorAlleles"; }

  @Override
  protected String getJobsDirPrefix() { return "hsssFindMajorAlleles."; }
    
  @Override
  protected String getResultsFileBaseName() { return "results"; }
    
  @Override
  protected List<String> makeCommandToCreateBashScript(File jobDir, Map<String, String> params, File organismDir) throws PluginUserException, PluginModelException  {
    List<String> command = new ArrayList<String>();
    String gusBin = GusHome.getGusHome() + "/bin";

    // set A
    String strainsA = params.get(PARAM_STRAIN_LIST_A);
    if (strainsA == null) throw new PluginUserException("Strains param is empty");
    int strainsCountA = writeStrainsFile(jobDir, strainsA, "strainsA");
    String readFreqPercentA = params.get(PARAM_READ_FREQ_PERCENT_A);
    File readFreqDirA = new File(organismDir, "readFreq" + readFreqPercentA);
    if (!readFreqDirA.exists()) throw new PluginModelException("StrainsA dir for readFreq ' " + readFreqPercentA
                                + "' does not exist:\n" + readFreqDirA);
    int percentMajorAllelesA = Integer.parseInt(params.get(PARAM_MIN_PERCENT_MAJOR_ALLELES_A));
    int percentUnknownsA = 100 - Integer.parseInt(params.get(PARAM_MIN_PERCENT_KNOWNS_A));
    int unknownsThresholdA = (int)Math.floor(strainsCountA * percentUnknownsA / 100.0);  // round down
    if (unknownsThresholdA > (strainsCountA - 1)) unknownsThresholdA = strainsCountA - 1;  // must be at least 1 known

    // set B
    String strainsB = params.get(PARAM_STRAIN_LIST_B);
    if (strainsB == null) throw new PluginUserException("Strains param is empty");
    int strainsCountB = writeStrainsFile(jobDir, strainsB, "strainsB");
    String readFreqPercentB = params.get(PARAM_READ_FREQ_PERCENT_B);
    File readFreqDirB = new File(organismDir, "readFreq" + readFreqPercentB);
    if (!readFreqDirB.exists()) throw new PluginModelException("StrainsB dir for readFreq ' " + readFreqPercentB
                                + "' does not exist:\n" + readFreqDirB);
    int percentMajorAllelesB = Integer.parseInt(params.get(PARAM_MIN_PERCENT_MAJOR_ALLELES_B));
    int percentUnknownsB = 100 - Integer.parseInt(params.get(PARAM_MIN_PERCENT_KNOWNS_B));
    int unknownsThresholdB = (int)Math.floor(strainsCountB * percentUnknownsB / 100.0);  // round down
    if (unknownsThresholdB > (strainsCountB - 1)) unknownsThresholdB = strainsCountB - 1;  // must be at least 1 known

    // hsssGenerateMajorAllelesScript tmp_dir strain_files_dir_a  set_a_major_alleles_threshold set_a_unknown_threshold set_a_strains_list_file strain_files_dir_a set_b_major_alleles_threshold set_b_unknown_threshold set_b_strains_list_file strains_are_names output_script_file [output_data_file]
    command.add(gusBin + "/hsssGenerateMajorAllelesScript");
    command.add(jobDir.getPath());
    command.add(readFreqDirA.getPath());
    command.add(new Integer(percentMajorAllelesA).toString());
    command.add(new Integer(unknownsThresholdA).toString());
    command.add(jobDir.getPath() + "/" + "strainsA");
    command.add(readFreqDirB.getPath());
    command.add(new Integer(percentMajorAllelesB).toString());
    command.add(new Integer(unknownsThresholdB).toString());
    command.add(jobDir.getPath() + "/" + "strainsB");
    command.add("1");
    command.add(jobDir.getPath() + "/" + getCommandName());
    command.add(jobDir.getPath() + "/" + getResultsFileBaseName());
    return command;
  }

  @Override
  protected String[] makeResultRow(String [] parts, Map<String, Integer> columns, String projectId) throws PluginModelException {
      if (parts.length != 11)
        throw new PluginModelException("Wrong number of columns in results file.  Expected 11, found " + parts.length);

    String[] row = new String[12];
    row[columns.get(COLUMN_SNP_SOURCE_ID)] = parts[0];
    row[columns.get(COLUMN_PROJECT_ID)] = projectId;
    row[columns.get(COLUMN_MAJOR_ALLELE_A)] = parts[1];
    row[columns.get(COLUMN_MAJOR_ALLELE_PCT_A)] = parts[2];
    row[columns.get(COLUMN_TRIALLELIC_A)] = parts[3];  
    row[columns.get(COLUMN_MAJOR_PRODUCT_A)] = parts[4];  
    row[columns.get(COLUMN_MAJOR_PRODUCT_VARIABLE_A)] = parts[5];  
    row[columns.get(COLUMN_MAJOR_ALLELE_B)] = parts[6];
    row[columns.get(COLUMN_MAJOR_ALLELE_PCT_B)] = parts[7];
    row[columns.get(COLUMN_TRIALLELIC_B)] = parts[8];  
    row[columns.get(COLUMN_MAJOR_PRODUCT_B)] = parts[9];  
    row[columns.get(COLUMN_MAJOR_PRODUCT_VARIABLE_B)] = parts[10];  
    return row;
  }
  
}
