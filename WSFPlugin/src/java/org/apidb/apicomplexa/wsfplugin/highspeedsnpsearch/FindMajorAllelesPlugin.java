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
  public static final String PARAM_STRAIN_LIST = "htsSnp_strain_meta";
  public static final String PARAM_MIN_PERCENT_KNOWNS = "MinPercentIsolateCalls";
  public static final String PARAM_MIN_PERCENT_POLYMORPHISMS = "MinPercentMinorAlleles";
  public static final String PARAM_READ_FREQ_PERCENT = "ReadFrequencyPercent";

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
    return new String[] { PARAM_ORGANISM, PARAM_STRAIN_LIST, PARAM_META,
			  PARAM_MIN_PERCENT_KNOWNS, PARAM_MIN_PERCENT_POLYMORPHISMS, PARAM_READ_FREQ_PERCENT, PARAM_WEBSVCPATH};
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
    // write strain IDs to file
    String strains = params.get(PARAM_STRAIN_LIST);
    int strainsCount = writeStrainsFile(jobDir, strains, "strains");
    if (strains == null) throw new WsfPluginException("Strains param is empty");
 
    List<String> command = new ArrayList<String>();
    String gusBin = GusHome.getGusHome() + "/bin";

    String readFreqPercent = params.get(PARAM_READ_FREQ_PERCENT);
    File readFreqDir = new File(organismDir, "readFreq" + readFreqPercent);
    if (!readFreqDir.exists()) throw new WsfPluginException("Strains dir for readFreq ' " + readFreqPercent
                                + "' does not exist:\n" + readFreqDir);
    
    int percentPolymorphisms = Integer.parseInt(params.get(PARAM_MIN_PERCENT_POLYMORPHISMS));
    int percentUnknowns = 100 - Integer.parseInt(params.get(PARAM_MIN_PERCENT_KNOWNS));
    int unknownsThreshold = (int)Math.floor(strainsCount * percentUnknowns / 100.0);  // round down
    if (unknownsThreshold > (strainsCount - 2)) unknownsThreshold = strainsCount - 2;  // must be at least 2 known

    //  hsssGeneratePolymorphismScript strain_files_dir tmp_dir polymorphism_threshold unknown_threshold strains_list_file 1 output_file result_file
    command.add(gusBin + "/hsssGenerateMajorAllelesScript");
    command.add(readFreqDir.getPath());
    command.add(jobDir.getPath());
    command.add(new Integer(percentPolymorphisms).toString());
    command.add(new Integer(unknownsThreshold).toString());
    command.add(jobDir.getPath() + "/" + "strains");
    command.add("1");
    command.add(jobDir.getPath() + "/" + getCommandName());
    command.add(jobDir.getPath() + "/" + "results");
    return command;
  }

  
}
