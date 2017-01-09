package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginUserException;

/**
 * @author steve
 */
public class FindChipPolymorphismsWithSeqFilterPlugin extends FindChipPolymorphismsPlugin {

  // required parameter definition
  public static final String PARAM_CHROMOSOME = "chromosomeOptional";
  public static final String PARAM_SEQUENCE = "sequenceId";
  public static final String PARAM_START_POINT = "start_point";
  public static final String PARAM_END_POINT = "end_point";

  public static final String genomicLocationsFileName = "genomicLocations.txt";

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#getRequiredParameterNames()
   */
  @Override
  public String[] getExtraParamNames() {
    return new String[] {PARAM_CHROMOSOME, PARAM_SEQUENCE, PARAM_START_POINT, PARAM_END_POINT};
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
      protected List<String> makeCommandToCreateBashScript(File jobDir, Map<String, String> params, File organismDir) throws PluginModelException, PluginUserException  {
    String chromosome = params.get(PARAM_CHROMOSOME);
    String seq = params.get(PARAM_SEQUENCE);
    //if (seq.equals("")) seq = chromosome;
    if (seq.contains("No Match")) seq = chromosome;
    String start = params.get(PARAM_START_POINT);
    int end = Integer.parseInt(params.get(PARAM_END_POINT));
    if (end == 0) end = 1000000000;
    List<String> command = super.makeCommandToCreateBashScript(jobDir, params, organismDir);
    writeLocationFile(jobDir, seq.replace("'", ""), start, end);
    command.add(genomicLocationsFileName);
    return command;
  }

    protected void writeLocationFile(File jobDir, String sourceId, String start, int end) throws PluginModelException {
    File filtersFile = new File(jobDir, genomicLocationsFileName);
    BufferedWriter bw = null;
    try {
	if (!filtersFile.exists())
	    filtersFile.createNewFile();
	FileWriter w = new FileWriter(filtersFile);
	bw = new BufferedWriter(w);

	bw.write(sourceId + "\t" + start + "\t" + end);
	bw.newLine();
	bw.close();
    }
    catch (IOException e) {
      throw new PluginModelException("Failed writing to file" + filtersFile, e);
    }
  }

}
