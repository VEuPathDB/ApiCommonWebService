package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.WsfPluginException;

/**
 * @author steve
 */
public class FindGenesWithSnpCharsPlugin extends FindPolymorphismsPlugin {

  // required parameter definition
  public static final String PARAM_SNP_CLASS = "snp_stat";
  public static final String PARAM_OCCURENCES_LOWER = "occurrences_lower";
  public static final String PARAM_OCCURENCES_UPPER = "occurrences_upper";
  public static final String PARAM_DNDS_LOWER = "dn_ds_ratio_lower";
  public static final String PARAM_DNDS_UPPER = "dn_ds_ratio_upper";
  public static final String PARAM_DENSITY_LOWER = "density_lower";
  public static final String PARAM_DENSITY_UPPER = "density_upper";

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#getRequiredParameterNames()
   */
  @Override
  public String[] getExtraParamNames() {
    return new String[] {PARAM_SNP_CLASS, PARAM_OCCURENCES_LOWER, PARAM_OCCURENCES_UPPER, PARAM_DNDS_LOWER, PARAM_DNDS_UPPER, PARAM_DENSITY_LOWER, PARAM_DENSITY_UPPER};
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
  protected void initForBashScript(File jobDir, Map<String, String> params, File organismDir) throws WsfPluginException {
    String gene_list_dataset = params.get(PARAM_GENES_DATASET);
    File filtersFile = new File(jobDir, genomicLocationsFileName);
    BufferedWriter bw = null;
    try {
      if (!filtersFile.exists()) filtersFile.createNewFile();
      FileWriter w = new FileWriter(filtersFile);
      bw = new BufferedWriter(w);
      if (gene_list_dataset.equals("unit test")) {
	String[] testFilters = new String[] {"e99\t1000\t3000\tg1", "f100\t500\t700\tg2", "h103\t30021\t40000\tg3", "j201\t20\t50\tg4"};
	for (String filter : testFilters ) {
	  bw.write(filter);
	  bw.newLine();
	}
      } else {
	WdkModelBean wdkModelBean = (WdkModelBean)context.get(CTX_CONTAINER_APP);
	WdkModel wdkModel = wdkModelBean.getModel();
	DataSource dataSource = wdkModel.getAppDb().getDataSource();
	String newline = System.lineSeparator();

	String organism = removeSingleQuotes(params.get(PARAM_ORGANISM));

	// can interpolate organism into sql w/o fear of injection because it came from a vocabulary param
	String sql = "select g.sequence_id, g.start_min, g.end_max, g.source_id" + newline +
	  "from apidbtuning.geneattributes g, " + newline +
	  "where g.source_id is not null" + newline +
          " and g.organism = '" + organism + "'" + newline +
	  "order by g.sequence_id, g.start_min, g.end_max";
   
	ResultSet rs = null;

	try {
	  rs = SqlUtils.executeQuery(dataSource, sql, "FindGenesWithSnpCharsPlugin");

	  while (rs.next()) {
	    String sourceId = rs.getString(1);
	    String start = rs.getString(2);
	    String end = rs.getString(3);
	    bw.write(sourceId + "\t" + start + "\t" + end);
	    bw.newLine();
	  }
	  logger.info("finished fetching rows");

	} catch (Exception ex) {
	  throw new WsfPluginException(ex);
	} finally {
	  SqlUtils.closeResultSetAndStatement(rs);
	}
      }
    } catch (IOException e) {
      throw new WsfPluginException("Failed writing to file" + filtersFile, e);
    } finally {
      try {
        if (bw != null) bw.close();
      } catch (IOException e) {
        throw new WsfPluginException("Failed closing file" + filtersFile,  e);
      }
    }
  }

  @Override
  protected List<String> makeCommandToCreateBashScript(File jobDir, Map<String, String> params, File organismDir) throws WsfPluginException {
    String snpClass = params.get(PARAM_SNP_CLASS);
    String min  = params.get(PARAM_OCCURENCES_LOWER);
    String max = params.get(PARAM_OCCURENCES_UPPER);
    String dnds_min = params.get(PARAM_DNDS_LOWER);
    String dnds_max = params.get(PARAM_DNDS_UPPER);
    String density_min = params.get(PARAM_DENSITY_LOWER);
    String density_max = params.get(PARAM_DENSITY_UPPER);

    List<String> command = super.makeCommandToCreateBashScript(jobDir, params, organismDir);
    command.add(snpClass);
    command.add(min);
    command.add(max);
    command.add(dnds_min);
    command.add(dnds_max);
    command.add(density_min);
    command.add(density_max);
    return command;
  }
 
}
