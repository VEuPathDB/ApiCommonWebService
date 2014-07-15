package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.gusdb.fgputil.db.SqlUtils;
import org.gusdb.wsf.common.PluginRequest;
import org.gusdb.wsf.plugin.WsfPluginException;

/**
 * @author steve
 */
public class FindGenesWithSnpCharsPlugin extends FindPolymorphismsPlugin {

  private static final Set<String> legalParams = 
    new HashSet<String>(Arrays.asList(new String[] {"coding","nonsynonymous","synonymous","nonsense", "all", "coding"} ));

  private static final String geneLocationsFileName = "geneLocations.txt";

  // required parameter definition
  public static final String PARAM_SNP_CLASS = "ngs_snp_class";
  public static final String PARAM_OCCURENCES_LOWER = "occurrences_lower";
  public static final String PARAM_OCCURENCES_UPPER = "occurrences_upper";
  public static final String PARAM_DNDS_LOWER = "dn_ds_ratio_lower";
  public static final String PARAM_DNDS_UPPER = "dn_ds_ratio_upper";
  public static final String PARAM_DENSITY_LOWER = "snp_density_lower";
  public static final String PARAM_DENSITY_UPPER = "snp_density_upper";

  public static final String COLUMN_GENE_SOURCE_ID = "source_id";
  public static final String COLUMN_GENE_PROJECT_ID = "project_id";
  public static final String COLUMN_DENSITY = "cds_snp_density";
  public static final String COLUMN_DNDS = "ngs_dn_ds_ratio";
  public static final String COLUMN_SYN = "ngs_num_synonymous";
  public static final String COLUMN_NONSYN = "ngs_num_non_synonymous";
  public static final String COLUMN_NONCODING = "num_noncoding";
  public static final String COLUMN_NONSENSE = "num_nonsense";
  public static final String COLUMN_TOTAL = "ngs_total_snps";

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
  public void validateParameters(PluginRequest request) {
  }

  @Override
  protected void initForBashScript(File jobDir, Map<String, String> params, File organismDir) throws WsfPluginException {
    File filtersFile = new File(jobDir, geneLocationsFileName);
    BufferedWriter bw = null;
    String snpClass = params.get(PARAM_SNP_CLASS);
    try {
      if (!filtersFile.exists()) filtersFile.createNewFile();
      FileWriter w = new FileWriter(filtersFile);
      bw = new BufferedWriter(w);
      if (snpClass.equals("unit test")) {
	String[] testFilters = new String[] {"e99\t1000\t3000\tg1", "f100\t500\t700\tg2", "h103\t30021\t40000\tg3", "j201\t20\t50\tg4"};
	for (String filter : testFilters ) {
	  bw.write(filter);
	  bw.newLine();
	}
      } else {
	DataSource dataSource = wdkModel.getAppDb().getDataSource();
	String newline = System.lineSeparator();

	String organism = removeSingleQuotes(params.get(PARAM_ORGANISM));

	// can interpolate organism into sql w/o fear of injection because it came from a vocabulary param
	String sql = "select g.sequence_id, g.start_min, g.end_max, g.source_id" + newline +
	  "from apidbtuning.geneattributes g " + newline +
	  "where g.source_id is not null" + newline +
          " and g.organism = '" + organism + "'" + newline +
	  "order by g.sequence_id, g.start_min, g.end_max";
   
	ResultSet rs = null;

	try {
	  rs = SqlUtils.executeQuery(dataSource, sql, "FindGenesWithSnpCharsPlugin");

	  while (rs.next()) {
	    String seqId = rs.getString(1);
	    String start = rs.getString(2);
	    String end = rs.getString(3);
	    String geneId = rs.getString(4);
	    bw.write(seqId + "\t" + start + "\t" + end + "\t" + geneId);
	    bw.newLine();
	  }

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

 /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#getColumns()
   */
  @Override
    public String[] getColumns() {
    return new String[] { COLUMN_GENE_SOURCE_ID, COLUMN_GENE_PROJECT_ID, COLUMN_DENSITY, COLUMN_DNDS, COLUMN_SYN, COLUMN_NONSYN, COLUMN_NONCODING, COLUMN_NONSENSE, COLUMN_TOTAL
               };
  }

  @Override
  protected List<String> makeCommandToCreateBashScript(File jobDir, Map<String, String> params, File organismDir) throws WsfPluginException {
    String snpClass = params.get(PARAM_SNP_CLASS);
    if (snpClass.equals("unit test")) snpClass = "coding";
    
    if (!legalParams.contains(snpClass)) {
        throw new WsfPluginException("SNP class param has unrecognized value: " + snpClass);
    }
    String min  = params.get(PARAM_OCCURENCES_LOWER);
    String max = params.get(PARAM_OCCURENCES_UPPER);
    String dnds_min = params.get(PARAM_DNDS_LOWER);
    String dnds_max = params.get(PARAM_DNDS_UPPER);
    String density_min = params.get(PARAM_DENSITY_LOWER);
    String density_max = params.get(PARAM_DENSITY_UPPER);

    List<String> command = super.makeCommandToCreateBashScript(jobDir, params, organismDir);
    command.add(geneLocationsFileName);
    command.add(snpClass);
    command.add(min);
    command.add(max);
    command.add(dnds_min);
    command.add(dnds_max);
    command.add(density_min);
    command.add(density_max);
    return command;
  }

  @Override
  protected String getGenerateScriptName() {
    return "hsssGenerateGeneCharsScript";
  }
    
  @Override
  protected String[] makeResultRow(String [] parts, Map<String, Integer> columns, String projectId) throws WsfPluginException {
    if (parts.length != 8)
      throw new WsfPluginException("Wrong number of columns in results file.  Expected 8, found " + parts.length);

    String[] row = new String[9];
    row[columns.get(COLUMN_GENE_SOURCE_ID)] = parts[0];
    row[columns.get(COLUMN_GENE_PROJECT_ID)] = projectId;
    row[columns.get(COLUMN_DENSITY)] = parts[1];
    row[columns.get(COLUMN_DNDS)] = parts[2];
    row[columns.get(COLUMN_SYN)] = parts[3];
    row[columns.get(COLUMN_NONSYN)] = parts[4];
    row[columns.get(COLUMN_NONCODING)] = parts[5];
    row[columns.get(COLUMN_NONSENSE)] = parts[6];
    row[columns.get(COLUMN_TOTAL)] = parts[7];
    return row;
  }  
}
