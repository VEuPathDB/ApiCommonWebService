package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.gusdb.fgputil.db.SqlUtils;
import org.gusdb.fgputil.runtime.GusHome;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginUserException;

/**
 * @author steve
 */
public class FindGenesWithSnpCharsPlugin extends FindPolymorphismsPlugin {

  // Must stay in step with the enumList of geneParams.snp_class in ApiCommonModel and
  // with the classes hsssGeneCharacteristicsFilter branches on. An entry missing here
  // fails at run time, not at build time - which is how "noncoding" was unreachable
  // despite both the other two layers supporting it.
  private static final Set<String> legalParams = new HashSet<String>(Arrays.asList(new String[] { "coding",
      "noncoding", "nonsynonymous", "synonymous", "nonsense", "all" }));

  private static final String geneLocationsFileName = "geneLocations.txt";

  // required parameter definition
  public static final String PARAM_SNP_CLASS = "snp_class";
  public static final String PARAM_OCCURENCES_LOWER = "occurrences_lower";
  public static final String PARAM_OCCURENCES_UPPER = "occurrences_upper";
  public static final String PARAM_DNDS_LOWER = "dn_ds_ratio_lower";
  public static final String PARAM_DNDS_UPPER = "dn_ds_ratio_upper";
  public static final String PARAM_DENSITY_LOWER = "snp_density_lower";
  public static final String PARAM_DENSITY_UPPER = "snp_density_upper";

  public static final String COLUMN_GENE_SOURCE_ID = "gene_source_id";
  public static final String COLUMN_SOURCE_ID = "source_id";
  @SuppressWarnings("hiding") /* this value is being changed from the parent class's version */
  public static final String COLUMN_PROJECT_ID = "project_id";
  public static final String COLUMN_MATCHED_RESULT = "matched_result";
  public static final String COLUMN_DENSITY = "cds_snp_density";
  public static final String COLUMN_SPAN_DENSITY = "span_snp_density";
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
    return new String[] { PARAM_SNP_CLASS, PARAM_OCCURENCES_LOWER, PARAM_OCCURENCES_UPPER, PARAM_DNDS_LOWER,
        PARAM_DNDS_UPPER, PARAM_DENSITY_LOWER, PARAM_DENSITY_UPPER };
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#validateParameters(java.util.Map)
   */
  @Override
  public void validateParameters(PluginRequest request) {}

  @Override
  protected void initForBashScript(File jobDir, Map<String, String> params, File organismDir) throws PluginModelException {
    File filtersFile = new File(jobDir, geneLocationsFileName);
    BufferedWriter bw = null;
    String snpClass = params.get(PARAM_SNP_CLASS);
    try {
      if (!filtersFile.exists())
        filtersFile.createNewFile();
      FileWriter w = new FileWriter(filtersFile);
      bw = new BufferedWriter(w);
      if (snpClass.equals("unit test")) {
        String[] testFilters = new String[] {
            "e99\t1000\t3000\tg1\t1200\t300\t900",
            "f100\t500\t700\tg2\t600\t150\t450",
            "h103\t30021\t40000\tg3\t\t\t",      // no coding sequence: normalizers empty
            "j201\t20\t50\tg4\t900\t0\t675" };   // zero synonymous sites: ratio undefined
        for (String filter : testFilters) {
          bw.write(filter);
          bw.newLine();
        }
      }
      else {
        DataSource dataSource = wdkModel.getAppDb().getDataSource();
        String newline = System.lineSeparator();

        String organism = removeSingleQuotes(params.get(PARAM_ORGANISM));

        // can interpolate organism into sql w/o fear of injection because it came from a vocabulary param
        // LEFT JOIN, never inner: GeneVariationSummary holds one row per gene that has
        // COHORT variants (5,579 of 5,720 annotated pfal3D7 genes), and a gene missing
        // from it must still get a locations line and still report its counts. Only its
        // normalized statistics come back empty.
        //
        // These three columns are gene properties derived from the genetic code, not
        // from any sample set, which is why reading them here is sound: the numerators
        // stay sample-set-dependent and HSSS still computes them.
        String sql = "select g.sequence_id, g.start_min, g.end_max, g.source_id," + newline +
            "       gvs.cds_length, gvs.syn_sites, gvs.nonsyn_sites" + newline +
            "from webready.GeneAttributes_p g " + newline +
            "left join apidbtuning.GeneVariationSummary gvs" + newline +
            "       on gvs.gene_source_id = g.source_id" + newline +
            "      and gvs.project_id     = g.project_id" + newline +
            "where g.source_id is not null" + newline +
            " and g.organism = '" + organism + "'";

        ResultSet rs = null;

        try {
          rs = SqlUtils.executeQuery(dataSource, sql, "FindGenesWithSnpCharsPlugin");

          while (rs.next()) {
            String seqId = rs.getString(1);
            String start = rs.getString(2);
            String end = rs.getString(3);
            String geneId = rs.getString(4);
            // getString returns null for a SQL NULL; the filter tests these for truth,
            // so an empty string reads as "no normalizer" exactly like a zero would.
            String cdsLen = rs.getString(5) == null ? "" : rs.getString(5);
            String synSites = rs.getString(6) == null ? "" : rs.getString(6);
            String nonsynSites = rs.getString(7) == null ? "" : rs.getString(7);
            bw.write(seqId + "\t" + start + "\t" + end + "\t" + geneId + "\t"
                + cdsLen + "\t" + synSites + "\t" + nonsynSites);
            bw.newLine();
          }

        }
        catch (SQLException ex) {
          throw new PluginModelException(ex);
        }
        finally {
          SqlUtils.closeResultSetAndStatement(rs);
        }
      }
    }
    catch (IOException e) {
      throw new PluginModelException("Failed writing to file" + filtersFile, e);
    }
    finally {
      try {
        if (bw != null) {
          bw.close();
	  // run Unix sort on newly-created file, so it's ordered like the SNP files
	  String gusBin = GusHome.getGusHome() + "/bin";
          ProcessBuilder builder
	      = new ProcessBuilder(gusBin + "/apiSortNoLocale", "-k", "1,1", "-k", "2,2n", "-o",
				   jobDir.getPath() + "/" + geneLocationsFileName,
				   jobDir.getPath() + "/" + geneLocationsFileName);
	  builder.start().waitFor();
	}
      }
      catch (IOException e) {
        throw new PluginModelException("Failed closing file" + filtersFile, e);
      }
      catch (InterruptedException e) {
        throw new PluginModelException("Failed sorting file" + filtersFile, e);
      }
    }
  }

  @Override
  public String[] getColumns(PluginRequest request) {
    return new String[] { COLUMN_GENE_SOURCE_ID, COLUMN_PROJECT_ID, COLUMN_DENSITY, COLUMN_SPAN_DENSITY,
        COLUMN_DNDS, COLUMN_SYN, COLUMN_NONSYN, COLUMN_NONCODING, COLUMN_NONSENSE, COLUMN_TOTAL };
  }

  @Override
  protected List<String> makeCommandToCreateBashScript(File jobDir, Map<String, String> params,
                                                       File organismDir) throws PluginUserException, PluginModelException {
    String snpClass = params.get(PARAM_SNP_CLASS);
    if (snpClass.equals("unit test"))
      snpClass = "coding";

    if (!legalParams.contains(snpClass)) {
      throw new PluginUserException("SNP class param has unrecognized value: " + snpClass);
    }
    String min = params.get(PARAM_OCCURENCES_LOWER);
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
  protected String[] makeResultRow(String[] parts, Map<String, Integer> columns, String projectId)
      throws PluginModelException {
    if (parts.length != 9)
      throw new PluginModelException("Wrong number of columns in results file.  Expected 9, found " +
          parts.length);

    String[] row = new String[12];
    row[columns.get(COLUMN_GENE_SOURCE_ID)] = parts[0];
    row[columns.get(COLUMN_SOURCE_ID)] = null;
    row[columns.get(COLUMN_PROJECT_ID)] = projectId;
    row[columns.get(COLUMN_MATCHED_RESULT)] = "Y";
    row[columns.get(COLUMN_DENSITY)] = parts[1];
    row[columns.get(COLUMN_SPAN_DENSITY)] = parts[2];
    row[columns.get(COLUMN_DNDS)] = parts[3];
    row[columns.get(COLUMN_SYN)] = parts[4];
    row[columns.get(COLUMN_NONSYN)] = parts[5];
    row[columns.get(COLUMN_NONCODING)] = parts[6];
    row[columns.get(COLUMN_NONSENSE)] = parts[7];
    row[columns.get(COLUMN_TOTAL)] = parts[8];
    return row;
  }
}
