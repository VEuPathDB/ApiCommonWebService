package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.gusdb.fgputil.db.SqlUtils;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginUserException;
import org.apache.log4j.Logger;

/**
 * @author steve
 */
public class FindChipSnpsByGeneIdsPlugin extends FindChipPolymorphismsPlugin {

  private static final Logger logger = Logger.getLogger(FindChipSnpsByGeneIdsPlugin.class);

  // required parameter definition
  public static final String PARAM_GENES_DATASET = "ds_gene_ids";

  public static final String genomicLocationsFileName = "genomicLocations.txt";

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#getRequiredParameterNames()
   */
  @Override
  public String[] getExtraParamNames() {
    return new String[] { PARAM_GENES_DATASET };
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
    String gene_list_dataset = params.get(PARAM_GENES_DATASET);
    File filtersFile = new File(jobDir, genomicLocationsFileName);
    BufferedWriter bw = null;
    try {
      if (!filtersFile.exists())
        filtersFile.createNewFile();
      FileWriter w = new FileWriter(filtersFile);
      bw = new BufferedWriter(w);
      if (gene_list_dataset.equals("unit test")) {
        String[] testFilters = new String[] { "e99\t1000\t3000", "f100\t500\t700", "h103\t30021\t40000",
            "j201\t20\t50" };
        for (String filter : testFilters) {
          bw.write(filter);
          bw.newLine();
        }
      }
      else {
        DataSource dataSource = wdkModel.getAppDb().getDataSource();
        String newline = System.lineSeparator();
        String sql = "select g.sequence_id, g.start_min, g.end_max" + newline +
            "from apidbtuning.geneattributes g, " + newline + "(" + gene_list_dataset + ") user_genes" +
            newline + "where g.source_id = user_genes.gene_source_id" + newline +
            "order by g.sequence_id, g.start_min, g.end_max";

        ResultSet rs = null;

        try {
          rs = SqlUtils.executeQuery(dataSource, sql, "FindSnpsByGeneIdsPlugin");

          while (rs.next()) {
            String sourceId = rs.getString(1);
            String start = rs.getString(2);
            String end = rs.getString(3);
            bw.write(sourceId + "\t" + start + "\t" + end);
            bw.newLine();
          }

        }
        catch (Exception ex) {
          throw new PluginModelException(ex);
        }
        finally {
          SqlUtils.closeResultSetAndStatement(rs, null);
        }
      }
    }
    catch (IOException e) {
      throw new PluginModelException("Failed writing to file" + filtersFile, e);
    }
    finally {
      try {
        if (bw != null)
          bw.close();
      }
      catch (IOException e) {
        throw new PluginModelException("Failed closing file" + filtersFile, e);
      }
    }
  }

  @Override
  protected String getGenerateScriptName() {
    return "hsssGenerateGenomicLocationsScript";
  }

  @Override
  protected List<String> makeCommandToCreateBashScript(File jobDir, Map<String, String> params,
                                                       File organismDir) throws PluginModelException, PluginUserException {
      List<String> command = super.makeCommandToCreateBashScript(jobDir, params, organismDir);
    command.add(genomicLocationsFileName);
    logger.info(command);
    return command;
  }

}
