package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

import org.gusdb.fgputil.db.SqlUtils;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.jspwrap.WdkModelBean;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.dbms.ConnectionContainer;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.WsfPluginException;


/**
 * @author steve
 */
public class FindSnpsByGeneIdsPlugin extends FindPolymorphismsPlugin {

  // required parameter definition
  public static final String PARAM_GENES_DATASET = "ds_gene_ids";

  public static final String genomicLocationsFileName = "genomicLocations.txt";

  private static final String CTX_CONTAINER_APP = "wdkModel";
  private static final String CONNECTION_APP = WdkModel.CONNECTION_APP;


  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#getRequiredParameterNames()
   */
  @Override
  public String[] getExtraParamNames() {
    return new String[] {PARAM_GENES_DATASET};
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
	String[] testFilters = new String[] {"e99\t1000\t3000", "f100\t500\t700", "h103\t30021\t40000", "j201\t20\t50"};
	for (String filter : testFilters ) {
	  bw.write(filter);
	  bw.newLine();
	}
      } else {
	WdkModelBean wdkModelBean = (WdkModelBean)context.get(CTX_CONTAINER_APP);
	WdkModel wdkModel = wdkModelBean.getModel();
	DataSource dataSource = wdkModel.getAppDb().getDataSource();
	String newline = System.lineSeparator();
	String sql = "select g.sequence_id, g.start_min, g.end_max" + newline +
	  "from apidbtuning.geneattributes g, " + newline +
	  "(" + gene_list_dataset + ") user_genes" + newline +
	  "where g.source_id = user_genes.source_id" + newline +
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

  PreparedStatement getPreparedStmt(String geneIdsSubquery, Connection connection) throws WsfPluginException {
    PreparedStatement ps = null;
    String newline = System.lineSeparator();
    String sql = "select g.sequence_id, g.start_min, g.end_max" + newline +
      "from apidbtuning.geneattributes g, " + newline +
      "(" + geneIdsSubquery + ") user_genes" + newline +
      "where g.source_id = user_genes.source_id" + newline +
      "order by g.sequence_id, g.start_min, g.end_max";

    try {
      ps = connection.prepareStatement(sql);
    } catch (SQLException e) {
      throw new WsfPluginException(e);
    }
    return ps;
  }

  protected Connection getDbConnection(String containerKey, String connectionKey)
    throws SQLException, WsfPluginException, WdkModelException {
    ConnectionContainer container = (ConnectionContainer) context.get(containerKey);
    if (container == null)
      throw new WsfPluginException("The container cannot be found in the "
          + "context with key: " + containerKey + ". Please check if the "
          + "container is declared in the context.");

    return container.getConnection(connectionKey);
  }


  @Override
  protected String getGenerateScriptName() {
    return "hsssGenerateGenomicLocationsScript";
  }
    
  @Override
  protected List<String> makeCommandToCreateBashScript(File jobDir, Map<String, String> params, File organismDir) throws WsfPluginException {
    List<String> command = super.makeCommandToCreateBashScript(jobDir, params, organismDir);
    command.add(genomicLocationsFileName);
    return command;
  }
 
}
