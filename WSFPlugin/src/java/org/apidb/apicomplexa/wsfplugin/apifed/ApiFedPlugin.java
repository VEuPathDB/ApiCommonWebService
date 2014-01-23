package org.apidb.apicomplexa.wsfplugin.apifed;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.eupathdb.common.model.ProjectMapper;
import org.gusdb.wdk.controller.CConstants;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.jspwrap.WdkModelBean;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.WsfPluginException;
import org.xml.sax.SAXException;

/**
 * @author Jerric Gao
 */
public class ApiFedPlugin extends AbstractPlugin {

  public static final String VERSION = "3.0";

  public static final String PARAM_SET_NAME = "VQ";
  // Input Parameters
  public static final String PARAM_PROCESSNAME = "ProcessName";
  public static final String PARAM_PARAMETERS = "Parameters";
  public static final String PARAM_COLUMNS = "Columns";
  public static final String[] PARAM_ORGANISMS = { "organism" };
  public static final String PARAM_QUERY = "Query";

  // Member Variable
  private ProjectMapper projectMapper;

  @Override
  protected String[] defineContextKeys() {
    return new String[] { CConstants.WDK_MODEL_KEY };
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.AbstractPlugin#initialize(java.util.Map)
   */
  @Override
  public void initialize(Map<String, Object> context) throws WsfPluginException {
    super.initialize(context);

    WdkModelBean wdkModel = (WdkModelBean) context.get(CConstants.WDK_MODEL_KEY);
    try {
      projectMapper = ProjectMapper.getMapper(wdkModel.getModel());
    } catch (WdkModelException | SAXException | IOException
        | ParserConfigurationException ex) {
      throw new WsfPluginException(ex);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.WsfPlugin#getRequiredParameters()
   */
  @Override
  public String[] getRequiredParameterNames() {
    return new String[] {};
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.WsfPlugin#getColumns()
   */
  @Override
  public String[] getColumns() {
    return new String[] {};

  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.plugin.WsfPlugin#validateParameters(java.util.Map)
   */
  @Override
  public void validateParameters(PluginRequest request)
      throws WsfPluginException {
    // Do Nothing in this plugin
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
   */
  @Override
  public void execute(PluginRequest request, PluginResponse response)
      throws WsfPluginException {
    logger.info("ApiFedPlugin Version : " + ApiFedPlugin.VERSION);

    // Splitting the QueryName up for Mapping
    Map<String, String> context = request.getContext();
    String questionName = context.get(Utilities.QUERY_CTX_QUESTION);
    String paramName = context.get(Utilities.QUERY_CTX_PARAM);
    logger.debug("question: " + questionName + ", param: " + paramName);

    Map<String, String> params = request.getParams();

    String[] tokens = projectMapper.getAllProjects().toArray(new String[0]);

    // Determine if the Query is a Parameter Query. if it's a param query, we
    // need to combine the result and remove duplicated values; for other type,
    // we just cache the result as we get them.

    // we will use tokens for param queries, so that the order of the terms are
    // preserved in each component.
    ComponentResult componentResult = (paramName != null)
        ? new UniqueComponentResult(response, tokens) : new ComponentResult(
            response);

    // determine components that we will call
    try {
      Set<String> projects = getProjects(params, (paramName != null));
      List<ComponentQuery> queries = getComponents(request, projects,
          componentResult);
      for (ComponentQuery query : queries) {
        query.start();
      }

      // wait for all component queries to finish, or the given time out is
      // reached.
      long timeout = projectMapper.getTimeout();
      long start = System.currentTimeMillis();

      // wait for a bit to make sure all queries are running
      try {
        Thread.sleep(500);
      } catch (InterruptedException ex) {}

      // this flag is to make sure we only request timeout stop once.
      boolean timedOut = false;
      while (!isAllStopped(queries)) {
        double elapsed = (System.currentTimeMillis() - start) / 1000D;
        if (timeout > 0 && !timedOut && elapsed >= timeout) {
          // timeout reached, force all queries to stop
          timedOut = true;
          for (ComponentQuery query : queries) {
            if (query.isRunning()) query.requestStop();
          }
        }
        try {
          Thread.sleep(500);
        } catch (InterruptedException ex) {}
      }
    } catch (SQLException ex) {
      throw new WsfPluginException(ex);
    }
    logger.info("ApiFedPlugin finished.");
  }

  private boolean isAllStopped(List<ComponentQuery> queries) {
    for (ComponentQuery query : queries) {
      if (query.isRunning()) return false;
    }
    return true;
  }

  private Set<String> getProjects(Map<String, String> params, boolean all)
      throws SQLException {
    Set<String> projects = new LinkedHashSet<>();
    if (all) {
      projects.addAll(projectMapper.getAllProjects());
      return projects;
    }

    // check if organism param is present
    String organisms = null;
    for (String paramName : PARAM_ORGANISMS) {
      if (params.containsKey(paramName)) {
        organisms = params.get(paramName);
        break;
      }
    }
    // if organism exists, find the mapped project
    if (organisms != null) {
      organisms = stripLeadingAndTrailingQuotes(organisms);
      for (String organism : organisms.split(",")) {
        String projectId = projectMapper.getProjectByOrganism(organism);
        projects.add(projectId);
      }
    } else { // organism doesn't exist, call all projects
      projects.addAll(projectMapper.getAllProjects());
    }
    return projects;
  }

  private List<ComponentQuery> getComponents(PluginRequest request,
      Set<String> projects, ComponentResult result) {
    List<ComponentQuery> queries = new ArrayList<>();
    for (String projectId : projects) {
      String url = projectMapper.getWebServiceUrl(projectId);
      ComponentQuery query = new ComponentQuery(projectId, url, request, result);
      queries.add(query);
    }
    return queries;
  }

  private String stripLeadingAndTrailingQuotes(String str) {
    if (str.startsWith("'")) {
      str = str.substring(1, str.length());
    }
    if (str.endsWith("'")) {
      str = str.substring(0, str.length() - 1);
    }
    return str;
  }
}
