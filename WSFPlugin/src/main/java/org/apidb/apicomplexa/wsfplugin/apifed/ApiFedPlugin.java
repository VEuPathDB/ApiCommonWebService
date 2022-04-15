package org.apidb.apicomplexa.wsfplugin.apifed;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.eupathdb.common.model.ProjectMapper;
import org.gusdb.fgputil.runtime.InstanceManager;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;

/**
 * @author Jerric Gao
 */
public class ApiFedPlugin extends AbstractPlugin {

  private static final Logger LOG = Logger.getLogger(ApiFedPlugin.class);

  public static final String VERSION = "3.0";

  public static final String PARAM_SET_NAME = "VQ";
  // Input Parameters
  public static final String PARAM_PROCESSNAME = "ProcessName";
  public static final String PARAM_PARAMETERS = "Parameters";
  public static final String PARAM_COLUMNS = "Columns";
  public static final String[] PARAM_ORGANISMS = { "organism","organismSinglePick","organism_select_all" };
  public static final String PARAM_QUERY = "Query";

  // Member Variable
  private ProjectMapper projectMapper;

  @Override
  public String[] getRequiredParameterNames() {
    return new String[] {};
  }

  @Override
  public String[] getColumns(PluginRequest request) {
    return new String[] {};
  }

  @Override
  public void validateParameters(PluginRequest request) {
    // Do Nothing in this plugin
  }

  @Override
  public int execute(PluginRequest request, PluginResponse response) throws PluginModelException {
    LOG.info("ApiFedPlugin Version : " + ApiFedPlugin.VERSION);

    String projectId = request.getProjectId();
    try {
      WdkModel wdkModel = InstanceManager.getInstance(WdkModel.class, projectId);
      projectMapper = ProjectMapper.getMapper(wdkModel);
    }
    catch (WdkModelException ex) {
      throw new PluginModelException(ex);
    }

    // Splitting the QueryName up for Mapping
    Map<String, String> context = request.getContext();
    String questionName = context.get(Utilities.QUERY_CTX_QUESTION);
    String paramName = context.get(Utilities.QUERY_CTX_PARAM);
    LOG.debug("question: " + questionName + ", param: " + paramName);

    Map<String, String> params = request.getParams();

    // Determine if the Query is a Parameter Query. if it's a param query, we
    // need to combine the result and remove duplicated values; for other type,
    // we just cache the result as we get them.

    // we will use tokens for param queries, so that the order of the terms are
    // preserved in each component.
    ComponentResult componentResult = (paramName != null) ? new UniqueComponentResult(response)
        : new ComponentResult(response);

    // determine components that we will call
    try {
      Set<String> projects = getProjects(params, (paramName != null));
      List<ComponentQuery> queries = getComponents(request, projects, componentResult);
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
      }
      catch (InterruptedException ex) {}

      // this flag is to make sure we only request timeout stop once.
      boolean timedOut = false;
      while (!isAllStopped(queries)) {
        double elapsed = (System.currentTimeMillis() - start) / 1000D;
        if (timeout > 0 && !timedOut && elapsed >= timeout) {
          // timeout reached, force all queries to stop
          timedOut = true;
          for (ComponentQuery query : queries) {
            if (query.isRunning())
              query.requestStop();
          }
        }
        try {
          Thread.sleep(500);
        }
        catch (InterruptedException ex) {}
      }
    }
    catch (WdkModelException ex) {
      throw new PluginModelException(ex);
    }
    LOG.info("ApiFedPlugin finished. #Rows retieved = " + componentResult.getRowCount());
    return componentResult.getSignal();
  }

  private boolean isAllStopped(List<ComponentQuery> queries) {
    for (ComponentQuery query : queries) {
      if (query.isRunning())
        return false;
    }
    return true;
  }

  private Set<String> getProjects(Map<String, String> params, boolean all) throws WdkModelException {
    Set<String> projects = new LinkedHashSet<>();
    if (all) {
      projects.addAll(projectMapper.getFederatedProjects());
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
    }
    else { // organism doesn't exist, call all projects
      projects.addAll(projectMapper.getFederatedProjects());
    }
    return projects;
  }

  private List<ComponentQuery> getComponents(PluginRequest request, Set<String> projects,
      ComponentResult result) throws PluginModelException {
    List<ComponentQuery> queries = new ArrayList<>();
    for (String projectId : projects) {
      try {
        request.setContextTimeout(Duration.ofSeconds(projectMapper.getTimeout()));
        String url = projectMapper.getWebServiceUrl(projectId);
        ComponentQuery query = new ComponentQuery(projectId, url, request, result);
        queries.add(query);
      }
      catch (WdkModelException e) {
        throw new PluginModelException(e);
      }
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
