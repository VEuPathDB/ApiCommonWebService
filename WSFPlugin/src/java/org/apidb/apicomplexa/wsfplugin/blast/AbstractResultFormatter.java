package org.apidb.apicomplexa.wsfplugin.blast;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apidb.apicommon.model.ProjectMapper;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.WdkUserException;

public abstract class AbstractResultFormatter implements ResultFormatter {

  private static final Logger logger = Logger.getLogger(AbstractResultFormatter.class);

  protected ProjectMapper projectMapper;
  private BlastConfig config;

  @Override
  public void setProjectMapper(ProjectMapper projectMapper) {
    this.projectMapper = projectMapper;
  }

  @Override
  public void setConfig(BlastConfig config) {
    this.config = config;
  }

  protected int[] findSourceId(String defline) {
    return findField(defline, config.getSourceIdRegex(),
        config.getSourceIdRegexIndex());
  }

  protected int[] findOrganism(String defline) {
    return findField(defline, config.getOrganismRegex(),
        config.getOrganismRegexIndex());
  }

  private int[] findField(String defline, String regex, int regexIndex) {
    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(defline);
    if (matcher.find()) {
      // the match is located at group of the given index
      return new int[] { matcher.start(regexIndex), matcher.end(regexIndex) };
    } else {
      logger.warn("Couldn't find pattern \"" + regex + "\" in defline \""
          + defline + "\"");
      return null;
    }
  }

  protected String insertIdUrl(String defline, String recordClass)
      throws UnsupportedEncodingException, WdkModelException, WdkUserException,
      SQLException {
    int[] orgPos = findOrganism(defline);
    String organism = defline.substring(orgPos[0], orgPos[1]);
    String projectId = projectMapper.getProjectByOrganism(organism);

    logger.debug("\ninsertIdUrl() organism is: " + organism);

    return insertIdUrl(defline, recordClass, projectId);
  }

  protected String insertIdUrl(String defline, String recordClass,
      String projectId) throws UnsupportedEncodingException, WdkModelException,
      WdkUserException, SQLException {
    // extract organism from the defline
    logger.trace("\ninsertIdUrl() line is: " + defline + "\n");

    int[] srcPos = findSourceId(defline);
    String sourceId = defline.substring(srcPos[0], srcPos[1]);
    logger.debug("\ninsertIdUrl() sourceId is " + sourceId + "\n");
    logger.debug("\ninsertIdUrl() project is: " + projectId + "\n");

    // since we don't know the webapp name, assumed the summary action is on
    // the same level as the record action.
    String url = "showRecord.do?name=" + recordClass + "&project_id="
        + URLEncoder.encode(projectId, "UTF-8") + "&source_id="
        + URLEncoder.encode(sourceId, "UTF-8");

    StringBuffer sb = new StringBuffer();
    // insert a link from source id to record page
    sb.append(defline.substring(0, srcPos[0]));
    sb.append("<a href=\"" + url + "\">" + sourceId + "</a>");
    sb.append(defline.substring(srcPos[1]));
    return sb.toString();

  }

  protected String insertGbrowseLink(String hit_sourceId, String hspStart,
      String hspEnd, String projectId) {
    String gb_url = projectMapper.getBaseUrl(projectId);

    gb_url += "/cgi-bin/gbrowse/" + projectId.toLowerCase() + "/?name="
        + hit_sourceId + ":" + hspStart + "-" + hspEnd;
    String gb_link = "\n<a href=\"" + gb_url + "\"> <B><font color=\"red\">"
        + "Link to Genome Browser</font></B></a>,   ";
    return gb_link;
  }
}
