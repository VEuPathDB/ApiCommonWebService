package org.apidb.apicomplexa.wsfplugin;

import java.sql.SQLException;

import org.apidb.apicommon.model.ProjectMapper;

public class MockProjectMapper extends ProjectMapper {

  public static final String TEST_SITE = "http://www.testdb.org";
  public static final String TEST_WEBAPP = "/testapp/";
  public static final String TEST_PROJECT = "TestDB";

  public MockProjectMapper() {
    super(null);
  }

  @Override
  protected String getSite(String projectId) {
    return TEST_SITE + TEST_WEBAPP;
  }

  @Override
  public synchronized String getProjectByOrganism(String organism)
      throws SQLException {
    return TEST_PROJECT;
  }
}
