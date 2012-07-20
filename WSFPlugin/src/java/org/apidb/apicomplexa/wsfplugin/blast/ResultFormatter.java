package org.apidb.apicomplexa.wsfplugin.blast;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.apidb.apicommon.model.ProjectMapper;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.WdkUserException;

public interface ResultFormatter {

  void setConfig(BlastConfig config);

  void setProjectMapper(ProjectMapper projectMapper);

  String[][] formatResult(String[] orderedColumns, File outFile, String dbType,
      String recordClass, StringBuffer message) throws IOException,
      WdkModelException, WdkUserException, SQLException;
}
