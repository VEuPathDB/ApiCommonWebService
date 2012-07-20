package org.apidb.apicomplexa.wsfplugin.blast;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.gusdb.wsf.plugin.WsfServiceException;

public interface CommandFormatter {

  void setConfig(BlastConfig config);

  String[] formatCommand(Map<String, String> params, File seqFile,
      File outFile, String dbType) throws IOException, WsfServiceException;
}
