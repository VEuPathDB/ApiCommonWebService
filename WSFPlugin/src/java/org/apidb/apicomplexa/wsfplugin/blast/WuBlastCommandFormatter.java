package org.apidb.apicomplexa.wsfplugin.blast;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Vector;

import org.gusdb.wsf.plugin.WsfServiceException;

public class WuBlastCommandFormatter extends AbstractCommandFormatter {

  @Override
  public String[] formatCommand(Map<String, String> params, File seqFile,
      File outFile, String dbType) throws IOException, WsfServiceException {

    Vector<String> cmds = new Vector<String>();

    String dbOrgs = null;
    String dbOrgName = null;
    for (String paramName : params.keySet()) {
      if (paramName.startsWith(AbstractBlastPlugin.PARAM_DATABASE_ORGANISM)) {
        dbOrgName = paramName;
        dbOrgs = params.get(paramName);
        break;
      }
    }
    params.remove(dbOrgName);
    if (dbOrgs == null)
      throw new WsfServiceException("The required organism param is "
          + "missing.");

    // String blastApp = getBlastProgram(qType, dbType);

    String blastApp = params.get(AbstractBlastPlugin.PARAM_ALGORITHM);
    params.remove(AbstractBlastPlugin.PARAM_ALGORITHM);
    // so database name is built correctly for the Translated cases
    if (dbType.contains("Translated")) {
      if (dbType.contains("Transcripts"))
        dbType = "Transcripts";
      else if (dbType.contains("Genomics"))
        dbType = "Genomics";
      else if (dbType.contains("EST"))
        dbType = "EST";
    }

    // now prepare the commandline
    cmds.add(config.getBlastPath() + "/" + blastApp);

    String blastDbs = getBlastDatabase(dbType, dbOrgs);
    // logger.info("\n\nWB prepareParameters(): FULL DATAPATH is: " +
    // blastDbs + "\n");

    cmds.add(blastDbs);
    cmds.add(seqFile.getAbsolutePath());
    cmds.add("O=" + outFile.getAbsolutePath());

    // add extra options into the command
    cmds.add(config.getExtraOptions());

    for (String param : params.keySet()) {
      if (!(param.contains("filter") && params.get(param).equals("no"))) {
        if (param.contains("filter") && params.get(param).equals("yes"))
          params.put(param, "seg");
        cmds.add(param);
        cmds.add(params.get(param));
      }

    }
    // logger.info("\n\nWB prepareParameters(): " + blastDbs + " INFERRED
    // from (" + dbType + ", '" + dbOrgs + "')");
    // logger.info("\n\nWB prepareParameters(): " + blastApp + " inferred
    // from (" + qType + ", " + dbType + ")");
    String[] cmdArray = new String[cmds.size()];
    cmds.toArray(cmdArray);
    return cmdArray;
  }
}
