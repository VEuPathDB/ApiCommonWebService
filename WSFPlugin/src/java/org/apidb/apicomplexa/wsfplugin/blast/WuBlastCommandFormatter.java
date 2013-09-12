package org.apidb.apicomplexa.wsfplugin.blast;


import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Vector;

import org.gusdb.wsf.plugin.WsfServiceException;

import org.apache.log4j.Logger;

public class WuBlastCommandFormatter extends AbstractCommandFormatter {

  private static final Logger logger = Logger.getLogger(WuBlastCommandFormatter.class);

  @Override
  public String[] formatCommand(Map<String, String> params, File seqFile,
      File outFile, String dbType) throws IOException, WsfServiceException {

    Vector<String> cmds = new Vector<String>();

    String dbOrgs = null;
    String dbOrgName = null;
    for (String paramName : params.keySet()) {
      if (paramName.startsWith(EuPathDBBlastPlugin.PARAM_DATABASE_ORGANISM)) {
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

    String blastApp = params.get(EuPathDBBlastPlugin.PARAM_ALGORITHM);
    params.remove(EuPathDBBlastPlugin.PARAM_ALGORITHM);
    // so database name is built correctly for the Translated cases
    if (dbType.contains("Translated")) {
      if (dbType.contains("Transcripts"))
        dbType = "Transcripts";
      else if (dbType.contains("Genomics"))
        dbType = "Genomics";
      else if (dbType.contains("EST"))
        dbType = "EST";
    }

    // make sure the -v & -b are the same
    if (params.containsKey(EuPathDBBlastPlugin.PARAM_MAX_ALIGNMENTS)) {
      String alignments = params.get(EuPathDBBlastPlugin.PARAM_MAX_ALIGNMENTS);
      logger.debug(EuPathDBBlastPlugin.PARAM_MAX_ALIGNMENTS + " = " + alignments);

      params.put(EuPathDBBlastPlugin.PARAM_MAX_DESCRIPTION, alignments);
    } else if (params.containsKey(EuPathDBBlastPlugin.PARAM_MAX_DESCRIPTION)) {
      String descs = params.get(EuPathDBBlastPlugin.PARAM_MAX_DESCRIPTION);
      logger.debug(EuPathDBBlastPlugin.PARAM_MAX_DESCRIPTION + " = " + descs);

      params.put(EuPathDBBlastPlugin.PARAM_MAX_ALIGNMENTS, descs);
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
    String extraOptions = config.getExtraOptions();
    if (extraOptions != null && extraOptions.trim().length() > 0)
      cmds.add(extraOptions);

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
