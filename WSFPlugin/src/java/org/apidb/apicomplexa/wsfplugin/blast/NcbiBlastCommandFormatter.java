package org.apidb.apicomplexa.wsfplugin.blast;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.gusdb.wsf.plugin.WsfServiceException;

public class NcbiBlastCommandFormatter extends AbstractCommandFormatter {

  private static final Logger logger = Logger.getLogger(NcbiBlastCommandFormatter.class);


  @Override
  public String[] formatCommand(Map<String, String> params,
      File seqFile, File outFile, String dbType) throws IOException,
      WsfServiceException {
    
    // now prepare the commandline
    List<String> cmds = new ArrayList<String>();
    cmds.add(config.getBlastPath() + "blastall");
    
    String dbOrgs = params.remove(AbstractBlastPlugin.PARAM_DATABASE_ORGANISM);

    // String blastApp = getBlastProgram(qType, dbType);

    String blastApp = params.remove(AbstractBlastPlugin.PARAM_ALGORITHM);

    String blastDbs = getBlastDatabase(dbType, dbOrgs);
    cmds.add("-p");
    cmds.add(blastApp);
    cmds.add("-d");
    cmds.add(blastDbs);
    cmds.add("-i");
    cmds.add(seqFile.getAbsolutePath());
    cmds.add("-o");
    cmds.add(outFile.getAbsolutePath());
    
    // add extra options into the command
    cmds.add(config.getExtraOptions());

    for (String param : params.keySet()) {

        if (param.equals("-filter")) {
            cmds.add("-F");
            if (params.get(param).equals("yes")) cmds.add("T");
            else cmds.add("F");
        }

        if (!param.equals("-p") && !param.equals("-d")
                && !param.equals("-i") && !param.equals("-o")
                && !param.equals("-filter")) {
            cmds.add(param);
            cmds.add(params.get(param));
        }
    }
    logger.debug(blastDbs + " inferred from (" + dbType + ", '" + dbOrgs
            + "')");
    logger.debug(blastApp);// + " inferred from (" + qType + ", " + dbType
    // + ")");

    String[] cmdArray = new String[cmds.size()];
    cmds.toArray(cmdArray);
    return cmdArray;
  }

}
