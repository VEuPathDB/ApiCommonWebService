package org.apidb.apicomplexa.wsfplugin.blast;

import java.io.File;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.log4j.Logger;
import org.eupathdb.websvccommon.wsfplugin.blast.NcbiBlastCommandFormatter;
import org.gusdb.wsf.plugin.PluginModelException;

public class EuPathBlastCommandFormatter extends NcbiBlastCommandFormatter {

  private static final Logger logger = Logger.getLogger(EuPathBlastCommandFormatter.class);

  @Override
  public String getBlastDatabase(Map<String, String> params) throws PluginModelException
       {
    String dbType = params.get(EuPathBlastPlugin.PARAM_DATA_TYPE);
    if (dbType.equals("PopSet")){
	dbType = "Isolates";
    }


    // the dborgs is a multipick value, containing several organisms,
    // separated by a comma
    String dbOrgs = params.get(EuPathBlastPlugin.PARAM_DATABASE_ORGANISM);
    String[] organisms = dbOrgs.split(",");
    StringBuffer sb = new StringBuffer();
    for (String organism : organisms) {
      organism = organism.trim();
      // parent organisms in a treeParam, we only need the leave nodes.
      // But don't trigger on organisms that have -1 in the name (e.g.
      // SylvioX10-1).
      if (organism.equals("-1") || organism.length() <= 3) {
        logger.debug("organism value: (" + organism
            + ") not included, we only care for leave nodes\n");
        continue;
      }
      // construct file path pattern
      String path = EuPathBlastPlugin.BLAST_DB_NAME.replaceAll("\\$\\$"
          + EuPathBlastPlugin.PARAM_DATABASE_ORGANISM + "\\$\\$",
          Matcher.quoteReplacement(organism));
      path = path.replaceAll("\\$\\$" + EuPathBlastPlugin.PARAM_DATA_TYPE
          + "\\$\\$", dbType);

      // check if database file exists
      File blastnFile = new File(path + ".nin"), blastpFile = new File(path + ".pin");
      if (!blastnFile.exists() && !blastpFile.exists())
        throw new PluginModelException("The blast database doesn't exist: "
            + path);

      sb.append(path + " ");
    }
    // sb.append("\"");
    return sb.toString().trim();
    // TEST
    // return
    // "/eupath/data/apiSiteFilesStaging/ToxoDB/19/real/webServices/ToxoDB/release-CURRENT/TgondiiME49/blast/AnnotatedTranscripts";
  }

}
