package org.apidb.apicomplexa.wsfplugin.blast;

import java.util.regex.Matcher;

import org.apache.log4j.Logger;

public abstract class AbstractCommandFormatter implements CommandFormatter {

  private static final Logger logger = Logger.getLogger(AbstractCommandFormatter.class);

  protected BlastConfig config;

  @Override
  public void setConfig(BlastConfig config) {
    this.config = config;
  }

  protected String getBlastDatabase(String dbType, String dbOrgs) {
    // the dborgs is a multipick value, containing several organisms,
    // separated by a comma
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
      String path = config.getBlastDbName().replaceAll(
          "\\$\\$" + EuPathDBBlastPlugin.PARAM_DATABASE_ORGANISM + "\\$\\$",
          Matcher.quoteReplacement(organism));
      path = path.replaceAll("\\$\\$" + EuPathDBBlastPlugin.PARAM_DATABASE_TYPE
          + "\\$\\$", dbType);
      sb.append(path + " ");
    }
    // sb.append("\"");
    return sb.toString().trim();
  }

}
