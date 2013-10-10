package org.apidb.apicomplexa.wsfplugin.blast;

import org.eupathdb.websvccommon.wsfplugin.blast.AbstractBlastPlugin;
import org.eupathdb.websvccommon.wsfplugin.blast.NcbiBlastResultFormatter;

public class EuPathBlastPlugin extends AbstractBlastPlugin {
  
  public static final String PARAM_DATABASE_ORGANISM = "BlastDatabaseOrganism";
  
  static final String BLAST_DB_NAME = "$$BlastDatabaseOrganism$$$$BlastDatabaseType$$";

  public EuPathBlastPlugin() {
    super(new EuPathBlastCommandFormatter(), new NcbiBlastResultFormatter());
  }

  @Override
  public String[] getRequiredParameterNames() {
    String[] partParams = super.getRequiredParameterNames();
    String[] params = new String[partParams.length + 1];
    System.arraycopy(partParams, 0, params, 0, partParams.length);
    params[params.length - 1] = PARAM_DATABASE_ORGANISM;
    return params;
  }
}
