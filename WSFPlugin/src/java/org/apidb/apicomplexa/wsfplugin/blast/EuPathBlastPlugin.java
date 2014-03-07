package org.apidb.apicomplexa.wsfplugin.blast;

import org.eupathdb.websvccommon.wsfplugin.blast.AbstractBlastPlugin;
import org.eupathdb.websvccommon.wsfplugin.blast.NcbiBlastResultFormatter;
import org.gusdb.fgputil.ArrayUtil;

public class EuPathBlastPlugin extends AbstractBlastPlugin {
  
  public static final String PARAM_DATABASE_ORGANISM = "BlastDatabaseOrganism";
  
  static final String BLAST_DB_NAME = "$$BlastDatabaseOrganism$$$$BlastDatabaseType$$";

  public EuPathBlastPlugin() {
    super(new EuPathBlastCommandFormatter(), new NcbiBlastResultFormatter());
  }

  @Override
  public String[] getRequiredParameterNames() {
    return ArrayUtil.append(super.getRequiredParameterNames(), PARAM_DATABASE_ORGANISM);
  }
}
