package org.apidb.apicomplexa.wsfplugin.blast;

import org.eupathdb.websvccommon.wsfplugin.blast.AbstractBlastPlugin;
import org.eupathdb.websvccommon.wsfplugin.blast.NcbiBlastResultFormatter;
import org.apidb.apicomplexa.wsfplugin.blast.EuPathBlastPlugin;
//import org.apidb.apicomplexa.wsfplugin.blast.EuPathBlastCommandFormatter;
//import org.apidb.apicomplexa.wsfplugin.blast.GeneBlastResultFormatter;
import org.gusdb.fgputil.ArrayUtil;

public class GeneBlastPlugin extends AbstractBlastPlugin {
  
	public static final String COLUMN_MATCHED_RESULT = "matched_result";
  public static final String COLUMN_GENE_SOURCE_ID = "gene_source_id";

 public static final String PARAM_DATABASE_ORGANISM = "BlastDatabaseOrganism";
  
  static final String BLAST_DB_NAME = "$$BlastDatabaseOrganism$$$$BlastDatabaseType$$";

  public GeneBlastPlugin() {
    super(new EuPathBlastCommandFormatter(), new GeneBlastResultFormatter());
  }

  @Override
  public String[] getRequiredParameterNames() {
    return ArrayUtil.append(super.getRequiredParameterNames(), PARAM_DATABASE_ORGANISM);
  }

  @Override
  public String[] getColumns() {
	 /*  return new String[] { COLUMN_IDENTIFIER, COLUMN_PROJECT_ID, COLUMN_EVALUE_MANT, COLUMN_EVALUE_EXP,
			  COLUMN_SCORE, COLUMN_SUMMARY, COLUMN_ALIGNMENT, COLUMN_GENE_SOURCE_ID, COLUMN_MATCHED_RESULT };
	 */
    return ArrayUtil.append(super.getColumns(),  COLUMN_GENE_SOURCE_ID + "," +  COLUMN_MATCHED_RESULT );
  }
}
