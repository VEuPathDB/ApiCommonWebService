package org.apidb.apicomplexa.wsfplugin.blast;

import org.gusdb.fgputil.ArrayUtil;
import org.gusdb.wsf.plugin.PluginRequest;

public class GeneBlastPlugin extends EuPathBlastPlugin {
  
  public static final String COLUMN_MATCHED_RESULT = "matched_result";
  public static final String COLUMN_GENE_SOURCE_ID = "gene_source_id";

  public GeneBlastPlugin() {
    super(new EuPathBlastCommandFormatter(), new GeneBlastResultFormatter());
  }

  @Override
  public String[] getColumns(PluginRequest request) {
    return ArrayUtil.append(super.getColumns(request),COLUMN_GENE_SOURCE_ID,COLUMN_MATCHED_RESULT);
  }
}
