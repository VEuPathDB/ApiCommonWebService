package org.apidb.apicomplexa.wsfplugin.eda;

import java.io.InputStream;
import java.util.Map;

public class GeneEdaVizWithComputePlugin extends AbstractEdaGenesPlugin {

  @Override
  protected InputStream getEdaTabularDataStream(String edaBaseUrl, Map<String, String> authHeader)
      throws Exception {
    
  }

  @Override
  protected Boolean filterRow(String[] edaRow) {

  }

  @Override
  protected Object[] convertToTmpTableRow(String[] edaRow) {
    
  }


}
