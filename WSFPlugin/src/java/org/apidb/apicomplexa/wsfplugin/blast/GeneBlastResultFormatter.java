package org.apidb.apicomplexa.wsfplugin.blast;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.eupathdb.websvccommon.wsfplugin.EuPathServiceException;
import org.eupathdb.websvccommon.wsfplugin.blast.NcbiBlastResultFormatter;

public class GeneBlastResultFormatter extends NcbiBlastResultFormatter {

  private String getGeneSourceId(String defline) {
    return getField(defline, findGene(defline));
  }

  @Override
  protected boolean assignExtraColumns(int index, String[] row, String[] columns, String defline) {
    if (columns[index].equals(GeneBlastPlugin.COLUMN_MATCHED_RESULT)) {
      row[index] = new String("Y");
      return true;
    }
    if (columns[index].equals(GeneBlastPlugin.COLUMN_GENE_SOURCE_ID)) {
      row[index] = getGeneSourceId(defline);
      return true;
    }   
    return false;
  }

  @Override
  protected String getIdUrl(String recordClass, String projectId,
      String sourceId,  String defline) throws EuPathServiceException {
    try {
      if(sourceId.endsWith("-p1")) {
        // until we handle proteins, trim "-p1" suffix to turn protein ID into transcript ID
        // (cannot test until we generate deflines with gene information,
        // until then the blast protein is breaking because it cannot find the gene ID)
        sourceId = sourceId.replace("-p1", "");
      }
      String url = "showRecord.do?name=" + recordClass + "&project_id="
        + URLEncoder.encode(projectId, "UTF-8") + "&source_id="
        + URLEncoder.encode(sourceId, "UTF-8") + "&gene_source_id="
        + URLEncoder.encode(getGeneSourceId(defline), "UTF-8");
      return url;
    } catch (UnsupportedEncodingException ex) {
      throw new EuPathServiceException(ex);
    }
  }

}
