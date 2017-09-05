package org.apidb.apicomplexa.wsfplugin.blast;

import static org.gusdb.fgputil.FormatUtil.urlEncodeUtf8;

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
    String sourceId, String defline) throws EuPathServiceException {
    if(sourceId.endsWith("-p1")) {
      // until we handle proteins, trim "-p1" suffix to turn protein ID into transcript ID
      // (cannot test until we generate deflines with gene information,
      // until then the blast protein is breaking because it cannot find the gene ID)
      sourceId = sourceId.replace("-p1", "");
    }
    return "showRecord.do?name=" + recordClass + "&project_id="
      + urlEncodeUtf8(projectId) + "&source_id="
      + urlEncodeUtf8(sourceId) + "&gene_source_id="
      + urlEncodeUtf8(getGeneSourceId(defline));
  }

}
