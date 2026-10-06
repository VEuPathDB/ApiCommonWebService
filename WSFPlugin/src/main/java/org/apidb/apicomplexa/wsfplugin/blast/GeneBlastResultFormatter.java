package org.apidb.apicomplexa.wsfplugin.blast;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apidb.apicommon.model.TranscriptUtil;
import org.eupathdb.websvccommon.wsfplugin.EuPathServiceException;
import org.eupathdb.websvccommon.wsfplugin.blast.NcbiBlastResultFormatter;
import org.gusdb.fgputil.ArrayUtil;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.record.RecordClass;

public class GeneBlastResultFormatter extends NcbiBlastResultFormatter {

 private static final Logger logger = LogManager.getLogger(GeneBlastResultFormatter.class);

 public static final String COLUMN_MATCHED_RESULT = "matched_result";
 public static final String COLUMN_GENE_SOURCE_ID = "gene_source_id";

 private String getGeneSourceId(String defline) {
    return getField(defline, findGene(defline));
  }

  @Override
  public String[] getDeclaredColumns() {
    return ArrayUtil.append(
      super.getDeclaredColumns(),
      COLUMN_GENE_SOURCE_ID,
      COLUMN_MATCHED_RESULT);
  }

  @Override
  protected boolean assignExtraColumns(int index, String[] row, String[] columns, String defline) {
    if (columns[index].equals(COLUMN_MATCHED_RESULT)) {
      row[index] = "Y";
      return true;
    }
    if (columns[index].equals(COLUMN_GENE_SOURCE_ID)) {
      row[index] = getGeneSourceId(defline);
      return true;
    }
    return false;
  }

  @Override
  protected String getIdUrl(RecordClass recordClass, String projectId,
    String sourceId, String defline) throws EuPathServiceException {

    logger.debug("GENE FORMATTER: getIdUrl()  recordClass: {}", recordClass);

    try {
      // don't use passed recordclass; this formatter will only be used for transcript results / found genes
      String geneRecordClassName = TranscriptUtil.getGeneRecordClass(recordClass.getWdkModel()).getFullName();
      return getIdUrl(recordClass.getWdkModel(), geneRecordClassName, projectId, getGeneSourceId(defline));
    }
    catch (WdkModelException e) {
      throw new EuPathServiceException("Unable to format result", e);
    }
  }

}
