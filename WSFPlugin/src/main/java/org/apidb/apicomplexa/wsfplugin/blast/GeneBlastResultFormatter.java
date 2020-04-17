package org.apidb.apicomplexa.wsfplugin.blast;

import org.apache.log4j.Logger;
import org.apidb.apicommon.model.TranscriptUtil;
import org.eupathdb.common.model.ProjectMapper;
import org.eupathdb.websvccommon.wsfplugin.EuPathServiceException;
import org.eupathdb.websvccommon.wsfplugin.blast.NcbiBlastResultFormatter;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.record.RecordClass;

public class GeneBlastResultFormatter extends NcbiBlastResultFormatter {

 private static final Logger logger = Logger.getLogger(GeneBlastResultFormatter.class);

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
  protected String getIdUrl(RecordClass recordClass, String projectId,
    String sourceId, String defline) throws EuPathServiceException {

    logger.debug("GENE FORMATTER: getIdUrl()  recordClass: " + recordClass);

    if (sourceId.endsWith("-p1")) {
      // until we handle proteins, trim "-p1" suffix to turn protein ID into transcript ID
      // (cannot test until we generate deflines with gene information,
      // until then the blast protein is breaking because it cannot find the gene ID)
      // TODO: why is this needed; this is dead code (sourceId not used)
      sourceId = sourceId.replace("-p1", "");
    }

    try {
      // don't use passed recordclass; this formatter will only be used for transcript results / found genes
      String geneRecordClassName = TranscriptUtil.getGeneRecordClass(recordClass.getWdkModel()).getFullName();
      return ProjectMapper.getMapper(recordClass.getWdkModel())
          .getRecordUrl(geneRecordClassName, projectId, getGeneSourceId(defline));
    }
    catch (WdkModelException e) {
      throw new EuPathServiceException("Unable to format result", e);
    }
  }

}
