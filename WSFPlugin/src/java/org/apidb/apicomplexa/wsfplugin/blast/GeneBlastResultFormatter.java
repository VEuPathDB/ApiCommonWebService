package org.apidb.apicomplexa.wsfplugin.blast;

import java.net.URLEncoder;
import java.io.UnsupportedEncodingException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

import org.apache.log4j.Logger;
import org.eupathdb.websvccommon.wsfplugin.blast.NcbiBlastResultFormatter;
import org.eupathdb.websvccommon.wsfplugin.blast.AbstractBlastPlugin;
import org.apidb.apicomplexa.wsfplugin.blast.GeneBlastPlugin;
import org.eupathdb.common.model.view.BlastSummaryViewHandler;
import org.eupathdb.websvccommon.wsfplugin.EuPathServiceException;
import org.gusdb.fgputil.FormatUtil;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.PluginUserException;

public class GeneBlastResultFormatter extends NcbiBlastResultFormatter {

 @Override
  public void processAlignment(PluginResponse response, String[] columns, String recordClass, String dbType,
      Map<String, String> summaries, String alignment) throws PluginUserException, PluginModelException {
    try {
      // get the defline, and get organism from it
      String defline = alignment.substring(0, alignment.indexOf("Length="));
      String organism = "none";
      try { // Ortho does not have organism info in defline
        organism = getField(defline, findOrganism(defline));
      }
      catch (NullPointerException e) {}
      String projectId = getProject(organism);

      // get gene_source_id from defline
      String geneSourceId = getField(defline, findGene(defline));

      // get the source id in the alignment, and insert a link there
      int[] sourceIdLocation = findSourceId(alignment);
      String sourceId = getField(defline, sourceIdLocation);
      String idUrl = getIdUrl(recordClass, projectId, sourceId, geneSourceId);
      alignment = insertUrl(alignment, sourceIdLocation, idUrl, sourceId);

      // get score and e-value from summary;
      String summary = summaries.get(sourceId);
      String evalue = getField(summary, findEvalue(summary));
      int[] scoreLocation = findScore(summary);
      float score = Float.valueOf(getField(summary, scoreLocation));

      // insert a link to the alignment section - need to do it before the id link.
      summary = insertUrl(summary, scoreLocation, "#" + sourceId);
      // insert id url into the summary
      summary = insertUrl(summary, findSourceId(summary), idUrl);

      // insert the gbrowse link if the DB type is genome
      if (dbType != null && dbType.equals(DB_TYPE_GENOME))
        alignment = insertGbrowseLink(alignment, projectId, sourceId);

      // format and write the row
      String[] row = formatRow(columns, projectId, sourceId, geneSourceId, summary, alignment, evalue, score);
      response.addRow(row);
    }
    catch (SQLException ex) {
      throw new EuPathServiceException(ex);
    }
  }



  private String[] formatRow(String[] columns, String projectId, String sourceId, String geneSourceId, String summary,
      String alignment, String evalue, float score) throws EuPathServiceException {
    String[] evalueParts = evalue.split("e");
    String evalueExp = (evalueParts.length == 2) ? evalueParts[1] : "0";
    String evalueMant = evalueParts[0];
    // sometimes the mant part is empty if the blast score is very high, assign a default 1.
    if (evalueMant.length() == 0)
      evalueMant = "1";
    String[] row = new String[columns.length];
    for (int i = 0; i < columns.length; i++) {
      if (columns[i].equals(AbstractBlastPlugin.COLUMN_ALIGNMENT)) {
        row[i] = alignment;
      }
      else if (columns[i].equals(AbstractBlastPlugin.COLUMN_EVALUE_EXP)) {
        row[i] = evalueExp;
      }
      else if (columns[i].equals(AbstractBlastPlugin.COLUMN_EVALUE_MANT)) {
        row[i] = evalueMant;
      }
      else if (columns[i].equals(AbstractBlastPlugin.COLUMN_IDENTIFIER)) {
        row[i] = sourceId;
      }
      else if (columns[i].equals(AbstractBlastPlugin.COLUMN_PROJECT_ID)) {
        row[i] = projectId;
      }
      else if (columns[i].equals(AbstractBlastPlugin.COLUMN_SCORE)) {
        row[i] = Float.toString(score);
      }
      else if (columns[i].equals(AbstractBlastPlugin.COLUMN_SUMMARY)) {
        row[i] = summary;
      }
      else if (columns[i].equals(GeneBlastPlugin.COLUMN_MATCHED_RESULT)) {
	  row[i] = new String("Y");
      }
      else if (columns[i].equals(GeneBlastPlugin.COLUMN_GENE_SOURCE_ID)) {
	  row[i] = geneSourceId;
      }
      else {
        throw new EuPathServiceException("Unsupported blast result column: " + columns[i]);
      }
    }
    return row;
  }


  private String getIdUrl(String recordClass, String projectId,
		                      String sourceId,  String geneSourceId) throws EuPathServiceException {
    try {
    String url = "showRecord.do?name=" + recordClass + "&project_id="
        + URLEncoder.encode(projectId, "UTF-8") + "&source_id="
	  		+ URLEncoder.encode(sourceId, "UTF-8") + "&gene_source_id="
        + URLEncoder.encode(geneSourceId, "UTF-8");
    return url;
    } catch (UnsupportedEncodingException ex) {
      throw new EuPathServiceException(ex);
    }
  }


}
