package org.apidb.apicomplexa.wsfplugin.spanlogic;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class TranscriptSpanCompositionPlugin extends SpanCompositionPlugin {
  
  public static final String COLUMN_GENE_SOURCE_ID = "gene_source_id";
  
  @Override
  protected String[] makeRow(Map<String, Integer> columnOrders, Feature feature, String matched) {
    String[] row = new String[columnOrders.size()];
    row[columnOrders.get(COLUMN_SOURCE_ID)] = feature.sourceId;
    row[columnOrders.get(COLUMN_GENE_SOURCE_ID)] = feature.geneSourceId;
    row[columnOrders.get(COLUMN_PROJECT_ID)] = feature.projectId;
    row[columnOrders.get(COLUMN_FEATURE_REGION)] = feature.getRegion();
    row[columnOrders.get(COLUMN_MATCHED_COUNT)] = Integer.toString(feature.matched.size());
    row[columnOrders.get(COLUMN_WDK_WEIGHT)] = Integer.toString(feature.weight);
    row[columnOrders.get(COLUMN_MATCHED_REGIONS)] = matched;
    row[columnOrders.get(COLUMN_MATCHED_RESULT)] = "Y";
    return row;
  }

  @Override
  public String[] getColumns() {
    return new String[] { COLUMN_PROJECT_ID, COLUMN_SOURCE_ID, COLUMN_GENE_SOURCE_ID, COLUMN_WDK_WEIGHT, COLUMN_FEATURE_REGION,
              COLUMN_MATCHED_COUNT, COLUMN_MATCHED_REGIONS, COLUMN_MATCHED_RESULT };
  }

}
