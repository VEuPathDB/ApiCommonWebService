package org.apidb.apicomplexa.wsfplugin.spanlogic;

import org.gusdb.wdk.model.WdkModelException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SpanSourceTest {

  @Test
  public void unregisteredRecordClassThrowsNamingTheClass() {
    try {
      SpanCompositionPlugin.spanSourceFor("OrfRecordClasses.OrfRecordClass");
      fail("expected WdkModelException for an unregistered record class");
    }
    catch (WdkModelException e) {
      assertTrue("message should name the record class, was: " + e.getMessage(),
          e.getMessage().contains("OrfRecordClasses.OrfRecordClass"));
    }
  }

  @Test
  public void transcriptRecordClassIsRegistered() throws WdkModelException {
    assertNotNull(SpanCompositionPlugin.spanSourceFor("TranscriptRecordClasses.TranscriptRecordClass"));
  }

  @Test
  public void dynSpanRecordClassIsRegistered() throws WdkModelException {
    assertNotNull(SpanCompositionPlugin.spanSourceFor("DynSpanRecordClasses.DynSpanRecordClass"));
  }

  @Test
  public void variantRecordClassIsRegistered() throws WdkModelException {
    assertNotNull(SpanCompositionPlugin.spanSourceFor("VariantRecordClasses.VariantRecordClass"));
  }

  @Test
  public void onlyVariantIsStrandless() throws WdkModelException {
    assertEquals(false,
        SpanCompositionPlugin.spanSourceFor("TranscriptRecordClasses.TranscriptRecordClass").isStrandless());
    assertEquals(false,
        SpanCompositionPlugin.spanSourceFor("DynSpanRecordClasses.DynSpanRecordClass").isStrandless());
    assertEquals(true,
        SpanCompositionPlugin.spanSourceFor("VariantRecordClasses.VariantRecordClass").isStrandless());
  }

  private static final String[] REGION = { "(CASE WHEN COALESCE(fl.is_reversed, 0) = 0 THEN (start_min + 1*(0)) END)",
                                           "(CASE WHEN COALESCE(fl.is_reversed, 0) = 0 THEN (end_max + 1*(0)) END)" };
  private static final String CACHE = "(SELECT source_id, gene_source_id, project_id, wdk_weight FROM some_cache)";

  private static String sqlFor(String recordClassName) throws WdkModelException {
    return SpanCompositionPlugin.spanSourceFor(recordClassName).createTableSql("temp_1", REGION, CACHE);
  }

  @Test
  public void transcriptSourceJoinsOnGeneAndKeepsTopLevelFilter() throws WdkModelException {
    String sql = sqlFor("TranscriptRecordClasses.TranscriptRecordClass");
    assertTrue(sql, sql.contains("FROM apidb.FeatureLocation fl"));
    assertTrue(sql, sql.contains("fl.feature_source_id = ca.gene_source_id"));
    assertTrue("is_top_level filter is load-bearing for PAR genes: " + sql,
        sql.contains("fl.is_top_level = 1"));
    assertTrue(sql, sql.contains("fl.feature_type = 'GeneFeature'"));
    assertTrue(sql, sql.contains("CREATE TABLE temp_1 AS"));
  }

  @Test
  public void noSourceEmitsOracleOnlyConstructs() throws WdkModelException {
    for (String rc : new String[] { "TranscriptRecordClasses.TranscriptRecordClass",
                                    "DynSpanRecordClasses.DynSpanRecordClass",
                                    "VariantRecordClasses.VariantRecordClass" }) {
      String sql = sqlFor(rc);
      assertTrue(rc + " must not use rownum: " + sql, !sql.contains("rownum"));
      assertTrue(rc + " must not use DECODE: " + sql, !sql.contains("DECODE("));
      assertTrue(rc + " must alias its location table fl: " + sql, sql.contains(" fl,") || sql.contains(" fl "));
    }
  }

  @Test
  public void dynSpanSourceParsesCoordinatesWithoutOracleSyntax() throws WdkModelException {
    String sql = sqlFor("DynSpanRecordClasses.DynSpanRecordClass");
    assertTrue(sql, sql.contains("CASE WHEN regexp_substr(source_id, '[^:]+', 1, 3) = 'r' THEN 1 ELSE 0 END AS is_reversed"));
    assertTrue("coordinates come from the step's own cache table: " + sql, sql.contains(CACHE));
    assertTrue("one row per record, so no is_top_level: " + sql, !sql.contains("is_top_level"));
    assertTrue("one row per record, so no feature_type: " + sql, !sql.contains("feature_type"));
    assertTrue(sql, sql.contains("fl.feature_source_id = ca.source_id"));
  }
}
