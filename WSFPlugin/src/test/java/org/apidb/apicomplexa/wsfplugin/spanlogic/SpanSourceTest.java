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
}
