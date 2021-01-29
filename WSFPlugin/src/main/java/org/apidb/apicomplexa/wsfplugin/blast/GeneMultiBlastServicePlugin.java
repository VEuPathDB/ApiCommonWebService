package org.apidb.apicomplexa.wsfplugin.blast;

import org.eupathdb.websvccommon.wsfplugin.blast.AbstractMultiBlastServicePlugin;

public class GeneMultiBlastServicePlugin extends AbstractMultiBlastServicePlugin {

  public GeneMultiBlastServicePlugin() {
    super(new GeneBlastResultFormatter());
  }

}
