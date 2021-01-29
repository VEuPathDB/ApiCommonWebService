package org.apidb.apicomplexa.wsfplugin.blast;

import org.eupathdb.websvccommon.wsfplugin.blast.AbstractMultiBlastServicePlugin;
import org.eupathdb.websvccommon.wsfplugin.blast.NcbiBlastResultFormatter;

public class EuPathMultiBlastServicePlugin extends AbstractMultiBlastServicePlugin {

  public EuPathMultiBlastServicePlugin() {
    super(new NcbiBlastResultFormatter());
  }

}
