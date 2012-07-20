package org.apidb.apicomplexa.wsfplugin.blast;


public class NcbiBlastPlugin extends AbstractBlastPlugin {

  public NcbiBlastPlugin() {
    super(new NcbiBlastCommandFormatter(), new NcbiBlastResultFormatter());
  }
}
