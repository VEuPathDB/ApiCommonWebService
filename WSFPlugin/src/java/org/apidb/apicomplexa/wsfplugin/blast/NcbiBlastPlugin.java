package org.apidb.apicomplexa.wsfplugin.blast;


public class NcbiBlastPlugin extends EuPathDBBlastPlugin {

  public NcbiBlastPlugin() {
    super(new NcbiBlastCommandFormatter(), new NcbiBlastResultFormatter());
  }
}
