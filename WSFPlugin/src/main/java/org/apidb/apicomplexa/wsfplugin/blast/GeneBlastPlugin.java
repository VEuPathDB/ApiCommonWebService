package org.apidb.apicomplexa.wsfplugin.blast;

public class GeneBlastPlugin extends EuPathBlastPlugin {

  public GeneBlastPlugin() {
    super(new EuPathBlastCommandFormatter(), new GeneBlastResultFormatter());
  }

}
