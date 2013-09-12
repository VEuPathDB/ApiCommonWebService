package org.apidb.apicomplexa.wsfplugin.blast;


/**
 * 
 */

/**
 * @author Jerric
 * @created Nov 2, 2005
 */

public class WuBlastPlugin extends EuPathDBBlastPlugin {

    /**
     */
    public WuBlastPlugin()   {
        super(new WuBlastCommandFormatter(), new WuBlastResultFormatter());
    }
}