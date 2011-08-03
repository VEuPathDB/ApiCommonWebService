/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.motifsearch;

import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author Jerric, modified by Cristina 2010 to add DNA motif
 * @created Jan 31, 2006
 */

// geneID could be an ORF or a genomic sequence deending on who uses the plugin
public class OrfMotifPlugin extends ProteinMotifPlugin {

    // let's store files in same directory
    public static final String FIELD_ORF_DEFLINE_REGEX = "OrfDeflineRegex";

    /**
     * @throws WsfServiceException
     * 
     */
    public OrfMotifPlugin() throws WsfServiceException {
        super();
    }

    @Override
    protected String getDeflineField() {
        return FIELD_ORF_DEFLINE_REGEX;
    }
}