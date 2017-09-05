/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.motifsearch;

/**
 * @author Jerric, modified by Cristina 2010 to add DNA motif
 * @created Jan 31, 2006
 */

// geneID could be an ORF or a genomic sequence deending on who uses the plugin
public class OrfMotifPlugin extends AAMotifPlugin {

  // let's store files in same directory
  private static final String CUSTOM_FIELD_REGEX = "OrfDeflineRegex";

  public OrfMotifPlugin() {
    super(CUSTOM_FIELD_REGEX, DEFAULT_REGEX);
  }
}
