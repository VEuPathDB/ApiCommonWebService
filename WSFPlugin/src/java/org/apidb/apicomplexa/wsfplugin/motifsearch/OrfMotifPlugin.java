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
  private static final String FIELD_REGEX = "OrfDeflineRegex";

  private static final String DEFAULT_REGEX = ">(?:\\w*\\|)*([^|\\s]+)\\s*\\|.*?\\s*organism=([^|\\s]+)";

  public OrfMotifPlugin() {
    super(FIELD_REGEX, DEFAULT_REGEX);
  }
}
