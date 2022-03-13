package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.util.HashMap;
import java.util.Map;

/**
 * Superclass for Orf and Protein motif plugins
 *
 * geneID could be an ORF or a genomic sequence depending on who uses the plugin
 *
 * @author Jerric, modified by Cristina 2010 to add DNA motif
 * @created Jan 31, 2006
 */
abstract class AAMotifPlugin extends AbstractMotifPlugin {

  //protected static final String DEFAULT_REGEX = ">(?:\\w*\\|)*([^|\\s]+)\\s*\\|.*?\\s*organism=([^|\\s]+)";
  private static final String DEFAULT_REGEX = ">.*transcript=([^|\\s]+).*organism=([^|\\s]+)";

  private static final Map<Character, String> SYMBOL_MAP = new HashMap<>(){{
    put('0', "DE");
    put('1', "ST");
    put('2', "ILV");
    put('3', "FHWY");
    put('4', "KRH");
    put('5', "DEHKR");
    put('6', "AVILMFYW");
    put('7', "KRHDENQ");
    put('8', "CDEHKNQRST");
    put('9', "ACDGNPSTV");
    put('B', "AGS");
    put('Z', "ACDEGHKNQRST");
    put('X', "ACDEFGHIKLMNPQRSTVWY");
  }};

  /**
   * This constructor is provided to support children extension for motif search
   * of other protein related types, such as ORF, etc.
   *
   * @param regexField
   */
  protected AAMotifPlugin(String regexField) {
    super(regexField, DEFAULT_REGEX);
  }

  @Override
  protected Map<Character, String> getSymbols() {
    return SYMBOL_MAP;
  }

  @Override
  protected MatchFinder getMatchFinder(MotifConfig config) {
    return new AAMatchFinder(config);
  }
}
