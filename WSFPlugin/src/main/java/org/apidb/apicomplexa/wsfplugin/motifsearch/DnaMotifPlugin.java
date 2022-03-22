package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Jerric, modified by Cristina 2010 to add DNA motif
 * @created Jan 31, 2006
 */
public class DnaMotifPlugin extends AbstractMotifPlugin {

  public static final String FIELD_REGEX = "DnaDeflineRegex";
  public static final String DEFAULT_REGEX = ">(?:\\w*\\|)*([^|\\s]+)\\s*\\|.*?\\s*strand=\\(([+\\-])\\)\\s*\\|\\s*organism=([^|\\s]+)";

  public static final Map<Character, String> SYMBOL_MAP = new HashMap<>(){{
    put('R', "AG");
    put('Y', "CT");
    put('M', "AC");
    put('K', "GT");
    put('S', "CG");
    put('W', "AT");
    put('B', "CGT");
    put('D', "AGT");
    put('H', "ACT");
    put('V', "ACG");
    put('N', "ACGT");
    
  }};

  public DnaMotifPlugin() {
    super(FIELD_REGEX, DEFAULT_REGEX);
  }

  @Override
  protected Map<Character, String> getSymbols() {
    return SYMBOL_MAP;
  }

  @Override
  protected MatchFinder getMatchFinder(MotifConfig config) {
    return new DnaMatchFinder(config);
  }
}
