package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.util.Properties;
import java.util.regex.Pattern;

public class MotifConfig {

  public static final String FIELD_CONTEXT_LENGTH = "ContextLength";

  private Pattern pattern;
  private int contextLength = 20;

  public MotifConfig(Properties properties, String regexField,
      String defaultRegex) {

    // load optional properties
    String regex = properties.getProperty(regexField, defaultRegex);
    pattern = Pattern.compile(regex);

    if (properties.containsKey(FIELD_CONTEXT_LENGTH))
      contextLength = Integer.valueOf(properties.getProperty(FIELD_CONTEXT_LENGTH));
  }

  public Pattern getDeflinePattern() {
    return pattern;
  }

  public int getContextLength() {
    return contextLength;
  }

}
