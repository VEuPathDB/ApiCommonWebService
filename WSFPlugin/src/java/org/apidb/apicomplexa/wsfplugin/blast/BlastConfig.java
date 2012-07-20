package org.apidb.apicomplexa.wsfplugin.blast;

import java.io.File;
import java.util.Properties;

import org.gusdb.wsf.plugin.WsfServiceException;

public class BlastConfig {

  /**
   * This is the only required field in the config file.
   */
  public static final String FIELD_BLAST_PATH = "BlastPath";
  public static final String FIELD_TEMP_PATH = "TempPath";
  public static final String FIELD_BLAST_DB_NAME = "BlastDbName";
  public static final String FIELD_EXTRA_OPTIONS = "ExtraOptions";
  public static final String FIELD_TIMEOUT = "Timeout";
  public static final String FIELD_SOURCE_ID_REGEX = "SourceIdRegex";
  public static final String FIELD_SOURCE_ID_REGEX_INDEX = "SourceIdRegexIndex";
  public static final String FIELD_ORGANISM_REGEX = "OrganismRegex";
  public static final String FIELD_ORGANISM_REGEX_INDEX = "OrganismRegexIndex";
  public static final String FIELD_SCORE_REGEX = "ScoreRegex";
  public static final String FIELD_SCORE_REGEX_INDEX = "ScoreRegexIndex";

  private String blastPath;

  private String tempPath = "/tmp/blast";
  private String extraOptions = "";

  private long timeout = 300;
  private String blastDbName = "$$BlastDatabaseOrganism$$$$BlastDatabaseType$$";

  private String sourceIdRegex = "^>*(?:\\w*\\|)*(\\S+)";
  private int sourceIdRegexIndex = 1;
  private String organismRegex = "\\|\\s*organism=([^_|\\s]+)";
  private int organismRegexIndex = 1;
  private String scoreRegex = "^.+?\\s\\s+(?:[+-]\\d\\s+)?(\\d+)";
  private int scoreRegexIndex = 1;

  public BlastConfig(Properties properties) throws WsfServiceException {

    // load properties
    blastPath = properties.getProperty(FIELD_BLAST_PATH);
    if (blastPath == null)
      throw new WsfServiceException("The BLAST program path is not "
          + "specified in the config file.");

    // create temp path if it doesn't exist
    if (properties.containsKey(FIELD_TEMP_PATH))
      tempPath = properties.getProperty(FIELD_TEMP_PATH);
    File tempDir = new File(tempPath);
    if (!tempDir.exists())
      tempDir.mkdirs();

    if (properties.containsKey(FIELD_BLAST_DB_NAME))
      blastDbName = properties.getProperty(FIELD_BLAST_DB_NAME);

    if (properties.containsKey(FIELD_EXTRA_OPTIONS))
      extraOptions = properties.getProperty(FIELD_EXTRA_OPTIONS);

    if (properties.containsKey(FIELD_TIMEOUT))
      timeout = Integer.valueOf(properties.getProperty(FIELD_TIMEOUT));

    // get the regex for parsing the blast result
    if (properties.containsKey(FIELD_SOURCE_ID_REGEX))
      sourceIdRegex = properties.getProperty(FIELD_SOURCE_ID_REGEX);

    if (properties.containsKey(FIELD_SOURCE_ID_REGEX_INDEX))
      sourceIdRegexIndex = Integer.valueOf(properties.getProperty(FIELD_SOURCE_ID_REGEX_INDEX));

    if (properties.containsKey(FIELD_ORGANISM_REGEX))
      organismRegex = properties.getProperty(FIELD_ORGANISM_REGEX);

    if (properties.containsKey(FIELD_ORGANISM_REGEX_INDEX))
      organismRegexIndex = Integer.valueOf(properties.getProperty(FIELD_ORGANISM_REGEX_INDEX));

    if (properties.containsKey(FIELD_SCORE_REGEX))
      scoreRegex = properties.getProperty(FIELD_SCORE_REGEX);

    if (properties.containsKey(FIELD_SCORE_REGEX_INDEX))
      scoreRegexIndex = Integer.valueOf(properties.getProperty(FIELD_SCORE_REGEX_INDEX));
  }

  public String getBlastPath() {
    return blastPath;
  }

  public String getTempPath() {
    return tempPath;
  }

  public String getExtraOptions() {
    return extraOptions;
  }

  public long getTimeout() {
    return timeout;
  }

  public String getBlastDbName() {
    return blastDbName;
  }

  public String getSourceIdRegex() {
    return sourceIdRegex;
  }

  public int getSourceIdRegexIndex() {
    return sourceIdRegexIndex;
  }

  public String getOrganismRegex() {
    return organismRegex;
  }

  public int getOrganismRegexIndex() {
    return organismRegexIndex;
  }

  public String getScoreRegex() {
    return scoreRegex;
  }

  public int getScoreRegexIndex() {
    return scoreRegexIndex;
  }

  public void setBlastPath(String blastPath) {
    this.blastPath = blastPath;
  }

  public void setTempPath(String tempPath) {
    this.tempPath = tempPath;
  }

  public void setExtraOptions(String extraOptions) {
    this.extraOptions = extraOptions;
  }

  public void setTimeout(long timeout) {
    this.timeout = timeout;
  }

  public void setBlastDbName(String blastDbName) {
    this.blastDbName = blastDbName;
  }

  public void setSourceIdRegex(String sourceIdRegex) {
    this.sourceIdRegex = sourceIdRegex;
  }

  public void setSourceIdRegexIndex(int sourceIdRegexIndex) {
    this.sourceIdRegexIndex = sourceIdRegexIndex;
  }

  public void setOrganismRegex(String organismRegex) {
    this.organismRegex = organismRegex;
  }

  public void setOrganismRegexIndex(int organismRegexIndex) {
    this.organismRegexIndex = organismRegexIndex;
  }

  public void setScoreRegex(String scoreRegex) {
    this.scoreRegex = scoreRegex;
  }

  public void setScoreRegexIndex(int scoreRegexIndex) {
    this.scoreRegexIndex = scoreRegexIndex;
  }

}
