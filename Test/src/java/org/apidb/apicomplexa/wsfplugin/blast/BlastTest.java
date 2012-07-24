package org.apidb.apicomplexa.wsfplugin.blast;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;
import java.util.Random;

import junit.framework.Assert;

import org.apidb.apicommon.model.ProjectMapper;
import org.apidb.apicomplexa.wsfplugin.MockProjectMapper;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.WdkUserException;
import org.gusdb.wsf.plugin.WsfServiceException;
import org.junit.Before;
import org.junit.Test;

public class BlastTest {

  private final String[] columns = { AbstractBlastPlugin.COLUMN_BLOCK,
      AbstractBlastPlugin.COLUMN_COUNTER, AbstractBlastPlugin.COLUMN_FOOTER,
      AbstractBlastPlugin.COLUMN_HEADER, AbstractBlastPlugin.COLUMN_ID,
      AbstractBlastPlugin.COLUMN_PROJECT_ID, AbstractBlastPlugin.COLUMN_ROW };

  private static final String SYS_PROJECT_HOME = "project_home";

  private final String projectHome;

  private Properties properties;

  public BlastTest() throws Exception {
    projectHome = System.getProperty(SYS_PROJECT_HOME);
    if (projectHome == null)
      throw new Exception("Required system environment variable not set: "
          + SYS_PROJECT_HOME);
  }

  @Before
  public void prepareConfigFile() throws InvalidPropertiesFormatException,
      FileNotFoundException, IOException {

    // prepare the config file
    properties = new Properties();

    // read the sample config file with default values
    String sampleFile = projectHome
        + "/ApiCommonWebService/WSFPlugin/config/blast-config.xml.sample";
    InputStream inStream = new FileInputStream(sampleFile);
    properties.loadFromXML(inStream);
    inStream.close();
  }

  /**
   * test if the all the fields are populated correctly from the config file;
   * 
   * @throws WsfServiceException
   * @throws FileNotFoundException
   * @throws IOException
   */
  @Test
  public void testLoadAllFields() throws WsfServiceException,
      FileNotFoundException, IOException {
    // populate all fields with random values
    Random random = new Random();

    String blastPath = "blastPath" + random.nextInt();
    String tempPath = "tempPath" + random.nextInt();
    String dbName = "dbName" + random.nextInt();
    long timeout = random.nextInt(Integer.MAX_VALUE);
    String extraOptions = "extra" + random.nextInt();
    String sourceIdRegex = "sourceId" + random.nextInt();
    int sourceIdRegexIndex = random.nextInt(Integer.MAX_VALUE);
    String organismRegex = "organism" + random.nextInt();
    int organismRegexIndex = random.nextInt(Integer.MAX_VALUE);
    String scoreRegex = "score" + random.nextInt();
    int scoreRegexIndex = random.nextInt(Integer.MAX_VALUE);

    // apply changes to the properties
    Properties properties = new Properties();
    properties.setProperty(BlastConfig.FIELD_BLAST_PATH, blastPath);
    properties.setProperty(BlastConfig.FIELD_TEMP_PATH, tempPath);
    properties.setProperty(BlastConfig.FIELD_BLAST_DB_NAME, dbName);
    properties.setProperty(BlastConfig.FIELD_TIMEOUT, Long.toString(timeout));
    properties.setProperty(BlastConfig.FIELD_EXTRA_OPTIONS, extraOptions);
    properties.setProperty(BlastConfig.FIELD_SOURCE_ID_REGEX, sourceIdRegex);
    properties.setProperty(BlastConfig.FIELD_SOURCE_ID_REGEX_INDEX,
        Integer.toString(sourceIdRegexIndex));
    properties.setProperty(BlastConfig.FIELD_ORGANISM_REGEX, organismRegex);
    properties.setProperty(BlastConfig.FIELD_ORGANISM_REGEX_INDEX,
        Integer.toString(organismRegexIndex));
    properties.setProperty(BlastConfig.FIELD_SCORE_REGEX, scoreRegex);
    properties.setProperty(BlastConfig.FIELD_SCORE_REGEX_INDEX,
        Integer.toString(scoreRegexIndex));

    // create a plugin, and test the fields
    BlastConfig config = new BlastConfig(properties);

    Assert.assertEquals(blastPath, config.getBlastPath());
    Assert.assertEquals(tempPath, config.getTempPath());
    Assert.assertEquals(dbName, config.getBlastDbName());
    Assert.assertEquals(timeout, config.getTimeout());
    Assert.assertEquals(extraOptions, config.getExtraOptions());
    Assert.assertEquals(sourceIdRegex, config.getSourceIdRegex());
    Assert.assertEquals(sourceIdRegexIndex, config.getSourceIdRegexIndex());
    Assert.assertEquals(organismRegex, config.getOrganismRegex());
    Assert.assertEquals(organismRegexIndex, config.getOrganismRegexIndex());
    Assert.assertEquals(scoreRegex, config.getScoreRegex());
    Assert.assertEquals(scoreRegexIndex, config.getScoreRegexIndex());

    // make sure to drop the temp path
    File tempDir = new File(tempPath);
    if (tempDir.exists())
      tempDir.delete();
  }

  /**
   * test if the defaults are used if an optional field is not specified.
   * 
   * @throws WsfServiceException
   */
  @Test
  public void testUseDefaultFields() throws WsfServiceException {
    // populate all fields with random values
    Random random = new Random();

    String blastPath = "blastPath" + random.nextInt();
    Properties properties = new Properties();
    properties.setProperty(BlastConfig.FIELD_BLAST_PATH, blastPath);

    BlastConfig config = new BlastConfig(properties);
    BlastConfig defaultConfig = new BlastConfig(this.properties);

    Assert.assertEquals(blastPath, config.getBlastPath());
    Assert.assertEquals(defaultConfig.getTempPath(), config.getTempPath());
    Assert.assertEquals(defaultConfig.getBlastDbName(), config.getBlastDbName());
    Assert.assertEquals(defaultConfig.getTimeout(), config.getTimeout());
    Assert.assertEquals(defaultConfig.getExtraOptions(),
        config.getExtraOptions());
    Assert.assertEquals(defaultConfig.getSourceIdRegex(),
        config.getSourceIdRegex());
    Assert.assertEquals(defaultConfig.getSourceIdRegexIndex(),
        config.getSourceIdRegexIndex());
    Assert.assertEquals(defaultConfig.getOrganismRegex(),
        config.getOrganismRegex());
    Assert.assertEquals(defaultConfig.getOrganismRegexIndex(),
        config.getOrganismRegexIndex());
    Assert.assertEquals(defaultConfig.getScoreRegex(), config.getScoreRegex());
    Assert.assertEquals(defaultConfig.getScoreRegexIndex(),
        config.getScoreRegexIndex());
  }

  @Test
  public void testFormatNcbiResultsWithHits() throws URISyntaxException,
      WsfServiceException, WdkModelException, WdkUserException, IOException,
      SQLException {
    StringBuffer message = new StringBuffer();
    String[][] results = format(new NcbiBlastResultFormatter(),
        "ncbi-blast-hits.out", message);

    Assert.assertEquals(5, results.length);
    Assert.assertEquals(columns.length, results[0].length);
    Assert.assertTrue(results[0][2] == null);
    Assert.assertTrue(results[0][3].length() > 100);
    Assert.assertTrue(results[4][2].length() > 100);
    Assert.assertTrue(results[4][3] == null);
    Assert.assertTrue(message.length() == 0);
  }

  @Test
  public void testFormatNcbiResultsWithoutHits() throws WdkModelException,
      WdkUserException, WsfServiceException, URISyntaxException, IOException,
      SQLException {
    StringBuffer message = new StringBuffer();
    String[][] results = format(new NcbiBlastResultFormatter(),
        "ncbi-blast-no-hits.out", message);

    Assert.assertEquals(0, results.length);
    Assert.assertTrue(message.length() > 100);
  }

  @Test
  public void testFormatNcbiResultsWithError() throws WdkModelException,
      WdkUserException, WsfServiceException, URISyntaxException, IOException,
      SQLException {
    StringBuffer message = new StringBuffer();
    String[][] results = format(new NcbiBlastResultFormatter(),
        "ncbi-blast-err.out", message);

    Assert.assertEquals(0, results.length);
    Assert.assertTrue(message.length() > 100);
  }

  @Test
  public void testFormatWuResultsWithHits() throws URISyntaxException,
      WsfServiceException, WdkModelException, WdkUserException, IOException,
      SQLException {
    StringBuffer message = new StringBuffer();
    String[][] results = format(new WuBlastResultFormatter(),
        "wu-blast-hits.out", message);

    Assert.assertEquals(5, results.length);
    Assert.assertEquals(columns.length, results[0].length);
    Assert.assertTrue(results[0][2] == null);
    Assert.assertTrue(results[0][3].length() > 100);
    Assert.assertTrue(results[4][2].length() > 100);
    Assert.assertTrue(results[4][3] == null);
    Assert.assertTrue(message.length() == 0);
  }

  @Test
  public void testFormatWuResultsWithoutHits() throws WdkModelException,
      WdkUserException, WsfServiceException, URISyntaxException, IOException,
      SQLException {
    StringBuffer message = new StringBuffer();
    String[][] results = format(new WuBlastResultFormatter(),
        "wu-blast-no-hit.out", message);

    Assert.assertEquals(0, results.length);
    Assert.assertTrue(message.length() > 100);
  }

  @Test
  public void testFormatWuResultsWithError() throws WdkModelException,
      WdkUserException, WsfServiceException, URISyntaxException, IOException,
      SQLException {
    StringBuffer message = new StringBuffer();
    String[][] results = format(new WuBlastResultFormatter(),
        "wu-blast-err.out", message);

    Assert.assertEquals(0, results.length);
    Assert.assertTrue(message.length() > 100);
  }

  private String[][] format(ResultFormatter formatter, String fileName,
      StringBuffer message) throws WsfServiceException, URISyntaxException,
      WdkModelException, WdkUserException, IOException, SQLException {
    BlastConfig config = new BlastConfig(properties);
    ProjectMapper projectMapper = new MockProjectMapper();
    formatter.setConfig(config);
    formatter.setProjectMapper(projectMapper);

    // get blast output file
    URL url = this.getClass().getResource("/blast/" + fileName);
    File resultFile = new File(url.toURI());
    String dbType = "Genomics";
    String recordClass = "GeneRecordClasses.GeneRecordClass";

    return formatter.formatResult(columns, resultFile, dbType, recordClass,
        message);
  }
}
