package org.apidb.apicomplexa.wsfplugin.blast;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.InvalidPropertiesFormatException;
import java.util.Properties;
import java.util.Random;

import junit.framework.Assert;

import org.apidb.apicomplexa.wsfplugin.MockProjectMapper;
import org.eupathdb.common.model.ProjectMapper;
import org.eupathdb.websvccommon.wsfplugin.blast.AbstractBlastPlugin;
import org.eupathdb.websvccommon.wsfplugin.blast.BlastConfig;
import org.eupathdb.websvccommon.wsfplugin.blast.NcbiBlastResultFormatter;
import org.eupathdb.websvccommon.wsfplugin.blast.ResultFormatter;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.WsfPluginException;
import org.junit.Before;
import org.junit.Test;

public class BlastTest {

  private final String[] columns = { AbstractBlastPlugin.COLUMN_IDENTIFIER,
      AbstractBlastPlugin.COLUMN_PROJECT_ID,
      AbstractBlastPlugin.COLUMN_SUMMARY, AbstractBlastPlugin.COLUMN_ALIGNMENT,
      AbstractBlastPlugin.COLUMN_EVALUE_MANT,
      AbstractBlastPlugin.COLUMN_EVALUE_EXP, AbstractBlastPlugin.COLUMN_SCORE };

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
   */
  @Test
  public void testLoadAllFields() throws WsfPluginException {
    // populate all fields with random values
    Random random = new Random();

    String blastPath = "blastPath" + random.nextInt();
    String tempPath = "tempPath" + random.nextInt();
    long timeout = random.nextInt(Integer.MAX_VALUE);
    String extraOptions = "extra" + random.nextInt();
    String sourceIdRegex = "sourceId" + random.nextInt();
    String organismRegex = "organism" + random.nextInt();

    // apply changes to the properties
    Properties properties = new Properties();
    properties.setProperty(BlastConfig.FIELD_BLAST_PATH, blastPath);
    properties.setProperty(BlastConfig.FIELD_TEMP_PATH, tempPath);
    properties.setProperty(BlastConfig.FIELD_TIMEOUT, Long.toString(timeout));
    properties.setProperty(BlastConfig.FIELD_EXTRA_OPTIONS, extraOptions);
    properties.setProperty(BlastConfig.FIELD_IDENTIFIER_REGEX, sourceIdRegex);
    properties.setProperty(BlastConfig.FIELD_ORGANISM_REGEX, organismRegex);

    // create a plugin, and test the fields
    BlastConfig config = new BlastConfig(properties);

    Assert.assertEquals(blastPath, config.getBlastPath());
    Assert.assertEquals(tempPath, config.getTempDir().getName());
    Assert.assertEquals(timeout, config.getTimeout());
    Assert.assertEquals(extraOptions, config.getExtraOptions());
    Assert.assertEquals(sourceIdRegex, config.getSourceIdRegex());
    Assert.assertEquals(organismRegex, config.getOrganismRegex());

    // make sure to drop the temp path
    File tempDir = new File(tempPath);
    if (tempDir.exists())
      tempDir.delete();
  }

  /**
   * test if the defaults are used if an optional field is not specified.
   */
  @Test
  public void testUseDefaultFields() throws WsfPluginException {
    // populate all fields with random values
    Random random = new Random();

    String blastPath = "blastPath" + random.nextInt();
    Properties properties = new Properties();
    properties.setProperty(BlastConfig.FIELD_BLAST_PATH, blastPath);

    BlastConfig config = new BlastConfig(properties);
    BlastConfig defaultConfig = new BlastConfig(this.properties);

    Assert.assertEquals(blastPath, config.getBlastPath());
    Assert.assertEquals(defaultConfig.getTempDir().getAbsolutePath(),
        config.getTempDir().getAbsolutePath());
    Assert.assertEquals(defaultConfig.getTimeout(), config.getTimeout());
    Assert.assertEquals(defaultConfig.getExtraOptions(),
        config.getExtraOptions());
    Assert.assertEquals(defaultConfig.getSourceIdRegex(),
        config.getSourceIdRegex());
    Assert.assertEquals(defaultConfig.getOrganismRegex(),
        config.getOrganismRegex());
  }

  @Test
  public void testFormatNcbiResultsWithHits() throws URISyntaxException,
      WsfPluginException, IOException {
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
  public void testFormatNcbiResultsWithoutHits() throws
      WsfPluginException, URISyntaxException, IOException {
    StringBuffer message = new StringBuffer();
    String[][] results = format(new NcbiBlastResultFormatter(),
        "ncbi-blast-no-hits.out", message);

    Assert.assertEquals(0, results.length);
    Assert.assertTrue(message.length() > 100);
  }

  @Test
  public void testFormatNcbiResultsWithError() throws 
      WsfPluginException, URISyntaxException, IOException {
    StringBuffer message = new StringBuffer();
    String[][] results = format(new NcbiBlastResultFormatter(),
        "ncbi-blast-err.out", message);

    Assert.assertEquals(0, results.length);
    Assert.assertTrue(message.length() > 100);
  }

  // @Test
  // public void testFormatWuResultsWithHits() throws URISyntaxException,
  // WsfServiceException, WdkModelException, WdkUserException, IOException,
  // SQLException {
  // StringBuffer message = new StringBuffer();
  // String[][] results = format(new WuBlastResultFormatter(),
  // "wu-blast-hits.out", message);
  //
  // Assert.assertEquals(5, results.length);
  // Assert.assertEquals(columns.length, results[0].length);
  // Assert.assertTrue(results[0][2] == null);
  // Assert.assertTrue(results[0][3].length() > 100);
  // Assert.assertTrue(results[4][2].length() > 100);
  // Assert.assertTrue(results[4][3] == null);
  // Assert.assertTrue(message.length() == 0);
  // }
  //
  // @Test
  // public void testFormatWuResultsWithoutHits() throws WdkModelException,
  // WdkUserException, WsfServiceException, URISyntaxException, IOException,
  // SQLException {
  // StringBuffer message = new StringBuffer();
  // String[][] results = format(new WuBlastResultFormatter(),
  // "wu-blast-no-hit.out", message);
  //
  // Assert.assertEquals(0, results.length);
  // Assert.assertTrue(message.length() > 100);
  // }
  //
  // @Test
  // public void testFormatWuResultsWithError() throws WdkModelException,
  // WdkUserException, WsfServiceException, URISyntaxException, IOException,
  // SQLException {
  // StringBuffer message = new StringBuffer();
  // String[][] results = format(new WuBlastResultFormatter(),
  // "wu-blast-err.out", message);
  //
  // Assert.assertEquals(0, results.length);
  // Assert.assertTrue(message.length() > 100);
  // }

  private String[][] format(ResultFormatter formatter, String fileName,
      StringBuffer message) throws WsfPluginException, URISyntaxException, IOException {
    BlastConfig config = new BlastConfig(properties);
    ProjectMapper projectMapper = new MockProjectMapper();
    formatter.setConfig(config);
    formatter.setProjectMapper(projectMapper);

    // get blast output file
    URL url = this.getClass().getResource("/blast/" + fileName);
    File resultFile = new File(url.toURI());
    String dbType = "Genomics";
    String recordClass = "GeneRecordClasses.GeneRecordClass";

    File storageDir = File.createTempFile("temp/wsf", null);
    storageDir.mkdirs();
    PluginResponse response = new PluginResponse(storageDir, 0);
    String msg = formatter.formatResult(response, columns, resultFile,
        recordClass, dbType);
    message.append(msg);
    return response.getPage(0);
  }
}
