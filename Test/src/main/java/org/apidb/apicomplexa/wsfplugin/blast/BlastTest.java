package org.apidb.apicomplexa.wsfplugin.blast;

import org.apidb.apicomplexa.wsfplugin.MockProjectMapper;
import org.eupathdb.websvccommon.wsfplugin.blast.AbstractBlastPlugin;
import org.eupathdb.websvccommon.wsfplugin.blast.BlastConfig;
import org.eupathdb.websvccommon.wsfplugin.blast.NcbiBlastResultFormatter;
import org.eupathdb.websvccommon.wsfplugin.blast.ResultFormatter;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.Random;

public class BlastTest {

  private final String[] columns = { AbstractBlastPlugin.COLUMN_IDENTIFIER,
      AbstractBlastPlugin.COLUMN_PROJECT_ID,
      AbstractBlastPlugin.COLUMN_SUMMARY, AbstractBlastPlugin.COLUMN_ALIGNMENT,
      AbstractBlastPlugin.COLUMN_EVALUE_MANT,
      AbstractBlastPlugin.COLUMN_EVALUE_EXP, AbstractBlastPlugin.COLUMN_SCORE };

  private static final String SYS_PROJECT_HOME = "project_home";

  private final String projectHome;

  private Properties _properties;

  public BlastTest() throws Exception {
    projectHome = System.getProperty(SYS_PROJECT_HOME);
    if (projectHome == null)
      throw new Exception("Required system environment variable not set: "
          + SYS_PROJECT_HOME);
  }

  @BeforeEach
  public void prepareConfigFile() throws IOException {

    // prepare the config file
    _properties = new Properties();

    // read the sample config file with default values
    String sampleFile = projectHome
        + "/ApiCommonWebService/WSFPlugin/config/blast-config.xml.sample";
    InputStream inStream = new FileInputStream(sampleFile);
    _properties.loadFromXML(inStream);
    inStream.close();
  }

  /**
   * test if the all the fields are populated correctly from the config file;
   */
  @Test
  public void testLoadAllFields() throws Exception {
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

    Assertions.assertEquals(blastPath, config.getBlastPath());
    Assertions.assertEquals(tempPath, config.getTempDir().getName());
    Assertions.assertEquals(timeout, config.getTimeout());
    Assertions.assertEquals(extraOptions, config.getExtraOptions());
    Assertions.assertEquals(sourceIdRegex, config.getSourceIdRegex());
    Assertions.assertEquals(organismRegex, config.getOrganismRegex());

    // make sure to drop the temp path
    File tempDir = new File(tempPath);
    if (tempDir.exists())
      //noinspection ResultOfMethodCallIgnored
      tempDir.delete();
  }

  /**
   * test if the defaults are used if an optional field is not specified.
   */
  @Test
  public void testUseDefaultFields() throws Exception {
    // populate all fields with random values
    Random random = new Random();

    String blastPath = "blastPath" + random.nextInt();
    Properties properties = new Properties();
    properties.setProperty(BlastConfig.FIELD_BLAST_PATH, blastPath);

    BlastConfig config = new BlastConfig(properties);
    BlastConfig defaultConfig = new BlastConfig(_properties);

    Assertions.assertEquals(blastPath, config.getBlastPath());
    Assertions.assertEquals(defaultConfig.getTempDir().getAbsolutePath(),
        config.getTempDir().getAbsolutePath());
    Assertions.assertEquals(defaultConfig.getTimeout(), config.getTimeout());
    Assertions.assertEquals(defaultConfig.getExtraOptions(),
        config.getExtraOptions());
    Assertions.assertEquals(defaultConfig.getSourceIdRegex(),
        config.getSourceIdRegex());
    Assertions.assertEquals(defaultConfig.getOrganismRegex(),
        config.getOrganismRegex());
  }

  @Test
  public void testFormatNcbiResultsWithHits() throws URISyntaxException, IOException {
    StringBuffer message = new StringBuffer();
    String[][] results = format(new NcbiBlastResultFormatter(),
        "ncbi-blast-hits.out", message);

    Assertions.assertEquals(5, results.length);
    Assertions.assertEquals(columns.length, results[0].length);
    Assertions.assertNull(results[0][2]);
    Assertions.assertTrue(results[0][3].length() > 100);
    Assertions.assertTrue(results[4][2].length() > 100);
    Assertions.assertNull(results[4][3]);
    Assertions.assertEquals(0, message.length());
  }

  @Test
  public void testFormatNcbiResultsWithoutHits() throws URISyntaxException, IOException {
    StringBuffer message = new StringBuffer();
    String[][] results = format(new NcbiBlastResultFormatter(),
        "ncbi-blast-no-hits.out", message);

    Assertions.assertEquals(0, results.length);
    Assertions.assertTrue(message.length() > 100);
  }

  @Test
  public void testFormatNcbiResultsWithError()
  throws URISyntaxException, IOException, PluginModelException {
    var message = new StringBuffer();
    var results = format(new NcbiBlastResultFormatter(), "ncbi-blast-err.out",
      message);

    Assertions.assertEquals(0, results.length);
    Assertions.assertTrue(message.length() > 100);
  }

  private String[][] format(ResultFormatter formatter, String fileName,
      StringBuffer message)
  throws URISyntaxException, IOException, PluginModelException {
    var config = new BlastConfig(_properties);
    var projectMapper = new MockProjectMapper();
    formatter.setConfig(config);
    formatter.setProjectMapper(projectMapper);

    // get blast output file
    var url = this.getClass().getResource("/blast/" + fileName);
    var resultFile = new File(url.toURI());
    var dbType = "Genomics";
    var recordClass = "GeneRecordClasses.GeneRecordClass";

    var storageDir = File.createTempFile("temp/wsf", null);
    //noinspection ResultOfMethodCallIgnored
    storageDir.mkdirs();
    var response = new PluginResponse(storageDir, 0);
    var msg = formatter.formatResult(response, columns, resultFile,
        recordClass, dbType);
    message.append(msg);
    return response.getPage(0);
  }
}
