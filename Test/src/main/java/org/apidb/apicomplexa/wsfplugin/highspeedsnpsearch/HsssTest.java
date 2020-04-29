package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import org.apache.log4j.Logger;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wsf.plugin.Plugin;
import org.gusdb.wsf.plugin.PluginResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Super class for high speed snp search tests
 */
public class HsssTest {

  @SuppressWarnings("unused")
  private static final Logger logger = Logger.getLogger(HsssTest.class.getName());

  private static final String SYS_PROJECT_HOME = "PROJECT_HOME";

  private final String gusHome;
  protected final String projectHome;

  protected Properties properties;

  public HsssTest() throws Exception {
    projectHome = System.getenv(SYS_PROJECT_HOME);
    if (projectHome == null)
      throw new Exception("Required system environment variable not set: "
          + SYS_PROJECT_HOME);

    gusHome = System.getProperty(Utilities.SYSTEM_PROPERTY_GUS_HOME);
    if (gusHome == null)
      throw new Exception("Required system environment variable not set: "
          + Utilities.SYSTEM_PROPERTY_GUS_HOME);
  }

  @BeforeEach
  public void prepareConfigFile() throws IOException {

    // prepare the config file
    properties = new Properties();
    String sampleFile = gusHome
        + "/config/highSpeedSnpSearch-config.xml";
    InputStream inStream = new FileInputStream(sampleFile);
    properties.loadFromXML(inStream);
    inStream.close();
    properties.setProperty(HighSpeedSnpSearchAbstractPlugin.PROPERTY_JOBS_DIR, "/tmp");
  }

  @Test
  public void testReadConfigFile() throws IOException {

    // prepare the config file
    Properties properties = new Properties();

    // read the sample config file with default values
    String sampleFile = gusHome
        + "/config/highSpeedSnpSearch-config.xml";
    InputStream inStream = new FileInputStream(sampleFile);
    properties.loadFromXML(inStream);
    inStream.close();
    String jobDir = properties.getProperty(HighSpeedSnpSearchAbstractPlugin.PROPERTY_JOBS_DIR);
    Assertions.assertNotNull(jobDir);
  }


  protected Map<String, Object> getContext() {
    var context = new HashMap<String, Object>();
    context.put(Plugin.CTX_CONFIG_PATH, gusHome + "/config/");
    return context;
  }


  protected PluginResponse getResponse() {
    int invokeId = 0;
    var storageDir = new File("/tmp/junk");
    //noinspection ResultOfMethodCallIgnored
    new File(storageDir, Integer.toString(invokeId)).mkdirs();
    return new PluginResponse(storageDir, invokeId);
  }
}

