package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apidb.apicomplexa.wsfplugin.MockProjectMapper;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wsf.plugin.Plugin;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.WsfPluginException;
import org.gusdb.wsf.util.Formatter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

  @Before
  public void prepareConfigFile() throws InvalidPropertiesFormatException,
      FileNotFoundException, IOException {

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
  public void testReadConfigFile() throws InvalidPropertiesFormatException,
      FileNotFoundException, IOException {

    // prepare the config file
    Properties properties = new Properties();

    // read the sample config file with default values
    String sampleFile = gusHome
        + "/config/highSpeedSnpSearch-config.xml";
    InputStream inStream = new FileInputStream(sampleFile);
    properties.loadFromXML(inStream);
    inStream.close();
    String jobDir = properties.getProperty(HighSpeedSnpSearchAbstractPlugin.PROPERTY_JOBS_DIR);
    Assert.assertTrue(jobDir != null);
  }


  protected Map<String, Object> getContext() {
    Map<String, Object> context = new HashMap<String, Object>();
    context.put(Plugin.CTX_CONFIG_PATH, gusHome + "/config/");
    return context;
  }


  protected PluginResponse getResponse() {
    int invokeId = 0;
    File storageDir = new File("/tmp/junk");
    new File(storageDir, Integer.toString(invokeId)).mkdirs();
    PluginResponse response = new PluginResponse(storageDir, invokeId);
    return response;
  }
}
 
