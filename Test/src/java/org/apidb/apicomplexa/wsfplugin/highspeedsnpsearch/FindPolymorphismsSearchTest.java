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

public class FindPolymorphismsSearchTest {

  @SuppressWarnings("unused")
  private static final Logger logger = Logger.getLogger(FindPolymorphismsSearchTest.class.getName());

  private static final String SYS_PROJECT_HOME = "PROJECT_HOME";

  private final String gusHome;
  private final String projectHome;

  private Properties properties;

  public FindPolymorphismsSearchTest() throws Exception {
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
    String sampleFile = projectHome
        + "/ApiCommonWebService/WSFPlugin/config/highSpeedSnpSearch-config.xml.sample";
    InputStream inStream = new FileInputStream(sampleFile);
    properties.loadFromXML(inStream);
    inStream.close();
    properties.setProperty(FindPolymorphismsPlugin.PROPERTY_JOBS_DIR, "/tmp");
    properties.setProperty(FindPolymorphismsPlugin.PROPERTY_DATA_DIR, projectHome + "/ApiCommonWebServices/HighSpeedSnpSearch/test");

  }

  @Test
  public void testReadConfigFile() throws InvalidPropertiesFormatException,
      FileNotFoundException, IOException {

    // prepare the config file
    Properties properties = new Properties();

    // read the sample config file with default values
    String sampleFile = projectHome
        + "/ApiCommonWebService/WSFPlugin/config/highSpeedSnpSearch-config.xml.sample";
    InputStream inStream = new FileInputStream(sampleFile);
    properties.loadFromXML(inStream);
    inStream.close();
    String jobDir = properties.getProperty(FindPolymorphismsPlugin.PROPERTY_JOBS_DIR);
    String dataDir = properties.getProperty(FindPolymorphismsPlugin.PROPERTY_DATA_DIR);
    Assert.assertTrue(jobDir != null);
    Assert.assertTrue(dataDir != null);
  }

  @Test
  public void testSearch() throws WsfPluginException {
    FindPolymorphismsPlugin search = new FindPolymorphismsPlugin();
    System.err.println("first" + properties);
    try {
      System.err.println("calling init");
      search.initialize(getContext());
      System.err.println("done calling init");

    } catch (NullPointerException ex) {
      // ignore missing wdk model
      //      throw ex;
    }
    search.setProjectMapper(new MockProjectMapper());

    // prepare parameters
    Map<String, String> params = new HashMap<String, String>();
    params.put(FindPolymorphismsPlugin.PARAM_ORGANISM, "Hippo");
    params.put(FindPolymorphismsPlugin.PARAM_READ_FREQ_PERCENT, "80");
    params.put(FindPolymorphismsPlugin.PARAM_STRAIN_LIST, "1,2,3,4");
    params.put(FindPolymorphismsPlugin.PARAM_MIN_PERCENT_KNOWNS, "20");
    params.put(FindPolymorphismsPlugin.PARAM_MIN_PERCENT_POLYMORPHISMS, "20");

    // invoke the plugin and get result back
    PluginRequest request = getRequest(params);
    PluginResponse response = getResponse();
    search.execute(request, response);

    // print results
    String[][] results = response.getPage(0);
    System.out.println(Formatter.printArray(results));

    Assert.assertEquals(5, results.length);
  }

  private Map<String, Object> getContext() {
    Map<String, Object> context = new HashMap<String, Object>();
    context.put(Plugin.CTX_CONFIG_PATH, gusHome + "/config/");
    return context;
  }

  private PluginRequest getRequest(Map<String, String> params) {
    // prepare columns
    String[] columns = new String[] { FindPolymorphismsPlugin.COLUMN_SNP_SOURCE_ID,
        FindPolymorphismsPlugin.COLUMN_PROJECT_ID,
        FindPolymorphismsPlugin.COLUMN_PERCENT_OF_POLYMORPHISMS,
        FindPolymorphismsPlugin.COLUMN_PERCENT_OF_KNOWNS,
        FindPolymorphismsPlugin.COLUMN_IS_NONSYNONYMOUS };

    PluginRequest request = new PluginRequest();
    request.setParams(params);
    request.setOrderedColumns(columns);
    request.setContext(new HashMap<String, String>());

    return request;
  }

  private PluginResponse getResponse() {
    int invokeId = 0;
    File storageDir = new File("/tmp/junk");
    new File(storageDir, Integer.toString(invokeId)).mkdirs();
    PluginResponse response = new PluginResponse(storageDir, invokeId);
    return response;
  }
}
