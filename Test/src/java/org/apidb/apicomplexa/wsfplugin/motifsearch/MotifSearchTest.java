package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apidb.apicomplexa.wsfplugin.MockProjectMapper;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wsf.plugin.Plugin;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.gusdb.wsf.plugin.WsfServiceException;
import org.gusdb.wsf.util.Formatter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MotifSearchTest {

  @SuppressWarnings("unused")
  private static final Logger logger = Logger.getLogger(MotifSearchTest.class.getName());

  private static final String SYS_PROJECT_HOME = "project_home";

  private final String gusHome;
  private final String projectHome;

  private Properties properties;

  public MotifSearchTest() throws Exception {
    projectHome = System.getProperty(SYS_PROJECT_HOME);
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

    // read the sample config file with default values
    String sampleFile = projectHome
        + "/ApiCommonWebService/WSFPlugin/config/motifSearch-config.xml.sample";
    InputStream inStream = new FileInputStream(sampleFile);
    properties.loadFromXML(inStream);
    inStream.close();
  }

  @Test
  public void testDnaHeadlineRegex() {
    String regex = properties.getProperty(DnaMotifPlugin.FIELD_REGEX);
    String content = ">gb|scf_1107000998814 | strand=(+) | organism=Toxoplasma_gondii_GT1 | version=2008-07-23 | length=1231";

    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(content);
    Assert.assertTrue(matcher.find());
    Assert.assertEquals(3, matcher.groupCount());
    Assert.assertEquals("scf_1107000998814", matcher.group(1));
    Assert.assertEquals("+", matcher.group(2));
    Assert.assertEquals("Toxoplasma", matcher.group(3));
  }

  @Test
  public void testProteinHeadlineRegex() {
    String regex = properties.getProperty(ProteinMotifPlugin.FIELD_REGEX);
    String content = ">psu|NCLIV_009530 | organism=Neospora_caninum | product=hypothetical protein, conserved | location=NCLIV_chrIV:42585-46508(+) | length=1307";

    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(content);
    Assert.assertTrue(matcher.find());
    Assert.assertEquals(2, matcher.groupCount());
    Assert.assertEquals("NCLIV_009530", matcher.group(1));
    Assert.assertEquals("Neospora", matcher.group(2));
  }

  @Test
  public void testDnaMotifSearch() throws WsfServiceException,
      URISyntaxException, IOException {
    AbstractMotifPlugin search = new DnaMotifPlugin();
    try {
      search.initialize(getContext());
    } catch (Exception ex) {
      // ignore the exception
    }
    search.setProjectMapper(new MockProjectMapper());

    // prepare parameters
    Map<String, String> params = new HashMap<String, String>();
    params.put(AbstractMotifPlugin.PARAM_EXPRESSION, "GGATCC");
    params.put(AbstractMotifPlugin.PARAM_DATASET,
        getPath("/fasta/sample-dna.fasta"));

    // invoke the plugin and get result back
    PluginRequest request = getRequest(params);
    PluginResponse response = getResponse();
    search.execute(request, response);

    // print results
    String[][] results = response.getPage(0);
    System.out.println(Formatter.printArray(results));

    Assert.assertEquals(2, results.length);
  }

  @Test
  public void testProteinMotifSearch() throws WsfServiceException,
      URISyntaxException, IOException {
    AbstractMotifPlugin search = new ProteinMotifPlugin();

    // ignore the exceptions here, use a mock project mapper
    try {
      search.initialize(getContext());
    } catch (Exception ex) {
      // ignore the exception
    }
    search.setProjectMapper(new MockProjectMapper());

    // prepare parameters
    Map<String, String> params = new HashMap<String, String>();
    params.put(AbstractMotifPlugin.PARAM_EXPRESSION, "0[6]{2,8}G");
    params.put(AbstractMotifPlugin.PARAM_DATASET,
        getPath("/fasta/sample-protein.fasta"));

    // invoke the plugin and get result back
    PluginRequest request = getRequest(params);
    PluginResponse response = getResponse();
    search.execute(request, response);

    // print results
    String[][] results = response.getPage(0);

    // print results
    System.out.println(Formatter.printArray(results));

    Assert.assertEquals(3, results.length);
  }

  private Map<String, Object> getContext() {
    Map<String, Object> context = new HashMap<String, Object>();
    context.put(Plugin.CTX_CONFIG_PATH, gusHome + "/config/");
    return context;
  }

  private PluginRequest getRequest(Map<String, String> params) {
    // prepare columns
    String[] columns = new String[] { AbstractMotifPlugin.COLUMN_SOURCE_ID,
        AbstractMotifPlugin.COLUMN_PROJECT_ID,
        AbstractMotifPlugin.COLUMN_MATCH_COUNT,
        AbstractMotifPlugin.COLUMN_LOCATIONS,
        AbstractMotifPlugin.COLUMN_SEQUENCE };

    PluginRequest request = new PluginRequest();
    request.setParams(params);
    request.setOrderedColumns(columns);
    request.setContext(new HashMap<String, String>());

    return request;
  }

  private String getPath(String resourceName) throws URISyntaxException {
    URL url = getClass().getResource(resourceName);
    File file = new File(url.toURI());
    return file.getAbsolutePath();
  }

  private PluginResponse getResponse() throws IOException {
    File storageDir = File.createTempFile("temp/wsf", null);
    storageDir.mkdirs();
    PluginResponse response = new PluginResponse(storageDir, 0);
    return response;
  }
}
