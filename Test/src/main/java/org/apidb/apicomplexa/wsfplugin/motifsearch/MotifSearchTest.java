package org.apidb.apicomplexa.wsfplugin.motifsearch;

import org.apache.log4j.Logger;
import org.apidb.apicomplexa.wsfplugin.MockProjectMapper;
import org.gusdb.fgputil.FormatUtil;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wsf.plugin.Plugin;
import org.gusdb.wsf.plugin.PluginRequest;
import org.gusdb.wsf.plugin.PluginResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MotifSearchTest {

  @SuppressWarnings("unused")
  private static final Logger logger = Logger.getLogger(MotifSearchTest.class.getName());

  private static final String SYS_PROJECT_HOME = "PROJECT_HOME";

  private final String gusHome;
  private final String projectHome;

  private Properties properties;

  public MotifSearchTest() throws Exception {
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

    // read the sample config file with default values
    String sampleFile = projectHome
        + "/ApiCommonWebService/WSFPlugin/config/motifSearch-config.xml.sample";

    try (InputStream inStream = new FileInputStream(sampleFile)) {
      properties.loadFromXML(inStream);
    }

    properties.setProperty(DnaMotifPlugin.FIELD_REGEX, "");
  }

  @Test
  public void testDnaHeadlineRegex() {
    String regex = properties.getProperty(DnaMotifPlugin.FIELD_REGEX);
    String content = ">gb|scf_1107000998814 | strand=(+) | organism=Toxoplasma_gondii_GT1 | version=2008-07-23 | length=1231";

    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(content);
    Assertions.assertTrue(matcher.find());
    Assertions.assertEquals(3, matcher.groupCount());
    Assertions.assertEquals("scf_1107000998814", matcher.group(1));
    Assertions.assertEquals("+", matcher.group(2));
    Assertions.assertEquals("Toxoplasma", matcher.group(3));
  }

  @Test
  public void testProteinHeadlineRegex() {
    String regex = properties.getProperty(ProteinMotifPlugin.FIELD_REGEX);
    String content = ">psu|NCLIV_009530 | organism=Neospora_caninum | product=hypothetical protein, conserved | location=NCLIV_chrIV:42585-46508(+) | length=1307";

    Pattern pattern = Pattern.compile(regex);
    Matcher matcher = pattern.matcher(content);
    Assertions.assertTrue(matcher.find());
    Assertions.assertEquals(2, matcher.groupCount());
    Assertions.assertEquals("NCLIV_009530", matcher.group(1));
    Assertions.assertEquals("Neospora", matcher.group(2));
  }

  @Test
  public void testDnaMotifSearch() throws URISyntaxException, IOException {
    AbstractMotifPlugin search = new DnaMotifPlugin();
    try {
      search.initialize(getContext());
    } catch (Exception ex) {
      // ignore the exception
    }
    search.setProjectMapper(new MockProjectMapper());

    // prepare parameters
    var params = new HashMap<String, String>();
    params.put(AbstractMotifPlugin.PARAM_EXPRESSION, "GGATCC");
    params.put(AbstractMotifPlugin.PARAM_DATASET,
        getPath("/fasta/sample-dna.fasta"));

    // invoke the plugin and get result back
    PluginRequest request = getRequest(params);
    PluginResponse response = getResponse();
    search.execute(request, response);

    // print results
    String[][] results = response.getPage(0);
    System.out.println(FormatUtil.printArray(results));

    Assertions.assertEquals(2, results.length);
  }

  @Test
  public void testProteinMotifSearch() throws URISyntaxException, IOException {
    AbstractMotifPlugin search = new ProteinMotifPlugin();

    // ignore the exceptions here, use a mock project mapper
    try {
      search.initialize(getContext());
    } catch (Exception ex) {
      // ignore the exception
    }
    search.setProjectMapper(new MockProjectMapper());

    // prepare parameters
    var params = new HashMap<String, String>();
    params.put(AbstractMotifPlugin.PARAM_EXPRESSION, "0[6]{2,8}G");
    params.put(AbstractMotifPlugin.PARAM_DATASET,
        getPath("/fasta/sample-protein.fasta"));

    // invoke the plugin and get result back
    var request = getRequest(params);
    var response = getResponse();
    search.execute(request, response);

    // print results
    String[][] results = response.getPage(0);

    // print results
    System.out.println(FormatUtil.printArray(results));

    Assertions.assertEquals(3, results.length);
  }

  private Map<String, Object> getContext() {
    var context = new HashMap<String, Object>();
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
    request.setContext(new HashMap<>());

    return request;
  }

  private String getPath(String resourceName) throws URISyntaxException {
    URL url = getClass().getResource(resourceName);
    File file = new File(url.toURI());
    return file.getAbsolutePath();
  }

  private PluginResponse getResponse() throws IOException {
    var storageDir = File.createTempFile("temp/wsf", null);
    //noinspection ResultOfMethodCallIgnored
    storageDir.mkdirs();
    return new PluginResponse(storageDir, 0);
  }
}
