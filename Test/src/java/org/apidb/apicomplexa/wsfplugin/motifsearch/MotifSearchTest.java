package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wsf.plugin.Plugin;
import org.gusdb.wsf.plugin.WsfRequest;
import org.gusdb.wsf.plugin.WsfResponse;
import org.gusdb.wsf.plugin.WsfServiceException;
import org.gusdb.wsf.util.Formatter;
import org.junit.Assert;
import org.junit.Test;

public class MotifSearchTest {
    
    @SuppressWarnings("unused")
    private static final Logger logger = Logger.getLogger(MotifSearchTest.class.getName());

    private Properties properties;

    public MotifSearchTest() throws InvalidPropertiesFormatException,
            FileNotFoundException, IOException {
        // store original gusHome
        String gusHome = System.getProperty(Utilities.SYSTEM_PROPERTY_GUS_HOME);
        properties = new Properties();
        String fileName = gusHome + "/config/"
                + MotifSearchPlugin.PROPERTY_FILE;
        properties.loadFromXML(new FileInputStream(fileName));

    }

    @Test
    public void testDnaHeadlineRegex() {
        String regex = properties.getProperty(DnaMotifPlugin.FIELD_DNA_DEFLINE_REGEX);
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
        String regex = properties.getProperty(ProteinMotifPlugin.FIELD_PROTEIN_DEFLINE_REGEX);
        String content = ">psu|NCLIV_009530 | organism=Neospora_caninum | product=hypothetical protein, conserved | location=NCLIV_chrIV:42585-46508(+) | length=1307";

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(content);
        Assert.assertTrue(matcher.find());
        Assert.assertEquals(2, matcher.groupCount());
        Assert.assertEquals("NCLIV_009530", matcher.group(1));
        Assert.assertEquals("Neospora", matcher.group(2));
    }

    @Test
    public void testDnaMotifSearch() throws WsfServiceException {
        MotifSearchPlugin search = new DnaMotifPlugin();
        search.initialize(getContext());

        // prepare parameters
        Map<String, String> params = new HashMap<String, String>();
        params.put(MotifSearchPlugin.PARAM_EXPRESSION, "GGATCC");
        params.put(MotifSearchPlugin.PARAM_DATASET,
                "sample-dna.fasta");
        
        // invoke the plugin and get result back
        WsfRequest request = getRequest(params);
        WsfResponse wsfResult = search.execute(request);
        
        // print results
        System.out.println(Formatter.printArray(wsfResult.getResult()));

        Assert.assertEquals(2, wsfResult.getResult().length);
    }

    @Test
    public void testProteinMotifSearch() throws WsfServiceException {
        MotifSearchPlugin search = new ProteinMotifPlugin();
        search.initialize(getContext());

        // prepare parameters
        Map<String, String> params = new HashMap<String, String>();
        params.put(MotifSearchPlugin.PARAM_EXPRESSION, "0[6]{2,8}G");
        params.put(MotifSearchPlugin.PARAM_DATASET,
                "sample-protein.fasta");
        
        // invoke the plugin and get result back
        WsfRequest request = getRequest(params);
        WsfResponse wsfResult = search.execute(request);
        
        // print results
        System.out.println(Formatter.printArray(wsfResult.getResult()));

        Assert.assertEquals(2, wsfResult.getResult().length);
    }
    
    
    private Map<String, Object> getContext() {
        String gusHome = System.getProperty(Utilities.SYSTEM_PROPERTY_GUS_HOME);

        Map<String, Object> context = new HashMap<String, Object>();
        context.put(Plugin.CTX_CONFIG_PATH, gusHome + "/config/");
        return context;
    }
    
    private WsfRequest getRequest(Map<String, String> params) {
        // prepare columns
       String[] columns = new String[] { MotifSearchPlugin.COLUMN_SOURCE_ID,
                MotifSearchPlugin.COLUMN_PROJECT_ID,
                MotifSearchPlugin.COLUMN_MATCH_COUNT,
                MotifSearchPlugin.COLUMN_LOCATIONS,
                MotifSearchPlugin.COLUMN_SEQUENCE };

        WsfRequest request = new WsfRequest();
        request.setParams(params);
        request.setOrderedColumns(columns);
        request.setContext(new HashMap<String, String>());

        return request;
    }
}
