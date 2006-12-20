/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.wdkquery;

import java.io.*;
import java.lang.StringBuffer;
import java.net.MalformedURLException;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;
import java.util.HashMap;
import java.util.Map;

import org.gusdb.wdk.model.*;
import org.gusdb.wdk.model.implementation.ModelXmlParser;
import org.gusdb.wdk.model.AttributeFieldValue;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.jspwrap.*;
import org.gusdb.wdk.model.jspwrap.QuestionBean;
import org.gusdb.wdk.model.jspwrap.WdkModelBean;
import org.gusdb.wdk.model.implementation.SqlQuery;
import org.gusdb.wdk.model.implementation.SqlQueryInstance;
import org.gusdb.wdk.model.implementation.SqlResultList;
import org.gusdb.wdk.model.implementation.WSQuery;
import org.gusdb.wdk.model.implementation.WSQueryInstance;
import org.gusdb.wdk.model.implementation.WSResultList;
import org.gusdb.wdk.model.WdkModel;

import java.net.URL;
import org.gusdb.wdk.model.user.User;
import org.gusdb.wdk.model.user.UserFactory;
import org.gusdb.wdk.model.xml.XmlQuestionSet;
import org.gusdb.wdk.model.xml.XmlRecordClassSet;
import org.gusdb.wdk.model.implementation.*;
import org.w3c.dom.Document;

import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author Cary Pennington
 * @created Dec 20, 2006
 */
public class WdkQueryPlugin extends WsfPlugin {
    
    //Input Parameters
    public static final String PARAM_PARAMETERS = "Parameters";
    public static final String PARAM_COLUMNS = "Columns";
    
    //Output Parameters
    public static final String COLUMN_RETURN = "Response";
    
    
    private WdkModelBean model         = null;
    
    private static File          m_modelFile     = null;
    private static File          m_modelPropFile = null;
    private static File          m_schemaFile    = null;
    private static File          m_configFile    = null;
    
    private static File          m_xmlSchemaFile    = null;

    private static String modelName;
       
    public WdkQueryPlugin() throws WsfServiceException {
        super();
	initial();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getRequiredParameters()
     */
    @Override
    protected String[] getRequiredParameterNames() {
        return new String[] { };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getColumns()
     */
    @Override
    protected String[] getColumns() {
        return new String[] { COLUMN_RETURN };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#validateParameters(java.util.Map)
     */
    @Override
    protected void validateParameters(Map<String, String> params)
            throws WsfServiceException {
        // do nothing in this plugin
    }
    
    
    

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
     */
    @Override
    protected String[][] execute(String invokeKey, Map<String, String> params,
            String[] orderedColumns) throws WsfServiceException     
    {
        logger.info("Invoking WdkQueryPlugin...");
	String[][] componentResults = null;
	ResultList results = null;
	Map<String,Object>SOParams = convertParams(params);
	logger.info("Parameters were processed");
	try {
	    //Reset the QueryName for testing reasons
	    invokeKey = "GeneFeatureIds.GeneByLocusTag";
            
	    invokeKey = invokeKey.replace('.',':');
	    logger.info(invokeKey);
	    String[] queryName = invokeKey.split(":");
	    QuerySet qs = model.getModel().getQuerySet(queryName[0]);
	    Query q = qs.getQuery(queryName[1]);
	    logger.info("Query found " + q.getFullName());
	    
	    // WS Query processing
	    if(q instanceof WSQuery) {
	    	logger.info("Processing WSQuery ...");
		WSQuery wsquery = (WSQuery) q;
		WSQueryInstance wsqi = (WSQueryInstance)wsquery.makeInstance();
		wsqi.setValues(SOParams);
		ResultFactory resultFactory = wsquery.getResultFactory();
		results = resultFactory.getResult(wsqi);
	    }
	    //SQL Query Processing
	    else {
		logger.info("Process SqlQuery ...");
	    	SqlQuery sqlquery = (SqlQuery) q;
		SqlQueryInstance sqlqi = (SqlQueryInstance)sqlquery.makeInstance();
		sqlqi.setValues(SOParams);
		ResultFactory resultFactory = sqlquery.getResultFactory();
		results = resultFactory.getResult(sqlqi);
		
	    }
	    logger.info("Results set was filled");
	    componentResults = results2StringArray(results);
	    

            logger.info("Results have been processed ...");
	    } catch(Exception ex){
		logger.debug("ERROR IN execute(): ", ex);
	    }
	String[][] responseT = null;    
	if(componentResults == null) {
	    responseT = new String[1][1];
	    responseT[0][0] = "There was an Error";
	}else {
	    responseT = new String[componentResults.length][orderedColumns.length];
	    for(int i = 0; i < componentResults.length; i++){
	    	for(int j = 0; j < orderedColumns.length; j++){
	    	     responseT[i][j] = componentResults[i][j];
	    	}
	    }
	}      
	return responseT;
    }
       
    private Map<String,Object> convertParams(String[] p){
	Map<String,Object> ret = new HashMap<String,Object>();
	for (String param:p){
	    String[] pa = param.split("=");
	    ret.put(pa[0], (Object)pa[1]);
	}
	return ret;
    }
    
     private Map<String,Object> convertParams(Map<String,String> p){
    	Map<String,Object> ret = new HashMap<String,Object>();
	for (String key:p.keySet()){
		Object o = p.get(key);
		ret.put(key, o);
	}
	return ret;
	}
   
    private String results2String(ResultList result)throws WdkModelException{

	StringBuffer sb = new StringBuffer();
	result.write(sb);
	return sb.toString();

    }

    private String[][] results2StringArray(ResultList result)throws WdkModelException
    {
	int r = 0;
	int c = 0;
	StringBuffer sb = new StringBuffer();
	result.write(sb);
	String s =  sb.toString();
	logger.info(s);
	String nl = System.getProperty("line.separator");
	String[] rows = s.split(nl);
	String[][] ans = new String[rows.length][];
	for(String row:rows){
	    ans[r] = row.split("\t"); 
	    r++;
	}
	return ans;
    }


    private static void loadConfig(String mName)throws IOException {
	
        //model Name and path for xml files will be read from config file
        String modelName = "";
        String GUS_HOME = "";
	
        //config file where to retrieve above info
        String path = "/usr/local/apache-tomcat-5.5.15/webapps/axis/WEB-INF/wsf-config/";
        String fileprop = path + "wdkqueryplugin.prop";
	
        BufferedReader in = new BufferedReader(new FileReader(fileprop));
        while ( modelName.compareTo(mName) != 0 ) {
            modelName = in.readLine();
            GUS_HOME = in.readLine();
            if (  modelName.compareTo("END") == 0 ) break;
        }
        in.close();
	
	
        m_modelFile = new File(
			       GUS_HOME+"/config/"+modelName+".xml");
        m_modelPropFile = new File(
				   GUS_HOME+"/config/"+modelName+".prop");
        m_configFile = new File(
				GUS_HOME+"/config/"+modelName+"-config.xml");
        m_schemaFile = new File(
				GUS_HOME+"/lib/rng/wdkModel.rng");
        //added Jun26,2006
        m_xmlSchemaFile = new File(
				   GUS_HOME+"/lib/rng/xmlAnswer.rng");
	
	
    }//end loadConfig
    
    
    private WdkModelBean loadModel()
        throws MalformedURLException, WdkModelException {	
        WdkModel wdkModel = ModelXmlParser.parseXmlFile(
			m_modelFile.toURL(), m_modelPropFile.toURL(), m_schemaFile.toURL(), 
			m_xmlSchemaFile.toURL(), m_configFile.toURL());
        if(wdkModel != null ) logger.info(wdkModel.getName());
	WdkModelBean model = new WdkModelBean(wdkModel);
        return model;
	
    }//end of loadmodel
    
    private void initial() {
	if (model == null) {
	    try {
		loadConfig(modelName);		
		model = loadModel();
	    } catch (Exception ex) {
		logger.debug("ERROR :", ex);
	    }
	}
    }
    

}
