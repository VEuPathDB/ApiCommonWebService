/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.wdkquery;

import java.io.*;
import java.io.File;
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
import org.gusdb.wdk.model.Column;

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
    
    //Propert values
    public static final String PROPERTY_FILE = "wdkquery-config.xml";
    public static final String MODEL_NAME = "ModelName";
    public static final String GUS_HOME = "Gus_Home";
    
    public static final String VERSION = "1.1.0";
    //Input Parameters
    public static final String PARAM_PARAMETERS = "Parameters";
    public static final String PARAM_COLUMNS = "Columns";
    public static final String SITE_MODEL = "SiteModel";

    //Output Parameters
    public static final String COLUMN_RETURN = "Response";
    
    //Member Variables
    private WdkModelBean[] models         = null;
    private static File m_modelFile     = null;
    private static File m_modelPropFile = null;
    private static File m_schemaFile    = null;
    private static File m_configFile    = null;
    private static File m_xmlSchemaFile    = null;
    private static String[] modelNames;
    private static String[] gus_homes;
    private static String siteName;
    private static Map<String,WdkModelBean> modelName2Model = null;
    private static Object lock = new Object();

    public WdkQueryPlugin() throws WsfServiceException {
	super(PROPERTY_FILE);
	String modelName = getProperty(MODEL_NAME);
	modelNames = modelName.split(",");
	//logger.info("------------ModelName = "+modelName+"-----------------");
	String gus_home = getProperty(GUS_HOME);
	gus_homes = gus_home.split(",");
	//logger.info("------------Gus_Home = "+gus_home+"-----------------");
	//modelName2Model = new HashMap<String,WdkModelBean>();
	initial();
	//logger.info("------------Plugin Initialized-----------------");
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
        return new String[] { };
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
    
    @Override
    protected void validateColumns(String[] orderedColumns){
	//Overriding the parent class to do nothing in this plugin
    }

    private void validateQueryParams(Map<String, String> params, Query q) throws WsfServiceException
    {
	logger.info("--------Validating Parameters---------------");
	String[] reqParams= getParamsFromQuery(q);
        for (String param : reqParams) {
            if (!params.containsKey(param)) {
                throw new WsfServiceException(
                        "The required parameter is missing: " + param);
            }
        }
    }


    private void validateQueryColumns(String[] orderedColumns, Query query) throws WsfServiceException 
    {	
	logger.info("------------Validating Columns---------------");
	String[] reqColumns = getColumnsFromQuery(query);
        //Set<String> colSet = new HashSet<String>(orderedColumns.length);
        //for (String col : orderedColumns) {
        //    colSet.add(col);
        //}
	// for (String col : reqColumns) {
        //    if (!colSet.contains(col)) {
        //        throw new WsfServiceException(
        //               "The required column is missing: " + col);
        //    }
        //}
        // cross check
        //colSet.clear();
        Set<String> colSet = new HashSet<String>(reqColumns.length);
        for (String col : reqColumns) {
            colSet.add(col);
        }
        for (String col : orderedColumns) {
            if (!colSet.contains(col)) {
                throw new WsfServiceException("Unknown column: " + col);
            }
        }
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

       	logger.info("WdkQueryPlugin Version : " + this.VERSION);
        //logger.info("Invoking WdkQueryPlugin......");
	String[][] componentResults = null;
	int resultSize = 1;
	ResultList results = null;
	if(params.containsKey("Query")){
		invokeKey = params.get("Query");
		params.remove("Query");
	}
	String siteModel = params.get(SITE_MODEL);
	params.remove(SITE_MODEL);
	WdkModelBean model = modelName2Model.get(siteModel);
	//logger.info("QueryName = "+ invokeKey);

	//Map<String,Object>SOParams = convertParams(params);
	//logger.info("Parameters were processed");

	try {
	    


	    //Reset the QueryName for testing reasons
	    //invokeKey = "GeneFeatureIds.GeneByLocusTag";
            

	    
	    invokeKey = invokeKey.replace('.',':');
	    logger.info(invokeKey);
	    String[] queryName = invokeKey.split(":");
	    QuerySet qs = model.getModel().getQuerySet(queryName[0]);
	    Query q = qs.getQuery(queryName[1]);
	    //logger.info("Query found : " + q.getFullName());
	
	    Map<String,Object> SOParams = convertParams(params,q.getParams());//getParamsFromQuery(q));

	    //validateQueryParams(params,q);
	    //logger.info("Parameters Validated...");
	    validateQueryColumns(orderedColumns,q);
	    //logger.info("Columns Validated...");
	    
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
		//logger.info("Process SqlQuery ...");
	    	SqlQuery sqlquery = (SqlQuery) q;
		SqlQueryInstance sqlqi = (SqlQueryInstance)sqlquery.makeInstance();
		sqlqi.setValues(SOParams);
		ResultFactory resultFactory = sqlquery.getResultFactory();
		results = resultFactory.getResult(sqlqi);
	    }
	    logger.info("Results set was filled");
	    componentResults = results2StringArray(results);
	    logger.info("Results have been processed ...1");
      
	    } catch(WdkModelException ex){
		logger.info("WdkMODELexception in execute()" + ex.toString());
		String msg = ex.toString();
		//if(msg.matches("Invalid value"){}
		if(msg.contains("Invalid value") && msg.contains("parameter")){
		    resultSize = 0;
		}else{
		ex.printStackTrace();
		resultSize = -1;
		}
	    } catch(WdkUserException ex){
		logger.info("WdkUSERexception IN execute()" + ex.toString());
		ex.printStackTrace();
		resultSize = -2;
            } catch(Exception ex){
		logger.info("OTHERexception IN execute()" + ex.toString());
		String msg = ex.toString();
		if(msg.contains("String index out of range")){
		    resultSize = 0;
		}else{
		    ex.printStackTrace();
		    resultSize = -1;
		}
            }
	String[][] responseT = null;    
	if(componentResults == null) {
	    // logger.info("Component Results = null!!!");
	    responseT = new String[1][1];
	    responseT[0][0] = "ERROR";
	    if(resultSize > 0)
		resultSize = 0;
	}else {
	    // logger.info("Comp-Result not null... getting proper columns");
	    
	    responseT = new String[componentResults.length][orderedColumns.length];
	    for(int i = 0; i < componentResults.length; i++){
	    	for(int j = 0; j < orderedColumns.length; j++){
	    	     responseT[i][j] = componentResults[i][j];
	    	}
	    }
	    //logger.info("FINAL RESULT CALCULATED");
	}
	if(resultSize > 0)
	    resultSize = componentResults.length;
	message = String.valueOf(resultSize);
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
   
    /**   private Map<String,Object> convertParams(Map<String,String> p, String[] q)
	  {
	Map<String,Object> ret = new HashMap<String,Object>();
	for (String key:p.keySet()){
		Object o = p.get(key);
		for (String param : q) {
		    if (key.equals(param) || key.indexOf(param) != -1) {
			ret.put(param, o);
		    }
		}
	}
	return ret;
    }*/

    private Map<String,Object> convertParams(Map<String,String> p, Param[] q){
	Map<String,Object> ret = new HashMap<String,Object>();
	for (String key:p.keySet()){
		Object o = p.get(key);
		for (Param param : q) {
		    if (key.equals(param.getName()) || key.indexOf(param.getName()) != -1) {
			if(param instanceof DatasetParam){
			    String sig = (String)p.get("signature");
			    String compId = sig+":"+o.toString();
			    o = compId;
			    logger.info("full input ======== "+compId);
			    ret.put(param.getName(),o);
			}
			else if(param instanceof AbstractEnumParam){
			    String valList = (String)o;
			    String[] vals = valList.split(",");
			    String newVals = "";
			    for(String mystring : vals){
				try{
				    logger.info("ParamName = " + param.getName() + " ------ Value = " + mystring);
				if(validateSingleValues((AbstractEnumParam)param,mystring)){
				    //ret.put(param.getName(), o);
				    newVals = newVals + "," + mystring;
				    logger.info("validated-------------\n ParamName = " + param.getName() + " ------ Value = " + mystring);
				}
				}catch(WdkModelException e){
				    logger.info(e);
				}
			    }
			    if(newVals.length() != 0) newVals = newVals.substring(1);
			    logger.info("validated values string -------------" + newVals);
			    ret.put(param.getName(), (Object)newVals);
			}else{
			    ret.put(param.getName(), o);
			}
		    }
		}
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
	Column[] cols = result.getColumns();
	List<String[]> rows = new LinkedList<String[]>();
	while(result.next()){
	    String[] values = new String[cols.length];
	    for(int z = 0; z < cols.length; z++){
		Object obj = result.getValueFromResult(cols[z].getName());
		String val = null;
		if(obj instanceof String) 
		    val = (String)obj;
		else if(obj instanceof char[]) 
		    val = new String((char[]) obj);
		else if(obj instanceof byte[]) 
		    val = new String((byte[]) obj);
		else val = obj.toString();
		values[z] = val;
	    }
	    rows.add(values);
	}
	String[][] arr = new String[rows.size()][];
	return rows.toArray(arr);
    }

    private String[] getColumnsFromQuery(Query q)
    {
	Column[] qcols = q.getColumns();
	String [] ret = new String[qcols.length];
	int i = 0;
	for(Column c:qcols){
	    ret[i] = c.getName();
	    i++;
	}
	return ret;
    }

    private String[] getParamsFromQuery(Query q)
    {
	Param[] qp = q.getParams();
	String [] ret = new String[qp.length];
	int i = 0;
	for(Param p:qp){
	    ret[i] = p.getName();
	    i++;
	}
	return ret;
    }
    private static void loadConfig(String mName, String GH)throws IOException {
	
        //model Name and path for xml files will be read from config file
        String modelName = mName;
        String GUS_HOME = GH;
	
        //config file where to retrieve above info
        //String path = "/usr/local/apache-tomcat-5.5.15/webapps/axis/WEB-INF/wsf-config/";
        //String fileprop = path + "wdkqueryplugin.prop";
	
        //BufferedReader in = new BufferedReader(new FileReader(fileprop));
        //while ( modelName.compareTo(mName) != 0 ) {
        //    modelName = in.readLine();
        //    GUS_HOME = in.readLine();
        //    if (  modelName.compareTo("END") == 0 ) break;
        //}
        //in.close();
	
	
        m_modelFile = new File(GUS_HOME+"/config/"+modelName+".xml");
        m_modelPropFile = new File(GUS_HOME+"/config/"+modelName+".prop");
        m_configFile = new File(GUS_HOME+"/config/"+modelName+"-config.xml");
        m_schemaFile = new File(GUS_HOME+"/lib/rng/wdkModel.rng");
        //added Jun26,2006
        m_xmlSchemaFile = new File(GUS_HOME+"/lib/rng/xmlAnswer.rng");
	
    }//end loadConfig
    
    
    private WdkModelBean loadModel()
        { //throws MalformedURLException, WdkModelException {	
	    //logger.info("_______________________________________________________________________");
	    WdkModel wdkModel = null;
	    //logger.info("_______________________________________________________________________");
	    try{
		//CheckFiles();
	    wdkModel = ModelXmlParser.parseXmlFile(
	    m_modelFile.toURL(), m_modelPropFile.toURL(), m_schemaFile.toURL(), 
	    m_xmlSchemaFile.toURL(), m_configFile.toURL());
	    }catch(WdkModelException e){logger.info("ERROR  ERROR : -------" + e.toString());}
	     catch(MalformedURLException e){logger.info("ERROR  ERROR : -------" + e.toString());}
	    //logger.info("_______________________________________________________________________");
        if(wdkModel != null ) logger.info("Model is not Null!!! it is " + wdkModel.getName());
	WdkModelBean model = new WdkModelBean(wdkModel);
        //logger.info("---------Model Loading Completed-----------");
	return model;
	
    }//end of loadmodel
    
    private void initial() {
	synchronized(lock){ if (modelName2Model == null) {
	    modelName2Model = new HashMap<String,WdkModelBean>();
	    int i = 0;
	    for(String modelName:modelNames){ //Start the Model FileName Loop
		logger.info("===================ModelName = " + modelName );
	    try {
		//logger.info("------------intial---------------");
		logger.info("===================GUS_HOME = " + gus_homes[i] );
		loadConfig(modelName, gus_homes[i]);		
		//logger.info("------------Config Loaded---------------");
		logger.info(m_modelFile.toURL().toString()+"\n"+m_modelPropFile.toURL().toString()+"\n"+m_configFile.toURL().toString()+"\n"+m_schemaFile.toURL().toString()+"\n"+m_xmlSchemaFile.toURL().toString());
		WdkModelBean mb = loadModel();
		logger.info("===================Model Loaded Was  " + mb.getModel().getName());
		modelName2Model.put(modelName,mb);
		//logger.info("------------Model Loaded----------------");
	    } catch (Exception ex) {
		logger.info("ERROR : "+ex.toString());
	    }
	    i++;
	    }//End the Model FileName Loop

	}}
    }
    
    private void CheckFiles()
    {
	logger.info("-----------------Checking the Files fro Read Permissions---------------");
	if(!m_modelFile.canRead()){
	    logger.info(m_modelFile + " Cannot be Read!!!");
	}
	if(!m_modelPropFile.canRead()){
	    logger.info(m_modelPropFile + " Cannot be Read!!!");
	}
	if(!m_configFile.canRead()){
	    logger.info(m_configFile + " Cannot be Read!!!");
	}
	if(!m_schemaFile.canRead()){
	    logger.info(m_schemaFile + " Cannot be Read!!!");
	}
	if(!m_xmlSchemaFile.canRead()){
	    logger.info(m_xmlSchemaFile + " Cannot be Read!!!");
	}
	logger.info("------------DONE-------------");
    }
    
    private boolean validateSingleValues (AbstractEnumParam p, String value) throws WdkModelException
    {
	String[] conVocab = p.getVocab();
	// initVocabMap();

	for(String v : conVocab){
	    if(value.equals(v))
		return true;
	}
	return false;
    }
}
