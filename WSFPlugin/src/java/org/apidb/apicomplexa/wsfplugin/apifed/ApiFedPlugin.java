/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.apifed;
import java.io.*;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Iterator;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.xpath.XPath;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfServiceException;

import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.RemoteException;
import java.util.LinkedHashMap;

import javax.xml.rpc.ServiceException;
import org.gusdb.wsf.client.WsfResponse;
import org.gusdb.wsf.client.WsfService;
import org.gusdb.wsf.client.WsfServiceServiceLocator;

/**
 * @author Cary Pennington
 * @created Dec 20, 2006
 */
public class ApiFedPlugin extends WsfPlugin {
	
    //Static Property Values
    public static final String PROPERTY_FILE = "apifed-config.xml";
    
    public static final String URL = "Urls";
    public static final String MODELS = "Models";
    
    public static final String CRYPTO_WSF_URL = "CryptoURL";
    public static final String PLASMO_WSF_URL = "PlasmoURL";
    public static final String TOXO_WSF_URL = "ToxoURL";
    public static final String CRYPTO_MODEL = "CryptoModel";
    public static final String PLASMO_MODEL = "PlasmoModel";
    public static final String TOXO_MODEL = "ToxoModel";
    public static final String MAPPING_FILE = "MappingFile";

    public static final String VERSION = "1.0.1";
    //Input Parameters
    public static final String PARAM_PROCESSNAME = "ProcessName";
    public static final String PARAM_PARAMETERS = "Parameters";
    public static final String PARAM_COLUMNS = "Columns";
    public static final String PARAM_ORGANISMS = "organism";
    public static final String PARAM_QUERY = "Query";
    
    //Output Parameters
    public static final String COLUMN_RETURN = "Response";
    
    //Member Variable
    // private String[] urls = null;
    //private String[] models = null;
    
    private String cryptoUrl;
    private String plasmoUrl;
    private String toxoUrl;
    private String cryptoModel;
    private String plasmoModel;
    private String toxoModel;
    private Document mapDoc = null;

    public ApiFedPlugin() throws WsfServiceException {
	super(PROPERTY_FILE);
	
	//String urlList = getProperty(URLS);
	//urls = urlList.split(",");
	//String modelList = getProperty(MODELS);
	//models = modelList.split(",");

	cryptoUrl = getProperty(CRYPTO_WSF_URL);
	plasmoUrl = getProperty(PLASMO_WSF_URL);
	toxoUrl = getProperty(TOXO_WSF_URL);
	cryptoModel = getProperty(CRYPTO_MODEL);
	plasmoModel = getProperty(PLASMO_MODEL);
	toxoModel = getProperty(TOXO_MODEL);
	String mapfile = getMapFilePath().concat(getProperty(MAPPING_FILE));
	logger.info("Mapping File ========== " + mapfile);
	
	mapDoc = createMap(mapfile);
	
    }

    //Mapping Functions

    //private String[] initCalls(String val){
    //	String[] c = new String[models.length];
    //	for(int i=0; i < calls.length; i++){
    //	    calls[i] = val;
    //	}
    //	return c;
    // }

    private String getMapFilePath(){
	 String root = System.getProperty("webservice.home");
        File rootDir;
        if (root == null) {
            // if the webservice.home is not specified, by default, we assume
            // the Axis is installed under ${tomcat_home}/webapps
            //root = System.getProperty("catalina.home");
           root = System.getProperty("catalina.base");
	   root = root+"/webapps/axis/WEB-INF/wsf-config/";
        } else root = root + "WEB-INF/wsf-config/";
    return root;
    }

    private Document createMap(String mapFile){
	
	Document doc = null;
	try{
	    doc = new SAXBuilder().build(new File(mapFile));
	}catch (JDOMException e) {
	    logger.info(e.toString());
	    e.printStackTrace();
	} catch (IOException e) {
	    logger.info(e.toString());
	    e.printStackTrace();
	}
	   
	return doc;
    }

    private String mapQuerySet(String querySet, String project){
	Element querySetMapping = null;
	if(mapDoc == null)
	    logger.info("Error:::::::MapDocument is empty");
	querySetMapping = mapDoc.getRootElement().getChild("QuerySets").getChild(querySet);
	String componentQuerySet = querySetMapping.getAttributeValue(project);

	return componentQuerySet;
    }

    private String mapQuery(String querySet, String queryName, String project){
	String xPath = "/FederationMapping/QuerySets/" + querySet + "/wsQueries/" + queryName;

	Element queryMapping = mapDoc.getRootElement().getChild("QuerySets").getChild(querySet).getChild("wsQueries").getChild(queryName);
	String componentQuery = queryMapping.getAttributeValue(project);

	return componentQuery;
    }
    
    private String mapParam(String querySet, String queryName, String paramName, String project){
	String xPath = "/FederationMapping/QuerySets/" + querySet + "/wsQueries/" + queryName + "/Params/params." + paramName;

	Element paramMapping = mapDoc.getRootElement().getChild("QuerySets").getChild(querySet).getChild("wsQueries").
	    getChild(queryName).getChild("Params").getChild("params."+paramName);
	String componentParam = paramMapping.getAttributeValue(project);

	return componentParam;
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
    protected String[] getColumns() { //logger.info("------------"+col+"---"+col.equals("project_id")+"-----------");
	    
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
	//Do Nothing in this plugin
    }
    
    @Override
    protected void validateColumns(String[] orderedColumns){
	//Overriding the parent class to do nothing in this plugin
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
     */
    @Override
    protected String[][] execute(String queryName, Map<String, String> params,
            String[] orderedColumns) throws WsfServiceException {
	
	logger.info("ApiFedPlugin Version : " + this.VERSION);
	    String[] calls = {"","",""};

	    String orgName = hasOrganism(params);
	    String datasetName = hasDataset(params);
	    if(orgName != null){
		String orgsString = params.get(orgName);
		calls = getRemoteCalls(orgsString);
	    } else if(datasetName != null){	
		String datasetString = params.get(datasetName);
		calls = getRemoteCalls(datasetString);
	    } else {
		calls[0] = "doCrypto";
	        calls[1] = "doPlasmo";
	        calls[2] = "doToxo";
		
	    }

	    String query = "";	    
	    String processName = "org.apidb.apicomplexa.wsfplugin.wdkquery.WdkQueryPlugin";
	    if(params.containsKey(PARAM_QUERY)){
		query = params.get(PARAM_QUERY);
		params.remove(PARAM_QUERY);
		queryName = query;
	    } else{query = queryName;}
	   
	    String[] componentColumns = removeProjectId(orderedColumns);

	    //Objects to hold the results of the threads
	    CompResult cryptoResult = new CompResult();
	    CompResult plasmoResult = new CompResult();
	    CompResult toxoResult = new CompResult();
	    
	    //Objects to hold the status of the Threads
	    Status cryptoThreadStatus = new Status(false);
	    Status plasmoThreadStatus = new Status(false);
	    Status toxoThreadStatus = new Status(false);
	   
	    String apiQueryFullName = queryName.replace('.',':');
	    String[] apiQueryNameArray = apiQueryFullName.split(":");
	    String apiQuerySetName = apiQueryNameArray[0];
	    String apiQueryName = apiQueryNameArray[1];

	    //Call only the needed component sites
	    if(calls[0].equals("")){cryptoThreadStatus.setDone(true);}
	    else {
		
		String compQuerySetName = mapQuerySet(apiQuerySetName, cryptoModel);
		String compQueryName = mapQuery(apiQuerySetName, apiQueryName, cryptoModel);
		String compQueryFullName= "";
		if(compQuerySetName.length()!=0 && compQueryName.length()!=0){ 
		    compQueryFullName = compQuerySetName + "." + compQueryName;
		}else{compQueryFullName = queryName;} 
      	
		Map<String,String> cryptoParams = params;
		
		if(orgName != null){
		    if(orgName.indexOf("pforganism") != -1 || orgName.indexOf("pborganism") != -1)
			cryptoParams.remove(orgName);
		}

		String[] arrayParams = getParams(cryptoParams, calls[0], orgName, datasetName, cryptoModel, apiQuerySetName, apiQueryName);

		Thread cryptoThread = 
	                    new WdkQuery(cryptoUrl, processName, compQueryFullName, arrayParams, componentColumns, cryptoResult, cryptoThreadStatus);
		cryptoThread.start();
	    }
	    if(calls[1].equals("")){plasmoThreadStatus.setDone(true);}
	    else {
		
		String compQuerySetName = mapQuerySet(apiQuerySetName, plasmoModel);
		String compQueryName = mapQuery(apiQuerySetName, apiQueryName, plasmoModel);
		String compQueryFullName= "";
		if(compQuerySetName.length()!=0 && compQueryName.length()!=0){ 
		    compQueryFullName = compQuerySetName + "." + compQueryName;
		}else{compQueryFullName = queryName;} 
	
		Map<String,String> plasmoParams = params;

		if(orgName != null){
		    if(orgName.indexOf("pforganism") != -1 || orgName.indexOf("pborganism") != -1)
			plasmoParams.remove(orgName);
		}

		String[] arrayParams = getParams(plasmoParams, calls[1], orgName, datasetName, plasmoModel, apiQuerySetName, apiQueryName);

		Thread plasmoThread = 
		       new WdkQuery(plasmoUrl, processName, compQueryFullName, arrayParams, componentColumns, plasmoResult, plasmoThreadStatus);
		plasmoThread.start();
	    }
	    if(calls[2].equals("")){toxoThreadStatus.setDone(true);}
	    else {
		
		String compQuerySetName = mapQuerySet(apiQuerySetName, toxoModel);
		String compQueryName = mapQuery(apiQuerySetName, apiQueryName, toxoModel);
		String compQueryFullName= "";
		if(compQuerySetName.length()!=0 && compQueryName.length()!=0){ 
		    compQueryFullName = compQuerySetName + "." + compQueryName;
		}else{compQueryFullName = queryName;} 
		
		Map<String,String> toxoParams = params;
                if(orgName != null) toxoParams.remove(orgName);
	
		String[] arrayParams = getParams(toxoParams, calls[2], orgName, datasetName, toxoModel, apiQuerySetName, apiQueryName);

		Thread toxoThread = 
		    new WdkQuery(toxoUrl, processName, compQueryFullName, arrayParams, componentColumns, toxoResult, toxoThreadStatus);
		toxoThread.start();
	    }
	   
	    while(!(cryptoThreadStatus.getDone() && plasmoThreadStatus.getDone() && toxoThreadStatus.getDone())){
		try{
		  Thread.sleep(1000);
		continue;
		}catch(InterruptedException e){}
	    }
	    

	    String[][] result = null;
            logger.info("Entering Combine Results");
	    result = combineResults(cryptoResult, plasmoResult, toxoResult, orderedColumns);
	    return result; 
    }
    
    private String hasOrganism(Map<String,String> p)
    {
	String orgName = null;
	for(String pName:p.keySet()){
	    if(pName.indexOf("organism")!=-1 || pName.indexOf("Organism")!=-1){
		orgName = pName;
		break;
	    }
	}
	return orgName;
    }

    private String hasDataset(Map<String,String> p)
    {
	String dsName = null;
	for(String pName:p.keySet()){
	    if(pName.indexOf("Dataset")!=-1){
		dsName = pName;
		break;
	    }
	}
	return dsName;
    }
    /************** This Function does not include the Mapping ************************************
    private String[] getParams (Map<String,String> params, String localOrgs, String orgParam, String modelName)
    {
    	String[] arrParams = new String[params.size()+1];
	Iterator it = params.keySet().iterator();
	int i = 0;
	while(it.hasNext()){
		String key = (String)it.next();
		if(key.equals(orgParam)){
		    arrParams[i] = key+"="+localOrgs;
		}else{
		    String val = params.get(key);
		    arrParams[i] = key+"="+val;
		}
		i++;
	}
	arrParams[params.size()] = "SiteModel="+modelName;
	return arrParams;
    }
    ***************************************************************************/

    private String[] getParams (Map<String,String> params, String localOrgs, String orgParam, String datasetParam, String modelName, String querySetName, String queryName)
    {
    	String[] arrParams = new String[params.size()+1];
	logger.info("Params Size is " + params.size() + ".  THE END");
	Iterator it = params.keySet().iterator();
	int i = 0;
	while(it.hasNext()){
		String key = (String)it.next();
		logger.info("Param == "+key+"  Values == "+params.get(key));
		String compKey = mapParam(querySetName, queryName, key,  modelName);
		if(compKey.length()==0)
		    compKey = key;

		if(key.equals(orgParam)){
		    arrParams[i] = compKey+"="+localOrgs;
		}else if(key.equals(datasetParam)){
		    arrParams[i] = compKey+"="+localOrgs;
		}else{
		    String val = params.get(key);
		    arrParams[i] = compKey+"="+val;
		}
		i++;
	}
	arrParams[params.size()] = "SiteModel="+modelName;
	return arrParams;
    }
    
    private String[] getRemoteCalls(String orgs)
    {
	String[] calls = {"","",""};
	String[] orgArray = orgs.split(",");
	for(String organism:orgArray){
	    if(organism.charAt(0)=='C')
		calls[0] = calls[0] + "," + organism;
	    if(organism.charAt(0)=='P')
		calls[1] = calls[1] + "," + organism;
	    if(organism.charAt(0)=='T')
		calls[2] = calls[2] + "," + organism ;
	}
	if(calls[0].length() > 0) calls[0] = calls[0].substring(1);
        if(calls[1].length() > 0) calls[1] = calls[1].substring(1);
	if(calls[2].length() > 0) calls[2] = calls[2].substring(1);
	return calls;
    }

    private String[][] combineResults(CompResult cryptoCR, CompResult plasmoCR, CompResult toxoCR, String[] cols)
     {
	 message = "";
	 	 
	 String[][] crypto = cryptoCR.getAnswer();
 	 String[][] plasmo = plasmoCR.getAnswer();
	 String[][] toxo = toxoCR.getAnswer();
	 logger.info("Answers moved to local Variables");
	 //Determine the length of the new array
	int numrows = 0;
	if(crypto!=null && crypto.length > 0 && !crypto[0][0].equals("ERROR"))numrows = numrows + crypto.length;
        logger.info("Crypto total added .... "+ crypto.length);
	if(plasmo!=null && plasmo.length > 0 && !plasmo[0][0].equals("ERROR"))numrows = numrows + plasmo.length;
        logger.info("plasmo total added .... "+ plasmo.length);
	if(toxo!=null && toxo.length > 0 && !toxo[0][0].equals("ERROR"))numrows = numrows + toxo.length;
        logger.info("toxo total added ..... "+ toxo.length);
	int i = 0;
	String[][] combined = new String[numrows][cols.length + 1]; //add one column for the addition of projectId
	
	logger.info("Total Number of Rows in Combined Result is ----------> " + numrows);
	
	//Find the index for the projectId columns
	int projectIndex = 0;
	for(String col:cols){
	    if(col.equals("project_id")){break;}
	    else{projectIndex++;}
	}

	//Add crypto result, if it is not empty, to the final results
	if(crypto!=null && crypto.length > 0){
	    message = "cryptodb:"+cryptoCR.getMessage();
	    if(!crypto[0][0].equals("ERROR")){
		for(String[] rec:crypto){
		    combined[i] = rec;
		    combined[i] = insertProjectId(combined[i], projectIndex, "cryptodb");
		    i++;
		}
	    }
	}

	//Add plasmo result, if it is not empty, to the final results
	if(plasmo!=null && plasmo.length > 0){
	    if(message.length()!=0){message = message + ",plasmodb:" + plasmoCR.getMessage();}else{message = "plasmodb:"+plasmoCR.getMessage();}
	    if(!plasmo[0][0].equals("ERROR")){
		for(String[] rec:plasmo){
		    combined[i] = rec;
		    combined[i] = insertProjectId(combined[i], projectIndex, "plasmodb");
		    i++;
		}
	    }
	}
	
	//Add toxo result, if it is not empty, to the final results
	if(toxo!=null && toxo.length > 0){
	    if(message.length()!=0){message = message + ",toxodb:" + toxoCR.getMessage();}else{message = "toxodb:"+toxoCR.getMessage();}
	    if(!toxo[0][0].equals("ERROR")){
		for(String[] rec:toxo){
		    combined[i] = rec;
		    combined[i] = insertProjectId(combined[i], projectIndex, "toxodb");
		    i++;
		}
	    }
	}
	return combined;
    }
    
    private void logResults(String[] r, int i){
	//logger.info("-----------------------Results from Thread-----------------------");
	for(String cr:r){
	    logger.info("------- Record :" + i + " " + cr);
	}
    }

    // Method to insert the projectId in the proper place in the return data
    private String[] insertProjectId(String[] rec, int index, String projectId)
    {
	String[] newRec = new String[rec.length+1];
	if(index!=rec.length){
	    String t1 = "";
	    int newi = 0;
	    for(int i = 0; i<rec.length; i++){
		if(i==index){
		    t1 = rec[i];
		    newRec[newi] = projectId;
		    newi++;
		    newRec[newi] = t1;
		}else{
		    newRec[newi] = rec[i];
		}
		newi++;
	    }
	}else{
	    for(int i = 0;i<rec.length;i++){
		newRec[i] = rec[i];
	    }
	    newRec[newRec.length-1] = projectId;
	}
	return newRec;
    }

    // Method to remove the projectId column from the colums sent to the component sites
    private String[] removeProjectId(String[] cols)
    {
	int projectIndex = 0;
	for(String col:cols){
	   if(col.equals("project_id")){break;}
	    else{projectIndex++;}
	}
	int newindex = 0;
	String[] newCols = new String[cols.length - 1];
	for(int i = 0;i<cols.length;i++){
	    if(i==projectIndex){
		continue;
	    }
	    newCols[newindex] = cols[i];
	    newindex++;
	}
	return newCols;
    }

    //Inner Class to do invokations
    class CompResult {
	private String[][] answer;
	private String message;
	public CompResult(){
	    answer = null;
	    message = "";
	}
	public void setAnswer(String[][] answer){
	    this.answer = answer;
	}
	public String[][] getAnswer(){return this.answer;}
	public void setMessage(String message){
	    this.message = message;
	}
	public String getMessage(){return this.message;}
    }
    class Status {
	private boolean done;
	public Status(boolean done){
	    this.done = done;
	}
	public void setDone(boolean done){
	    this.done = done;
	}
	public boolean getDone(){return this.done;}
    }

    class WdkQuery extends Thread
    {
	private String url;
	private String pluginName;
	private String queryName;
	private String[] params;
	private String[] cols;
	private CompResult result;
	private Status status;

	WdkQuery (String URL,String pluginname, String query, String[] p, String[] c, CompResult r, Status s)
	{
	    url = URL;
	    pluginName = pluginname;
	    queryName = query;
	    params = p;
	    cols = c;
	    result = r;
	    status = s;
	}
 
	public void run()
	{
	    logger.info("The Thread is running.................." + url);
	    WsfServiceServiceLocator locator = new WsfServiceServiceLocator();
	           
	try {
            WsfService service = locator.getWsfService(new URL(url));
	    long start = System.currentTimeMillis();
            WsfResponse response = service.invoke(pluginName, queryName, params, cols);
	    long end = System.currentTimeMillis();

            logger.info("Thread (" + url +") has returned results in " + ((end - start) / 1000.0) + " seconds."); 
	    result.setMessage(response.getMessage());
            result.setAnswer(response.getResults());
	    
	    } catch (MalformedURLException ex) {
	    	ex.printStackTrace();
            } catch (ServiceException ex) {
            	ex.printStackTrace();
            } catch (RemoteException ex) {
	    	ex.printStackTrace();
            }
	status.setDone(true);
	logger.info("The Thread is stopped(" +url+").................. : " + status.getDone());
	return;
	}
    }

}
