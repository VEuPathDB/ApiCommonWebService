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
import java.net.URLConnection;
import java.net.SocketTimeoutException;
import java.rmi.RemoteException;
import java.util.LinkedHashMap;

import javax.xml.rpc.ServiceException;
import org.gusdb.wsf.client.WsfResponse;
import org.gusdb.wsf.client.WsfService;
import org.gusdb.wsf.client.WsfServiceServiceLocator;

import org.apache.axis.MessageContext;

import javax.servlet.ServletContext;
import javax.servlet.Servlet;

/**
 * @author Cary Pennington
 * @created Dec 20, 2006
 */
public class ApiFedPlugin extends WsfPlugin {
	
    //Static Property Values
    public static final String PROPERTY_FILE = "apifed-config.xml";
    
    public static final String MAPPING_FILE = "MappingFile";

    public static final String VERSION = "1.1.4";
    //Input Parameters
    public static final String PARAM_PROCESSNAME = "ProcessName";
    public static final String PARAM_PARAMETERS = "Parameters";
    public static final String PARAM_COLUMNS = "Columns";
    public static final String PARAM_ORGANISMS = "organism";
    public static final String PARAM_QUERY = "Query";
    
    //Output Parameters
    public static final String COLUMN_RETURN = "Response";
    
    //Member Variable
    private Site[] sites;
    private boolean doAll;
    private int timeOutInMinutes;
    private Document mapDoc = null;

    public ApiFedPlugin() throws WsfServiceException {
	super();
	logger.info("Parent Constructor Finished");
	loadProps();
	logger.info("Properties File Loaded");
    }

    //Mapping Functions

  
    private String getMapFilePath(){
	MessageContext msgContext = MessageContext.getCurrentContext();
	ServletContext servletContext = ((Servlet) msgContext.getProperty(org.apache.axis.transport.http.HTTPConstants.MC_HTTP_SERVLET)).getServletConfig().getServletContext();
	String root = servletContext.getRealPath("/");
	root = root + "WEB-INF/wsf-config/";
	logger.info("Mapping File Path == " + root);
    return root;
    }

    private void loadProps()
    {
	String propFilename = getMapFilePath().concat(PROPERTY_FILE);
	Document propDoc = null;
	try{
	    propDoc = new SAXBuilder().build(new File(propFilename));
	    Element config_e = propDoc.getRootElement();
	    Element sites_e = config_e.getChild("Sites");
	    List models = sites_e.getChildren();
	    int sites_count = models.size();
	    sites = new Site[sites_count];
	    for (int i=0; i < sites_count; i++){
		sites[i] = new Site();
		Element site = (Element)models.get(i);
		sites[i].setName(site.getAttributeValue("name"));
		logger.info("name ------- " + sites[i].getName());
		sites[i].setProjectId(site.getAttributeValue("projectId"));
		sites[i].setUrl(site.getAttributeValue("url"));
		sites[i].setMarker(site.getAttributeValue("marker"));
		sites[i].setOrganism("");
	    }
	    String mapfile = getMapFilePath().concat(config_e.getChild("MappingFile").getAttributeValue("name"));
	    timeOutInMinutes = new Integer(config_e.getChild("Timeout").getAttributeValue("minutes")).intValue();
	    logger.info("Mapping File ========== " + mapfile);
	    logger.info("Timeout Value ========== " + timeOutInMinutes);
	    mapDoc = createMap(mapfile);
	}catch(Exception e){logger.info(e);}
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
    protected String[][] execute(String queryName, Map<String, String> params, String[] orderedColumns) throws WsfServiceException {
	doAll = false;
	logger.info("ApiFedPlugin Version : " + this.VERSION);
	initSites();


            //Spliting the QueryName up for Mapping
	    String apiQueryFullName = queryName.replace('.',':');
	    String[] apiQueryNameArray = apiQueryFullName.split(":");
	    String apiQuerySetName = apiQueryNameArray[0];
	    String apiQueryName = apiQueryNameArray[1];

        String orgName = null;
        String datasetName = null;
        
	    //Determine if Query is Parameter, Attribute, or PrimaryKey Query

	    if(apiQuerySetName.contains("Params")){
		doAll = true;
		getRemoteCalls(doAll);
	    }else{

		orgName = hasOrganism(params);
		datasetName = hasDataset(params);
		if(orgName != null){
		    String orgsString = params.get(orgName);
		    getRemoteCalls(orgsString);
		} else if(datasetName != null){	
		    String datasetString = params.get(datasetName);
		    getRemoteCalls(datasetString);
		} else {
		    doAll = true;
		    getRemoteCalls(doAll);
		}
	    }

	    String query = "";	    
	    String processName = "org.apidb.apicomplexa.wsfplugin.wdkquery.WdkQueryPlugin";
	    if(params.containsKey(PARAM_QUERY)){
		query = params.get(PARAM_QUERY);
		params.remove(PARAM_QUERY);
		queryName = query;
	    } else{query = queryName;}
	   
	    String[] componentColumns = removeProjectId(orderedColumns);
	    //Object to hold the results of the threads
	    CompResult[] compResults = new CompResult[sites.length];
	    for(CompResult compResult:compResults)
		compResult = null;   

	    //Object to hold the status of the Threads
	    Status[] compStatus = new Status[sites.length];
	    for(Status myStatus:compStatus)
	    	myStatus = null;
	    
	    //Object to hold the Threads
	    Thread[] compThreads = new Thread[sites.length];
	    for(Thread thread:compThreads)
		thread = null;
	    
	  

	    //Preparing and executing the propriate component sites for this Query
	    logger.info("invoking the web services");
	    int thread_counter = 0;
	    for(Site site:sites){
		if(site.hasOrganism()){
		    String compQuerySetName = mapQuerySet(apiQuerySetName, site.getName());
		    String compQueryName = mapQuery(apiQuerySetName, apiQueryName, site.getName());
		    String compQueryFullName= "";
		    if(compQuerySetName.length()!=0 && compQueryName.length()!=0){ 
			compQueryFullName = compQuerySetName + "." + compQueryName;
		    }else{compQueryFullName = queryName;} 
      	
		    Map<String,String> siteParams = params;
		
		    if(orgName != null){
			if(orgName.indexOf("pforganism") != -1 || orgName.indexOf("pborganism") != -1)
			    siteParams.remove(orgName);
		    }

		    String[] arrayParams = getParams(siteParams, site.getOrganism(), orgName, datasetName, site.getName(), apiQuerySetName, apiQueryName);
		    //logger.info("getParams DONE   Organism = " + site.getOrganism());
		    compStatus[thread_counter] = new Status(false);
		    //logger.info("status set to false");
		    compResults[thread_counter] = new CompResult();
		    //logger.info("compResults initialized");
		    compResults[thread_counter].setSiteName(site.getProjectId());
		    //logger.info("siteName = projectId Done");
		    compThreads[thread_counter] = 
                          new WdkQuery(site.getUrl(), processName, compQueryFullName, arrayParams, componentColumns, compResults[thread_counter], compStatus[thread_counter]);
		    compThreads[thread_counter].start();
		}
		thread_counter++;
	    }
	    long tTime = System.currentTimeMillis();

	    while(!allDone(compStatus)){
	      try{
		Thread.sleep(500);		
		if((System.currentTimeMillis() - tTime) > timeOutInMinutes * (60 * 1000)){
		    for(int i = 0; i < sites.length; i++){
			if(!compStatus[i].getDone()){
			    compThreads[i].stop();
			    compStatus[i].setDone(true);
			    compResults[i].setAnswer(new String[0][0]);
			    compResults[i].setMessage("-1");
			    logger.info("Thread " + i + " killed!!!");
			}
		    }
		}
		continue;
	      }catch(InterruptedException e){logger.info("From InterruptedException Catch Block :::: " + e);}
	    }

	    String[][] result = null;
            logger.info("Entering Combine Results");
	    result = combineResults(compResults, orderedColumns);
	    return result; 
    }
    
    private void initSites()
    {
	for(int i = 0; i < sites.length; i++)
	    sites[i].setOrganism("");
    }

    private boolean allDone(Status[] S)
    {
	for(Status s:S){
	    if(s != null){
		if(!s.getDone()) return false;
	    }
	}
	return true;
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

    private String[] getParams (Map<String,String> params, String localOrgs, String orgParam, String datasetParam, String modelName, String querySetName, String queryName)
    {
    	String[] arrParams = new String[params.size()+1];
	Iterator it = params.keySet().iterator();
	int i = 0;
	while(it.hasNext()){
		String key = (String)it.next();
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
    

    private void getRemoteCalls(boolean all)
    {
	for(Site site:sites){
	    site.setOrganism("doAll");
	}
    }

    private void getRemoteCalls(String orgs)
    {
	String[] orgArray = orgs.split(",");
	for(String organism:orgArray){
	    for(int i = 0; i < sites.length; i++){
		if(organism.matches(sites[i].getMarker())){
		   sites[i].appendOrganism(organism);
		}
	    }
	}
    }

    private String[][] combineResults(CompResult[] compResults, String[] cols)
    {
	message = "";
	int numrows = 0;
	int  index = 0;
	for(CompResult cR:compResults){
	    if(cR != null){
		String[][] anser = cR.getAnswer();
		if(anser != null && anser.length > 0 && !anser[0][0].equals("ERROR")){
		    numrows = numrows + anser.length;
		    logger.info("Answer " + index + " total added .... "+ anser.length);
		}
	    }
	}
        int i = 0;
	String[][] combined = new String[numrows][cols.length + 1]; //add one column for the addition of projectId
	
	logger.info("Total Number of Rows in Combined Result is ----------> " + numrows);
	
	//Find the index for the projectId columns
	int projectIndex = 0;
	for(String col:cols){
	    if(col.equals("project_id")){break;}
	    else{projectIndex++;}
	}

	//Add  result, if it is not empty, to the final results
	for(CompResult compResult:compResults){
	    if(compResult != null){
		String[][] answer = compResult.getAnswer(); 
		if(answer!=null){ 
		    if(answer.length >= 0)
			if(message.length() == 0)
			    message = compResult.getSiteName()+":"+compResult.getMessage();
		        else
			    message = message + "," + compResult.getSiteName()+":"+compResult.getMessage();
		    if(answer.length > 0){ 
			if(!answer[0][0].equals("ERROR")){
			    for(String[] rec:answer){
				combined[i] = rec;
				combined[i] = insertProjectId(combined[i], projectIndex, compResult.getSiteName());
				i++;
			    }// Loop for records
			}// if answer[0][0] = ERROR
		    }// if answer is 0 lentgh
		}// if answer == null
	    }// if result = null
	}// Loop for all Results

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
	private String siteName;
	private String[][] answer;
	private String message;
	public CompResult(){
	    siteName = "";
	    answer = null;
	    message = "";
	}
	public void setSiteName(String siteName){
	    this.siteName = siteName;
	}
	public String getSiteName(){return this.siteName;}
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
	    String errorMessage = "Thread ran and exited Correctly";
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
		errorMessage = "MalformedURLException Occured : Thread exited";
            } catch (ServiceException ex) {
            	ex.printStackTrace();
		errorMessage = "ServiceException Occured : Thread exited";
            } catch (RemoteException ex) {
	    	ex.printStackTrace();
		errorMessage = "RemoteException Occured : Thread exited" + ex.getCause();
            }
	    finally {
		status.setDone(true);
		logger.info("The Thread is stopped(" +url+").................. : " + status.getDone() + "  Error Message = " + errorMessage);
		return;}
	}
    }

    class Site 
    {
	private String name;
	private String projectId;
	private String url;
	private String marker;
	private String organism;
	public String getName(){return name;}
	public String getProjectId(){return projectId;}
	public String getUrl(){return url;}
	public String getMarker(){return marker;}
	public String getOrganism(){return organism;}
	public void setName(String name) {this.name = name;}
	public void setProjectId(String projectId) {this.projectId = projectId;}
	public void setUrl(String url) {this.url = url;}
	public void setMarker(String marker) {this.marker = marker;}
	public void setOrganism(String organism) {this.organism = organism;}
	public void appendOrganism(String organism) 
	{
	    if(this.organism.length() == 0)
	       setOrganism(organism);
	    else 
	       setOrganism(this.organism + "," + organism);
	}
        public boolean hasOrganism()
	{
	    if(this.organism.length() == 0) return false;
	    else return true;
	}
    }

}
