/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.apifed;
import java.util.Map;
import java.util.Iterator;

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
    public static final String CRYPTO_WSF_URL = "CryptoURL";
    public static final String PLASMO_WSF_URL = "PlasmoURL";
    public static final String TOXO_WSF_URL = "ToxoURL";

    //Input Parameters
    public static final String PARAM_PROCESSNAME = "ProcessName";
    public static final String PARAM_PARAMETERS = "Parameters";
    public static final String PARAM_COLUMNS = "Columns";
    public static final String PARAM_ORGANISMS = "Organism";
    public static final String PARAM_QUERY = "Query";
    
    //Output Parameters
    public static final String COLUMN_RETURN = "Response";
    
    //Member Variable
    private String cryptoUrl;
    private String plasmoUrl;
    private String toxoUrl;

    public ApiFedPlugin() throws WsfServiceException {
	super(PROPERTY_FILE);
	cryptoUrl = getProperty(CRYPTO_WSF_URL);
	plasmoUrl = getProperty(PLASMO_WSF_URL);
	toxoUrl = getProperty(TOXO_WSF_URL);
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
	
	    String[] calls = {"doCrypto","",""};
	/*
	    if(params.containsKey(PARAM_ORGANISMS)){
		String orgsString = params.get(PARAM_ORGANISMS);
		calls = getRemoteCalls(orgsString);
	    } else{
		calls[0] = "doCrypto";
	        calls[1] = "doPlasmo";
	        calls[2] = "doToxo";
	    }
	*/
	
	    String query = "";	    
	    String processName = "org.apidb.apicomplexa.wsfplugin.wdkquery.WdkQueryPlugin";
	    if(params.containsKey(PARAM_QUERY)){
		query = params.get(PARAM_QUERY);
		params.remove(PARAM_QUERY);
	    } else{query = queryName;}
	       logger.info("QueryName = "+ query);
	    String[] arrayParams = getParams(params);
	    String[] componentColumns = removeProjectId(orderedColumns);

	    //String url = "http://mango.ctegd.uga.edu:8081/axis/services/WsfService";
	    //String url = "http://localhost:8082/axis/services/WsfService";
	    
	    //String cryptoUrl = "http://mango.ctegd.uga.edu:9020/axis/services/WsfService";
	    //String plasmoUrl = "http://qa.plasmodb.org/plasmoaxis/axis/services/WsfService";
	    //String toxoUrl = "http://qa.plasmodb.org/plasmoaxis/axis/services/WsfService";

	    //Objects to hold the results of the threads
	    CompResult cryptoResult = new CompResult();
	    CompResult plasmoResult = new CompResult();
	    CompResult toxoResult = new CompResult();
	    
	    //Objects to hold the status of the Threads
	    Status cryptoThreadStatus = new Status(false);
	    Status plasmoThreadStatus = new Status(false);
	    Status toxoThreadStatus = new Status(false);
	   
	    //Call only the needed component sites
	    if(calls[0].equals("")){cryptoThreadStatus.setDone(true);}
	    else {
		Thread cryptoThread = 
		    new WdkQuery(cryptoUrl, processName, queryName, arrayParams, componentColumns, cryptoResult, cryptoThreadStatus);
		cryptoThread.start();
	    }
	    if(calls[1].equals("")){plasmoThreadStatus.setDone(true);}
	    else {
		Thread plasmoThread = 
		    new WdkQuery(plasmoUrl, processName, queryName, arrayParams, componentColumns, plasmoResult, plasmoThreadStatus);
		plasmoThread.start();
	    }
	    if(calls[2].equals("")){toxoThreadStatus.setDone(true);}
	    else {
		Thread toxoThread = 
		    new WdkQuery(toxoUrl, processName, queryName, arrayParams, componentColumns, toxoResult, toxoThreadStatus);
		toxoThread.start();
	    }
	   
	    while(!(cryptoThreadStatus.getDone() && plasmoThreadStatus.getDone() && toxoThreadStatus.getDone())){
		try{
		    logger.info(cryptoThreadStatus.getDone());
		    Thread.sleep(1000);
		continue;
		}catch(InterruptedException e){}
	    }
	    

	    String[][] result = null;
	    result = combineResults(cryptoResult.getAnswer(), plasmoResult.getAnswer(), toxoResult.getAnswer(), orderedColumns);
	    //result = cryptoResult.getAnswer();
	    
	    return result; 
    }
    
    private String[] getParams (Map<String,String> params)
    {
    	String[] arrParams = new String[params.size()];
	Iterator it = params.keySet().iterator();
	int i = 0;
	while(it.hasNext()){
		String key = (String)it.next();
		String val = params.get(key);
		arrParams[i] = key+"="+val;
		i++;
	}
	return arrParams;
    }
    
    private String[] getRemoteCalls(String orgs)
    {
	String[] calls = {"","",""};
	String[] orgArray = orgs.split(",");
	for(String organism:orgArray){
	    if(organism.charAt(0)=='C')
		calls[0] = "doCrypto";
	    if(organism.charAt(0)=='P')
		calls[1] = "doPlasmo";
	    if(organism.charAt(0)=='T')
		calls[2] = "doToxo";
	}
	return calls;
    }

    private String[][] combineResults(String[][] crypto, String[][] plasmo, String[][] toxo, String[] cols)
    {
	//Determine the length of the new array
	int numrows = 0;
	if(crypto!=null)numrows = numrows + crypto.length;
	if(plasmo!=null)numrows = numrows + plasmo.length;
	if(toxo!=null)numrows = numrows + toxo.length;
	int i = 0;
	String[][] combined = new String[numrows][cols.length + 1]; //add one column for the addition of projectId

	//Find the index for the projectId columns
	int projectIndex = 0;
	for(String col:cols){
	    if(col.equals("project_id")){break;}
	    else{projectIndex++;}
	}

	//Add crypto result, if it is not empty, to the final results
	if(crypto!=null){
	    if(!crypto[0][0].equals("ERROR")){
		for(String[] rec:crypto){
		    logResults(rec);
		    combined[i] = rec;
		    combined[i] = insertProjectId(combined[i], projectIndex, "cryptodb");
		    i++;
		}
	    }//else {TODO :: Process this error to display message in the header section of the Summary Page	
	}

	//Add plasmo result, if it is not empty, to the final results
	if(plasmo!=null){
	    if(!plasmo[0][0].equals("ERROR")){
		for(String[] rec:plasmo){
		    combined[i] = rec;
		    combined[i] = insertProjectId(combined[i], projectIndex, "plasmodb");
		    i++;
		}
	    }//else {TODO :: Process this error to display message in the header section of the Summary Page	
	}
	
	//Add toxo result, if it is not empty, to the final results
	if(toxo!=null){
	    if(!toxo[0][0].equals("ERROR")){
		for(String[] rec:toxo){
		    combined[i] = rec;
		    combined[i] = insertProjectId(combined[i], projectIndex, "toxodb");
		    i++;
		}
	    }//else {TODO :: Process this error to display message in the header section of the Summary Page
	}
	return combined;
    }
    
    private void logResults(String[] r){
	for(String cr:r){
	    logger.info("------- Record : " + cr);
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
	    logger.info("------------"+col+"---"+col.equals("project_id")+"-----------");
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
	public CompResult(){
	    answer = null;
	}
	public void setAnswer(String[][] answer){
	    this.answer = answer;
	}
	public String[][] getAnswer(){return this.answer;}
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
         
            WsfResponse response = service.invoke(pluginName, queryName, params, cols);
          
	    String message = response.getMessage();
            result.setAnswer(response.getResults());
	    
	    } catch (MalformedURLException ex) {
	    	ex.printStackTrace();
            } catch (ServiceException ex) {
            	ex.printStackTrace();
            } catch (RemoteException ex) {
	    	ex.printStackTrace();
            }
	status.setDone(true);
	logger.info("INSDIE THREAD : " + status.getDone());
	return;
	}
    }

}
