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
	
    //Static Values
    public static final String CRYPTO_WSF_URL = "http://mango.ctegd.uga.edu:8081/axis/services/WsfService";
    public static final String PLASMO__WSF_URL = "http://qa.plasmodb.org/plasmoaxis/";
    public static final String TOXO_WSF_URL = "http://qa.plasmodb.org/plasmoaxis/";
    //Input Parameters
    public static final String PARAM_PROCESSNAME = "ProcessName";
    public static final String PARAM_PARAMETERS = "Parameters";
    public static final String PARAM_COLUMNS = "Columns";
    public static final String PARAM_QUERY = "Query";
    
    //Output Parameters
    public static final String COLUMN_RETURN = "Response";
    
    public ApiFedPlugin() throws WsfServiceException {}

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
	    
	    String orgsString = params.get(PARAM_ORGANISMS);
	    String[] calls = getRemoteCalls(orgsString);

	    String query = "";	    
	    String processName = "org.apidb.apicomplexa.wsfplugin.wdkquery.WdkQueryPlugin";
	    if(params.containsKey(PARAM_QUERY)){
		query = params.get(PARAM_QUERY);
		params.remove(PARAM_QUERY);
	    } else{query = queryName;}
	       logger.info("QueryName = "+ query);
	    String[] arrayParams = getParams(params);
	    
	    //String url = "http://mango.ctegd.uga.edu:8081/axis/services/WsfService";
	    String url = "http://localhost:8082/axis/services/WsfService";
	    //String url = "http://mango.ctegd.uga.edu:9020/axis/services/WsfService";
	    //String url = "http://qa.plasmodb.org/plasmoaxis/axis/services/WsfService";
	   
	    CompResult cryptoResult = new CompResult();
	    CompResult cryptoResult2 = new CompResult();
	    //String[][] toxoResult = null;
	    Status threadStatus = new Status(false);
	    Status threadStatus2 = new Status(false);
	    Thread myThread = new WdkQuery(url, processName, queryName, arrayParams, orderedColumns, cryptoResult, threadStatus); 
	    Thread myThread2 = new WdkQuery(url, processName, queryName, arrayParams, orderedColumns, cryptoResult2, threadStatus2);
	    myThread.start();
	    myThread2.start();
	    while(!(threadStatus.getDone() && threadStatus2.getDone())){
		try{
		    logger.info(threadStatus.getDone());
		    Thread.sleep(1000);
		continue;
		}catch(InterruptedException e){}
	    }
	    String[][] result = null;
	    result = cryptoResult.getAnswer();
	    
	    String[][] responseT = new String[result.length][orderedColumns.length];
	    
	    for(int i = 0; i < result.length; i++){
	    	for(int j = 0; j < orderedColumns.length; j++){
	    	     responseT[i][j] = result[i][j];
	    	}
	    }
	    
	    return responseT; 
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
	String[] calls = new String[3];
	String[] orgArray = orgs.split(",");
	for(organism:orgArray){
	    if(organism.charAt(0)=='C')
		calls[0] = "doCrypto";
	    if(organism.charAt(0)=='P')
		calls[1] = "doPlasmo";
	    if(organism.charAt(0)=='T')
		calls[2] = "doToxo";
	}
    }

    //Inner Class to do invokations
    class CompResult {
	private String[][] answer;
	public CompResult(){}
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
