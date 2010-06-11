package org.apidb.apicomplexa.wsfplugin.apifed;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wdk.model.query.ProcessQueryInstance;
import org.gusdb.wsf.client.WsfService;
import org.gusdb.wsf.client.WsfServiceServiceLocator;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.WsfRequest;
import org.gusdb.wsf.plugin.WsfResponse;
import org.gusdb.wsf.plugin.WsfServiceException;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

/**
 * @author Cary Pennington
 * @created Dec 20, 2006
 * 
 *          2.0.0 -- currently deployed for ApiDB Live Site 2.1 -- updated to
 *          take advantage of the consistancy in the Unified Model -- Added
 *          functionality to retrieve Parameters (Vocab and Enum) from the
 *          component Sites -- Removed all need fro DBLinks in the ApiDB Model
 *          -- Changed the combineResults to use a LinkedHashMap to eliminate
 *          duplicates for the parameter queries
 */
public class ApiFedPlugin extends AbstractPlugin {

    // Static Property Values
    public static final String PROPERTY_FILE = "apifed-config.xml";

    public static final String MAPPING_FILE = "MappingFile";

    public static final String VERSION = "2.1";

    public static final String PARAM_SET_NAME = "VQ";
    // Input Parameters
    public static final String PARAM_PROCESSNAME = "ProcessName";
    public static final String PARAM_PARAMETERS = "Parameters";
    public static final String PARAM_COLUMNS = "Columns";
    public static final String PARAM_ORGANISMS = "organism";
    public static final String PARAM_QUERY = "Query";

    // Output Parameters
    public static final String COLUMN_RETURN = "Response";

    // Member Variable
    private Site[] sites;
    private boolean doAll;
    private boolean hasProjectId;
    private int timeOutInMinutes;
    private Document mapDoc = null;
    private String primaryKey = null;

    public ApiFedPlugin() throws WsfServiceException {
        super();
    }

    // load properties

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.AbstractPlugin#initialize(java.util.Map)
     */
    @Override
    public void initialize(Map<String, Object> context)
            throws WsfServiceException {
        super.initialize(context);

        logger.debug("Parent Constructor Finished");
        loadProps();
        logger.debug("Properties File Loaded");
    }

    // Mapping Functions

    private String getMapFilePath() {
        String configDir = (String) context.get(CTX_CONFIG_PATH);
        if (configDir == null) {
            URL url = this.getClass().getResource("/");
            configDir = url.toString() + "/../wsf-config/";
        }
        logger.debug("Mapping File Path == " + configDir);
        return configDir;
    }

    private void loadProps() {
        logger.debug("******in loadProps()*******\n\n\n");
        String propFilename = getMapFilePath().concat(PROPERTY_FILE);
        Document propDoc = null;
        try {
            propDoc = new SAXBuilder().build(new File(propFilename));
            Element config_e = propDoc.getRootElement();
            Element sites_e = config_e.getChild("Sites");
            List<?> models = sites_e.getChildren();
            int sites_count = models.size();
            sites = new Site[sites_count];
            for (int i = 0; i < sites_count; i++) {
                sites[i] = new Site();
                Element site = (Element) models.get(i);
                sites[i].setName(site.getAttributeValue("name"));
                logger.debug("name ------- " + sites[i].getName());
                sites[i].setProjectId(site.getAttributeValue("projectId"));
                sites[i].setUrl(site.getAttributeValue("url"));
                sites[i].setMarker(site.getAttributeValue("marker"));
                sites[i].setOrganism("");
            }
            String mapfile = getMapFilePath().concat(
                    config_e.getChild("MappingFile").getAttributeValue("name"));
            timeOutInMinutes = new Integer(
                    config_e.getChild("Timeout").getAttributeValue("minutes")).intValue();
            logger.debug("Mapping File ========== " + mapfile);
            logger.debug("Timeout Value ========== " + timeOutInMinutes);
            // mapDoc = createMap(mapfile);
        } catch (Exception e) {
            logger.debug(e);
        }
    }

    private Document createMap(String mapFile) {

        Document doc = null;
        try {
            doc = new SAXBuilder().build(new File(mapFile));
        } catch (JDOMException e) {
            logger.debug(e.toString());
            e.printStackTrace();
        } catch (IOException e) {
            logger.debug(e.toString());
            e.printStackTrace();
        }

        return doc;
    }

    private String mapQuerySet(String querySet, String project) {
        Element querySetMapping = null;
        try {
            if (mapDoc == null)
                logger.debug("Error:::::::MapDocument is empty");
            querySetMapping = mapDoc.getRootElement().getChild("QuerySets").getChild(
                    querySet);
            String componentQuerySet = querySetMapping.getAttributeValue(project);
            return componentQuerySet;
        } catch (NullPointerException e) {
            String ret = "";
            return ret;
        }
    }

    private String mapQuery(String querySet, String queryName, String project) {
        String xPath = "/FederationMapping/QuerySets/" + querySet
                + "/wsQueries/" + queryName;
        try {
            Element queryMapping = mapDoc.getRootElement().getChild("QuerySets").getChild(
                    querySet).getChild("wsQueries").getChild(queryName);
            String componentQuery = queryMapping.getAttributeValue(project);

            return componentQuery;
        } catch (NullPointerException e) {
            String ret = "";
            return ret;
        }
    }

    private String mapParam(String querySet, String queryName,
            String paramName, String project) {
        String xPath = "/FederationMapping/QuerySets/" + querySet
                + "/wsQueries/" + queryName + "/Params/params." + paramName;
        try {
            Element paramMapping = mapDoc.getRootElement().getChild("QuerySets").getChild(
                    querySet).getChild("wsQueries").getChild(queryName).getChild(
                    "Params").getChild("params." + paramName);
            String componentParam = paramMapping.getAttributeValue(project);
            return componentParam;
        } catch (NullPointerException e) {
            String ret = "";
            return ret;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getRequiredParameters()
     */
    public String[] getRequiredParameterNames() {
        return new String[] {};
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getColumns()
     */
    public String[] getColumns() {

        return new String[] {};

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#validateParameters(java.util.Map)
     */
    public void validateParameters(WsfRequest request)
            throws WsfServiceException {
    // Do Nothing in this plugin
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
     */
    public WsfResponse execute(WsfRequest request) throws WsfServiceException {
        Site[] sites = new Site[this.sites.length];
        for (int i = 0; i < sites.length; i++) {
            sites[i] = (Site) this.sites[i].clone();
        }

        String ocStr = "";
        String[] orderedColumns = request.getOrderedColumns();
        for (String s : orderedColumns)
            ocStr = ocStr + "-------" + s;
        logger.debug("****************execute(): orderedColumns are: " + ocStr);
        primaryKey = null;
        hasProjectId = false;
        doAll = false;
        logger.info("ApiFedPlugin Version : " + ApiFedPlugin.VERSION);
        boolean isParam = false;
        logger.debug("Sites Array being Initialized ... ");
        logger.debug("***sites organisms are: \n\n" + sites[0].getName() + ":"
                + sites[0].getOrganism() + "\n" + sites[1].getName() + ":"
                + sites[1].getOrganism() + "\n" + sites[2].getName() + ":"
                + sites[2].getOrganism() + "\n" + sites[3].getName() + ":"
                + sites[3].getOrganism() + "\n" + sites[4].getName() + ":"
                + sites[4].getOrganism() + "\n" + sites[5].getName() + ":"
                + sites[5].getOrganism() + "\n" + sites[6].getName() + ":"
                + sites[6].getOrganism() + "\n" + sites[7].getName() + ":"
                + sites[7].getOrganism() + "\n\n");

        String processName = "org.apidb.apicomplexa.wsfplugin.wdkquery.WdkQueryPlugin";

        // Spliting the QueryName up for Mapping
        String queryName = request.getContext().get(ProcessQueryInstance.CTX_QUERY);
        logger.info("QueryName = " + queryName);
        String apiQueryFullName = queryName.replace('.', ':');
        logger.debug("ApiQueryFullName = " + apiQueryFullName);
        String[] apiQueryNameArray = apiQueryFullName.split(":");
        String apiQuerySetName = apiQueryNameArray[0];
        String apiQueryName = apiQueryNameArray[1];
        logger.debug("Full QueryName = " + queryName);

        // Determine if the Query is a Parameter Query
        if (apiQuerySetName.contains(PARAM_SET_NAME)) isParam = true;

        Map<String, String> params = request.getParams();
        String orgName = null;
        String datasetName = null;
        if (isParam) {
            logger.info("Found that QuerySet Name = VQ");
            doAll = true;
            getRemoteCalls(doAll, sites);
            logger.debug("RemoteCalls Returned successfully");
        } else {
            orgName = hasOrganism(params);
            logger.debug("orgName is " + orgName);
            datasetName = hasDataset(params);
            if (orgName != null) {
                if (orgName.indexOf("primaryKey") != -1) {

                    this.primaryKey = params.get(orgName); // WORKAROUND FOR
                    // COLUMN
                    // RESTRICTIONS

                    logger.debug("Working with AjaxRecordClass primaryKey calculations");
                    String pk = params.get(orgName);
                    logger.debug("orgName = " + orgName + ",   pk = " + pk);
                    String[] parts = new String[2];
                    if (pk.indexOf(":") != -1) {
                        parts = pk.split(":");
                        logger.debug("Query = " + parts[0] + ", primaryKey = "
                                + parts[1]);
                        params.put("Query", parts[0]);
                        getRemoteCalls(parts[1], sites);
                    } else {
                        getRemoteCalls(doAll, sites);
                        logger.debug("Query = " + queryName + ", primaryKey = "
                                + pk);
                    }
                } else {

                    String orgsString = params.get(orgName);
                    logger.debug("orgsString is:---|" + orgsString + "|----\n");
                    getRemoteCalls(orgsString, sites);
                }
            } else if (datasetName != null) {
                String datasetString = params.get(datasetName);
                getRemoteCalls(datasetString, sites);
            } else {
                doAll = true;
                getRemoteCalls(doAll, sites);
            }
        }

        String[] componentColumns = orderedColumns;
        if (!isParam) {
            // componentColumns = removeProjectId(orderedColumns);
            logger.debug("ProjectId Removed from Column Set");
        }

        // Object to hold the results of the threads
        CompResult[] compResults = new CompResult[sites.length];

        // Object to hold the status of the Threads
        Status[] compStatus = new Status[sites.length];

        // Object to hold the Threads
        Thread[] compThreads = new Thread[sites.length];

        // Preparing and executing the propriate component sites for this Query
        logger.info("invoking the web services");
        logger.info("******if the next message you see says: *Entering Combine Results* we are not accessing any component site: probably your apifed-config does not have all necessary organism values used to select site; or maybe the value stored in the organism parameter cannot be read as a string\n");
        int thread_counter = 0;
        logger.debug("***sites organisms are: \n\n" + sites[0].getName() + ":"
                + sites[0].getOrganism() + "\n" + sites[1].getName() + ":"
                + sites[1].getOrganism() + "\n" + sites[2].getName() + ":"
                + sites[2].getOrganism() + "\n" + sites[3].getName() + ":"
                + sites[3].getOrganism() + "\n" + sites[4].getName() + ":"
                + sites[4].getOrganism() + "\n" + sites[5].getName() + ":"
                + sites[5].getOrganism() + "\n" + sites[6].getName() + ":"
                + sites[6].getOrganism() + "\n" + sites[7].getName() + ":"
                + sites[7].getOrganism() + "\n\n");

        for (Site site : sites) {
            if (site.hasOrganism()) {
                String compQueryFullName = queryName;
                Map<String, String> siteParams = params;
                logger.debug("organismParameterName = " + orgName);
                if (orgName != null) {
                    if (orgName.indexOf("pforganism") != -1
                            || orgName.indexOf("pborganism") != -1) {
                        siteParams.remove(orgName);
                        logger.debug("organism Parameter Removed");
                    }
                }
                Map<String, String> arrayParams = getParams(siteParams,
                        site.getOrganism(), orgName, datasetName,
                        site.getName(), apiQuerySetName, apiQueryName);
                logger.debug("getParams DONE   Organism = "
                        + site.getOrganism());
                compStatus[thread_counter] = new Status(false);
                logger.debug("status set to false");
                compResults[thread_counter] = new CompResult();
                logger.debug("compResults initialized");
                compResults[thread_counter].setSiteName(site.getProjectId());
                logger.debug("siteName = projectId Done");

                WsfRequest compRequest = new WsfRequest();
                compRequest.setParams(arrayParams);
                compRequest.setOrderedColumns(componentColumns);

                Map<String, String> compContext = request.getContext();
                compContext.put(ProcessQueryInstance.CTX_QUERY,
                        compQueryFullName);

                compThreads[thread_counter] = new WdkQuery(site.getUrl(),
                        processName, compRequest, compResults[thread_counter],
                        compStatus[thread_counter]);
                compThreads[thread_counter].start();
            }
            thread_counter++;
        } // end for

        long tTime = System.currentTimeMillis();

        while (!allDone(compStatus)) {
            try {
                Thread.sleep(500);
                if ((System.currentTimeMillis() - tTime) > timeOutInMinutes
                        * (60 * 1000)) {
                    for (int i = 0; i < sites.length; i++) {
                        if (!compStatus[i].getDone()) {
                            compThreads[i].stop();
                            compStatus[i].setDone(true);
                            compResults[i].setAnswer(new String[0][0]);
                            compResults[i].setMessage("-1");
                            logger.info("Thread " + i + " killed!!!");
                        }
                    }
                }
                continue;
            } catch (InterruptedException e) {
                logger.debug("From InterruptedException Catch Block :::: " + e);
            }
        }

        logger.info("Entering Combine Results");
        StringBuffer message = new StringBuffer();
        String[][] result = combineResults(compResults, orderedColumns,
                isParam, message);
        WsfResponse wsfResult = new WsfResponse();
        wsfResult.setResult(result);
        wsfResult.setMessage(message.toString());
        return wsfResult;
    }

    private void initSites() {
        for (int i = 0; i < sites.length; i++)
            sites[i].setOrganism("");
    }

    private boolean allDone(Status[] S) {
        for (Status s : S) {
            if (s != null) {
                if (!s.getDone()) return false;
            }
        }
        return true;
    }

    private String hasOrganism(Map<String, String> p) {
        String orgName = null;
        for (String pName : p.keySet()) {
            if (pName.indexOf("organism") != -1
                    || pName.indexOf("Organism") != -1
                    || pName.indexOf("primaryKey") != -1) {
                orgName = pName;
                break;
            }
        }
        return orgName;
    }

    private String hasDataset(Map<String, String> p) {
        String dsName = null;
        for (String pName : p.keySet()) {
            if (pName.indexOf("Dataset") != -1) {
                dsName = pName;
                break;
            }
        }
        return dsName;
    }

    private Map<String, String> getParams(Map<String, String> params,
            String localOrgs, String orgParam, String datasetParam,
            String modelName, String querySetName, String queryName) {
        Map<String, String> arrParams = new HashMap<String, String>();
        logger.info("ORGPARAM = " + orgParam);
        for (String key : params.keySet()) {
            logger.info("getParams------> key = " + key + ", value = "
                    + params.get(key));
            String compKey = key;
            String val;
            if ((key.equals(orgParam)) && !(orgParam.equals("primaryKey"))) {
                val = localOrgs;
            } else if (key.equals(datasetParam)) {
                val = localOrgs;
            } else {
                val = params.get(key);
            }
            arrParams.put(compKey, val);
        }
        arrParams.put("SiteModel", modelName);
        return arrParams;
    }

    private void getRemoteCalls(boolean all, Site[] sites) {
        for (Site site : sites) {
            site.setOrganism("doAll");
        }
    }

    private void getRemoteCalls(String orgs, Site[] sites) {
        logger.debug("Organism = " + orgs);
        String[] orgArray = orgs.split(",");
        for (String organism : orgArray) {
            for (int i = 0; i < sites.length; i++) {
                if (organism.trim().matches(sites[i].getMarker())) {
                    sites[i].appendOrganism(organism);
                }
            }
        }
        logger.debug("GetRemoteCalls() Done");
    }

    private String[][] combineResults(CompResult[] compResults, String[] cols,
            boolean isParamResult, StringBuffer message) {
        int numrows = 0;
        int index = 0;
        for (CompResult cR : compResults) {
            if (cR != null) {
                String[][] anser = cR.getAnswer();
                if (anser != null && anser.length > 0
                        && !anser[0][0].equals("ERROR")) {
                    numrows = numrows + anser.length;
                    logger.info("\n*********\n**************Answer " + index
                            + " total added .... " + anser.length);
                }
            }
        }
        logger.info("\n*******\n**********Total Number of Rows in Combined Result is (numrows) ----------> "
                + numrows);
        int i = 0;
        Map<Integer, String[]> combined = new LinkedHashMap<Integer, String[]>();

        // Find the index for the projectId columns
        int projectIndex = 0;
        for (String col : cols) {
            if (col.equals("project_id")) {
                break;
            } else {
                projectIndex++;
            }
        }
        // Add result, if it is not empty, to the final results
        for (CompResult compResult : compResults) {
            if (compResult != null) {
                String[][] answer = compResult.getAnswer();
                if (answer != null) {
                    if (answer.length >= 0)
                        if (message.length() > 0) message.append(",");
                    message.append(compResult.getSiteName() + ":"
                            + compResult.getMessage());
                    if (answer.length > 0) {
                        if (!answer[0][0].equals("ERROR")) {

                            for (String[] rec : answer) {
								int keyVal = 0;
                                if (!isParamResult && hasProjectId
                                        && primaryKey == null)
                                    rec = insertProjectId(rec, projectIndex,
                                            compResult.getSiteName());
                                if(isParamResult)
									keyVal = Arrays.deepToString(rec).hashCode(); //Use a HashCode to prevent duplicate values from parameters
								else
									keyVal = i; //Use incremented integer for results to ensure that a hash function does not inadvertently ommit results
								// logger.debug("\n\n*********\n***rec[0] and
                                // rec[1] are: " + rec[0] + " and " + rec[1]);
                                //combined.put(Arrays.deepHashCode(rec), rec);
								combined.put(keyVal, rec);
                                //combined.put(i, rec);
                                // logger.debug("\n*******\n**********Total
                                // Number of Rows in Combined Result is
                                // (combined size) ----------> " +
                                // combined.size() );
                                i++;
                                // logger.debug("\n*********\n***number of
                                // records in combined is: " + i);
                            }// Loop for records

                        }// if answer[0][0] = ERROR
                    }// if answer is 0 lentgh
                }// if answer == null
            }// if result = null
        }// Loop for all Results

        logger.debug("\n*******\n**********Total Number of Rows in Combined Result is (combined size) ----------> "
                + combined.size());
        logger.debug("\n*******\n**********IF THE FIRST NUMBER IS LESS THAN SECOND NUMBER, PROBLEM ");
        Collection<String[]> combinedColl = combined.values();
        String[][] combinedArr = new String[combined.size()][];
        return combinedColl.toArray(combinedArr);
    }

    private void logResults(String[] r, int i) {
        for (String cr : r) {
            logger.debug("------- Record :" + i + " " + cr);
        }
    }

    // Method to insert the projectId in the proper place in the return data
    private String[] insertProjectId(String[] rec, int index, String projectId) {
        String[] newRec = new String[rec.length + 1];
        if (index != rec.length) {
            String t1 = "";
            int newi = 0;
            for (int i = 0; i < rec.length; i++) {
                if (i == index) {
                    t1 = rec[i];
                    newRec[newi] = projectId;
                    newi++;
                    newRec[newi] = t1;
                } else {
                    newRec[newi] = rec[i];
                }
                newi++;
            }
        } else {
            for (int i = 0; i < rec.length; i++) {
                newRec[i] = rec[i];
            }
            newRec[newRec.length - 1] = projectId;
        }
        return newRec;
    }

    // Method to remove the projectId column from the colums sent to the
    // component sites
    private String[] removeProjectId(String[] cols) {
        hasProjectId = false;
        int projectIndex = 0;
        for (String col : cols) {
            if (col.equals("project_id")) {
                hasProjectId = true;
                break;
            } else {
                projectIndex++;
            }
        }
        if (!hasProjectId) return cols;

        int newindex = 0;
        String[] newCols = new String[cols.length - 1];
        for (int i = 0; i < cols.length; i++) {
            if (i == projectIndex) {
                continue;
            }
            newCols[newindex] = cols[i];
            newindex++;
        }
        return newCols;
    }

    // Inner Class to do invokations
    class CompResult {
        private String siteName;
        private String[][] answer;
        private String message;

        public CompResult() {
            siteName = "";
            answer = null;
            message = "";
        }

        public void setSiteName(String siteName) {
            this.siteName = siteName;
        }

        public String getSiteName() {
            return this.siteName;
        }

        public void setAnswer(String[][] answer) {
            this.answer = answer;
        }

        public String[][] getAnswer() {
            return this.answer;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getMessage() {
            return this.message;
        }
    }

    class Status {
        private boolean done;

        public Status(boolean done) {
            this.done = done;
        }

        public void setDone(boolean done) {
            this.done = done;
        }

        public boolean getDone() {
            return this.done;
        }
    }

    class WdkQuery extends Thread {
        private String url;
        private WsfRequest request;
        private CompResult result;
        private Status status;

        WdkQuery(String URL, String pluginname, WsfRequest request,
                CompResult r, Status s) {
            url = URL;
            this.request = request;
            this.request.setPluginClass(pluginname);
            result = r;
            status = s;
        }

        public void run() {
            String errorMessage = "Thread ran and exited Correctly";
            logger.info("The Thread is running.................." + url);
            WsfServiceServiceLocator locator = new WsfServiceServiceLocator();

            try {
                WsfService service = locator.getWsfService(new URL(url));
                long start = System.currentTimeMillis();

                // HACK
                // in the future, this inovcation should be replaced by the
                // newer version, invokeEx()
                WsfResponse wsfResult = service.invoke(request.toString());
                int packets = wsfResult.getTotalPackets();
                if (packets > 1) {
                    StringBuffer buffer = new StringBuffer(
                            wsfResult.getResult()[0][0]);
                    String requestId = wsfResult.getRequestId();
                    for (int i = 1; i < packets; i++) {
                        logger.debug("getting message " + requestId
                                + " pieces: " + i + "/" + packets);
                        String more = service.requestResult(requestId, i);
                        buffer.append(more);
                    }
                    String[][] content = Utilities.convertContent(buffer.toString());
                    wsfResult.setResult(content);
                }
                long end = System.currentTimeMillis();

                logger.info("Thread (" + url + ") has returned results in "
                        + ((end - start) / 1000.0) + " seconds.");
                result.setMessage(wsfResult.getMessage());
                result.setAnswer(wsfResult.getResult());

            } catch (Exception ex) {
                ex.printStackTrace();
                errorMessage = ex.getMessage() + " Occured : Thread exited"
                        + ex.getCause();
            } finally {
                status.setDone(true);
                logger.debug("The Thread is stopped(" + url
                        + ").................. : " + status.getDone()
                        + "  Error Message = " + errorMessage);
            }
            return;
        }
    }

    class Site implements Cloneable {
        private String name;
        private String projectId;
        private String url;
        private String marker;
        private String organism;

        public String getName() {
            return name;
        }

        public String getProjectId() {
            return projectId;
        }

        public String getUrl() {
            return url;
        }

        public String getMarker() {
            return marker;
        }

        public String getOrganism() {
            return organism;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setProjectId(String projectId) {
            this.projectId = projectId;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public void setMarker(String marker) {
            this.marker = marker;
        }

        public void setOrganism(String organism) {
            this.organism = organism;
        }

        public void appendOrganism(String organism) {
            organism = organism.trim();
            if (this.organism.length() == 0) setOrganism(organism);
            else setOrganism(this.organism + "," + organism);
        }

        public boolean hasOrganism() {
            if (this.organism.length() == 0) return false;
            else return true;
        }

        public Object clone() {
            Site s = new Site();
            s.name = name;
            s.projectId = projectId;
            s.url = url;
            s.marker = marker;
            s.organism = organism;
            return s;
        }
    }

    @Override
    protected String[] defineContextKeys() {
        return null;
    }
}
