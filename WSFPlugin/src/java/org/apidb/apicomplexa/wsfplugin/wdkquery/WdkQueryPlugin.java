/**
 *Version 2.0.0 --
 * Updated to work with the new Wdk Model.  The loading subroutine was updated to call parse() correctly for the new code in teh WDK
 * 2/27/2008 -- Removed valiadtion of the columns and inserted code to insert "N/A" into the result is a column does not exists on a component site
 */
package org.apidb.apicomplexa.wsfplugin.wdkquery;

import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gusdb.wdk.model.ModelXmlParser;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.WdkUserException;
import org.gusdb.wdk.model.dbms.ResultList;
import org.gusdb.wdk.model.jspwrap.WdkModelBean;
import org.gusdb.wdk.model.query.Column;
import org.gusdb.wdk.model.query.ProcessQuery;
import org.gusdb.wdk.model.query.ProcessQueryInstance;
import org.gusdb.wdk.model.query.Query;
import org.gusdb.wdk.model.query.QuerySet;
import org.gusdb.wdk.model.query.SqlQuery;
import org.gusdb.wdk.model.query.SqlQueryInstance;
import org.gusdb.wdk.model.query.param.AbstractEnumParam;
import org.gusdb.wdk.model.query.param.DatasetParam;
import org.gusdb.wdk.model.query.param.EnumParam;
import org.gusdb.wdk.model.query.param.Param;
import org.gusdb.wdk.model.query.param.ParamSet;
import org.gusdb.wdk.model.user.Dataset;
import org.gusdb.wdk.model.user.User;
import org.gusdb.wdk.model.user.UserFactory;
import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfResult;
import org.gusdb.wsf.plugin.WsfServiceException;
import org.json.JSONException;

/**
 * @author Cary Pennington
 * @created Dec 20, 2006
 * 
 *          2.0.0 -- Worked with ApiFedPlugin 2.0.0 2.1 -- Ditched the three
 *          number versioning... not that many changes -- Added support for
 *          accessing Enum Parameters on the componet Sites
 */
public class WdkQueryPlugin extends WsfPlugin {

    // Propert values
    public static final String PROPERTY_FILE = "wdkquery-config.xml";
    public static final String MODEL_NAME = "ModelName";
    public static final String GUS_HOME = "Gus_Home";

    public static final String VERSION = "2.1";
    // Input Parameters
    public static final String PARAM_PARAMETERS = "Parameters";
    public static final String PARAM_COLUMNS = "Columns";
    public static final String SITE_MODEL = "SiteModel";

    // Output Parameters
    public static final String COLUMN_RETURN = "Response";

    // Member Variables
    // private WdkModelBean[] models = null;
    private WdkModelBean model = null;
    // private static File m_modelFile = null;
    // private static File m_modelPropFile = null;
    // private static File m_schemaFile = null;
    // private static File m_configFile = null;
    // private static File m_xmlSchemaFile = null;
    private static String[] modelNames;
    private static String[] gus_homes;
    private static String[] siteNames;
    private static Map<String, WdkModelBean> modelName2Model = null;
    private static Object lock = new Object();

    public WdkQueryPlugin() throws WsfServiceException {
        super(PROPERTY_FILE);
        String modelName = getProperty(MODEL_NAME);
        modelNames = modelName.split(",");

        String gus_home = getProperty(GUS_HOME);
        gus_homes = gus_home.split(",");

        String siteName = getProperty(SITE_MODEL);
        siteNames = siteName.split(",");

        // modelName2Model = new HashMap<String,WdkModelBean>();
        initial();
        // logger.info("------------Plugin Initialized-----------------");
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getRequiredParameters()
     */
    @Override
    protected String[] getRequiredParameterNames() {
        return new String[] {};
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getColumns()
     */
    @Override
    protected String[] getColumns() {
        return new String[] {};
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
    protected void validateColumns(String[] orderedColumns) {
    // Overriding the parent class to do nothing in this plugin
    }

    // private void validateQueryParams(Map<String, String> params, Query q)
    // throws WsfServiceException {
    // logger.info("--------Validating Parameters---------------");
    // String[] reqParams = getParamsFromQuery(q);
    // for (String param : reqParams) {
    // if (!params.containsKey(param)) {
    // throw new WsfServiceException(
    // "The required parameter is missing: " + param);
    // }
    // }
    // }

    // private void validateQueryColumns(String[] orderedColumns, Query query)
    // throws WsfServiceException {
    // logger.info("------------Validating Columns---------------");
    // String[] reqColumns = getColumnsFromQuery(query);
    // // Set<String> colSet = new HashSet<String>(orderedColumns.length);
    // // for (String col : orderedColumns) {
    // // colSet.add(col);
    // // }
    // // for (String col : reqColumns) {
    // // if (!colSet.contains(col)) {
    // // throw new WsfServiceException(
    // // "The required column is missing: " + col);
    // // }
    // // }
    // // cross check
    // // colSet.clear();
    // Set<String> colSet = new HashSet<String>(reqColumns.length);
    // for (String col : reqColumns) {
    // colSet.add(col);
    // }
    // for (String col : orderedColumns) {
    // if (!colSet.contains(col)) {
    // throw new WsfServiceException("Unknown column: " + col);
    // }
    // }
    // }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
     */
    @Override
    protected WsfResult execute(String invokeKey, Map<String, String> params,
            String[] orderedColumns) throws WsfServiceException {

        logger.info("WdkQueryPlugin Version : " + WdkQueryPlugin.VERSION);
        // logger.info("Invoking WdkQueryPlugin......");
        String[][] componentResults = null;
        int resultSize = 1;
        ResultList results = null;
        if (params.containsKey("Query")) {
            invokeKey = params.get("Query");
            params.remove("Query");
        }
        String siteModel = params.get(SITE_MODEL);
        params.remove(SITE_MODEL);
        model = modelName2Model.get(siteModel);
        // logger.info("QueryName = "+ invokeKey);

        // Map<String,Object>SOParams = convertParams(params);
        // logger.info("Parameters were processed");
        Integer[] colindicies = new Integer[orderedColumns.length];// Variable
        // to
        // maintain
        // the order
        // of
        // columns
        // in the
        // result...
        // maintains
        // order
        // given by
        // Federation
        // Plugin
        try {

            // Reset the QueryName for testing reasons
            // invokeKey = "GeneFeatureIds.GeneByLocusTag";

            invokeKey = invokeKey.replace('.', ':');
            logger.info(invokeKey);
            Query q = null;
            String[] queryName = invokeKey.split(":");
            if (model.getModel().hasQuerySet(queryName[0])) {
				//if(params.containsKey("ServedQuery")){
					//String servedquery = params.get("servedQuery");
					//String[] sQuery = servedquery.split(".");
				//	if(model.getModel().hasQuerySet(sQuery[0])){
				//			Query que = model.getModel().getQuerySet(sQuery[0]).getQuery(sQuery[1]);
				//			QuerySet qs = model.getModel().getQuerySet(queryName[0]);
		          //      	q = qs.getQuery(queryName[1]);
		            //    	logger.info("Query found : " + q.getFullName());
					//}
				//}else{
                	QuerySet qs = model.getModel().getQuerySet(queryName[0]);
                	q = qs.getQuery(queryName[1]);
                	logger.info("Query found : " + q.getFullName());
				}
            } else {
                ParamSet ps = model.getModel().getParamSet(queryName[0]);
                Param p = ps.getParam(queryName[1]);
                logger.info("Parameter found : " + p.getFullName());
                String[][] enumValues = handleEnumParameters(p, orderedColumns);

                WsfResult wsfResult = new WsfResult();
                wsfResult.setResult(enumValues);
                return wsfResult;
            }

            Map<String, String> SOParams = convertParams(params, q.getParams());// getParamsFromQuery(q));

            // validateQueryParams(params,q);
            // logger.info("Parameters Validated...");
            // validateQueryColumns(orderedColumns,q);
            // logger.info("Columns Validated...");

            // Get the indicies of the correct columns for the component Query

            int i = 0;
            for (String oCol : orderedColumns) {
                int value = findColumnIndex(q, oCol);
                Integer iValue = new Integer(value);
                colindicies[i] = iValue;
                i++;
            }

            // WS Query processing
            if (q instanceof ProcessQuery) {
                logger.info("Processing WSQuery ...");
                ProcessQuery wsquery = (ProcessQuery) q;
                ProcessQueryInstance wsqi = (ProcessQueryInstance) wsquery.makeInstance(SOParams);
                results = wsqi.getResults();
            }
            // SQL Query Processing
            else {
                logger.info("Process SqlQuery ...");
                SqlQuery sqlquery = (SqlQuery) q;
                SqlQueryInstance sqlqi = (SqlQueryInstance) sqlquery.makeInstance(SOParams);
                results = sqlqi.getResults();
            }
            logger.info("Results set was filled");
            componentResults = results2StringArray(q.getColumns(), results);
            logger.info("Results have been processed.... "
                    + componentResults.length);

        } catch (WdkModelException ex) {
            logger.info("WdkMODELexception in execute()" + ex.toString());
            // String msg = ex.toString();
            String msg = ex.formatErrors();
            logger.info("Message = " + msg);
            // if(msg.matches("Invalid value"){}
            if (msg.indexOf("Please choose value(s) for parameter") != -1) {
                resultSize = 0;
            } else if (msg.contains("No value supplied for param")) {
                resultSize = 0;
            } else if (msg.indexOf("does not contain") != -1) {
                resultSize = -2;
            } else if (msg.indexOf("does not include") != -1) {
                resultSize = -2;
            } else if (msg.contains("datasets value '' has an error: Missing the value")) {
                resultSize = 0;
            } else {
                ex.printStackTrace();
                resultSize = -1;
            }
            // } catch(WdkUserException ex){
            // logger.info("WdkUSERexception IN execute()" + ex.toString());
            // ex.printStackTrace();
            // resultSize = -2;
        } catch (Exception ex) {
            logger.info("OTHERexception IN execute()" + ex.toString());

            ex.printStackTrace();
            resultSize = -1;

        }
        String[][] responseT = null;

        // Error condition

        if (componentResults == null) {
            // logger.info("Component Results = null!!!");
            responseT = new String[1][1];
            responseT[0][0] = "ERROR";
            if (resultSize > 0) resultSize = 0;
        } else {
            logger.info("Comp-Result not null... getting proper columns");

            // Successfull Query... need to ensure that the correct columns are
            // rerieved from the component site
            // use the Column name to Find the correct columns to return instead
            // of assumeing them to be in order.. oops

            responseT = new String[componentResults.length][orderedColumns.length];
            for (int i = 0; i < componentResults.length; i++) {
                for (int j = 0; j < colindicies.length; j++) {
                    int index = colindicies[j].intValue();
                    if (index >= 0) responseT[i][j] = componentResults[i][index];
                    else responseT[i][j] = "N/A";
                }
            }
            // logger.info("FINAL RESULT CALCULATED");
        }

        // Empty Result

        if (resultSize > 0) resultSize = componentResults.length;

        WsfResult wsfResult = new WsfResult();
        wsfResult.setResult(responseT);
        wsfResult.setMessage(Integer.toString(resultSize));
        wsfResult.setSignal(resultSize);

        return wsfResult;
    }

    private int findColumnIndex(Query q, String colName) {
        Column[] cols = q.getColumns();
        int index = 0;
        for (Column col : cols) {
            if (col.getName().equalsIgnoreCase(colName)) return index;
            else index++;
        }
        return -1;
    }

    // private Map<String, Object> convertParams(String[] p) {
    // Map<String, Object> ret = new HashMap<String, Object>();
    // for (String param : p) {
    // String[] pa = param.split("=");
    // ret.put(pa[0], (Object) pa[1]);
    // }
    // return ret;
    // }
    //
    // private Map<String, Object> convertParams(Map<String, String> p) {
    // Map<String, Object> ret = new HashMap<String, Object>();
    // for (String key : p.keySet()) {
    // Object o = p.get(key);
    // ret.put(key, o);
    // }
    // return ret;
    // }

    /**
     * private Map<String,Object> convertParams(Map<String,String> p, String[]
     * q) { Map<String,Object> ret = new HashMap<String,Object>(); for (String
     * key:p.keySet()){ Object o = p.get(key); for (String param : q) { if
     * (key.equals(param) || key.indexOf(param) != -1) { ret.put(param, o); } }
     * } return ret; }
     */

    private String convertDatasetId2DatasetChecksum(String sig_id)
            throws Exception {
        String[] parts = sig_id.split(":");
        String sig = parts[0];
        String id = parts[1];
        UserFactory userfactory = model.getModel().getUserFactory();
        User user = userfactory.getUser(sig);
        Integer idInt = new Integer(id);
        Dataset dataset = user.getDataset(idInt.intValue());
        String checksum = dataset.getChecksum();
        String sig_checksum = sig + ":" + checksum;
        return sig_checksum;
    }

    private Map<String, String> convertParams(Map<String, String> p, Param[] q) {
        Map<String, String> ret = new LinkedHashMap<String, String>();
        for (String key : p.keySet()) {
            String o = p.get(key);
            for (Param param : q) {
                if (key.equals(param.getName())
                        || key.indexOf(param.getName()) != -1) {
                    if (param instanceof DatasetParam) {
                        logger.info("Working on a DatasetParam");
                        try {
                            String sig = (String) p.get("signature");
                            String compId = sig + ":" + o.toString();
                            compId = convertDatasetId2DatasetChecksum(compId);
                            o = compId;
                            logger.info("full input ======== " + compId);
                            ret.put(param.getName(), o);
                        } catch (Exception e) {
                            logger.info(e);
                        }
                    } else if (param instanceof AbstractEnumParam) {
                        String valList = (String) o;

                        // Code to specificly work around a specific problem
                        // created by the OrthologPattern Question
                        if (param.getName().equalsIgnoreCase(
                                "phyletic_indent_map")) valList = "Archaea";
                        if (param.getName().equalsIgnoreCase(
                                "phyletic_term_map")) valList = "rno";
                        // end workaround

                        String[] vals;
                        Boolean multipick = ((AbstractEnumParam) param).getMultiPick();
                        if (multipick) {
                            vals = valList.split(",");
                        } else {
                            vals = new String[1];
                            vals[0] = valList;
                        }
                        String newVals = "";
                        for (String mystring : vals) {
                            try {
                                logger.info("ParamName = " + param.getName()
                                        + " ------ Value = " + mystring);
                                if (validateSingleValues(
                                        (AbstractEnumParam) param,
                                        mystring.trim())) {
                                    // ret.put(param.getName(), o);
                                    newVals = newVals + "," + mystring.trim();
                                    logger.info("validated-------------\n ParamName = "
                                            + param.getName()
                                            + " ------ Value = " + mystring);
                                }
                            } catch (Exception e) {
                                logger.info(e);
                            }
                        }

                        if (newVals.length() != 0) newVals = newVals.substring(1);
                        else newVals = "\u0000";
                        logger.info("validated values string -------------"
                                + newVals);
                        ret.put(param.getName(), newVals);
                    } else {
                        ret.put(param.getName(), o);
                    }
                }
            }
        }
        return ret;
    }

    // private String results2String(ResultList result) throws WdkModelException
    // {
    //
    // StringBuffer sb = new StringBuffer();
    // result.write(sb);
    // return sb.toString();
    //
    // }

    private String[][] results2StringArray(Column[] cols, ResultList result)
            throws WdkModelException {
        List<String[]> rows = new LinkedList<String[]>();
        while (result.next()) {
            String[] values = new String[cols.length];
            for (int z = 0; z < cols.length; z++) {
                Object obj = result.get(cols[z].getName());
                String val = null;
                if (obj instanceof String) val = (String) obj;
                else if (obj instanceof char[]) val = new String((char[]) obj);
                else if (obj instanceof byte[]) val = new String((byte[]) obj);
                else val = obj.toString();
                values[z] = val;
            }
            rows.add(values);
        }
        result.close();

        String[][] arr = new String[rows.size()][];
        return rows.toArray(arr);
    }

    // private String[] getColumnsFromQuery(Query q) {
    // Column[] qcols = q.getColumns();
    // String[] ret = new String[qcols.length];
    // int i = 0;
    // for (Column c : qcols) {
    // ret[i] = c.getName();
    // i++;
    // }
    // return ret;
    // }
    //
    // private String[] getParamsFromQuery(Query q) {
    // Param[] qp = q.getParams();
    // String[] ret = new String[qp.length];
    // int i = 0;
    // for (Param p : qp) {
    // ret[i] = p.getName();
    // i++;
    // }
    // return ret;
    // }

    /*
     * private static void loadConfig(String mName, String GH)throws IOException
     * {
     * 
     * //model Name and path for xml files will be read from config file String
     * modelName = mName; String GUS_HOME = GH;
     * 
     * //config file where to retrieve above info //String path =
     * "/usr/local/apache-tomcat-5.5.15/webapps/axis/WEB-INF/wsf-config/";
     * //String fileprop = path + "wdkqueryplugin.prop";
     * 
     * //BufferedReader in = new BufferedReader(new FileReader(fileprop));
     * //while ( modelName.compareTo(mName) != 0 ) { // modelName =
     * in.readLine(); // GUS_HOME = in.readLine(); // if (
     * modelName.compareTo("END") == 0 ) break; //} //in.close();
     * 
     * 
     * m_modelFile = new File(GUS_HOME+"/config/"+modelName+".xml");
     * m_modelPropFile = new File(GUS_HOME+"/config/"+modelName+".prop");
     * m_configFile = new File(GUS_HOME+"/config/"+modelName+"-config.xml");
     * m_schemaFile = new File(GUS_HOME+"/lib/rng/wdkModel.rng"); //added
     * Jun26,2006 m_xmlSchemaFile = new File(GUS_HOME+"/lib/rng/xmlAnswer.rng");
     * 
     * }//end loadConfig
     */

    /*
     * private WdkModelBean loadModel() { //throws MalformedURLException,
     * WdkModelException {logger.info(
     * "_______________________________________________________________________"
     * ); WdkModel wdkModel = null;logger.info(
     * "_______________________________________________________________________"
     * ); try{ //CheckFiles(); // wdkModel = ModelXmlParser.parseXmlFile( //
     * m_modelFile.toURL(), m_modelPropFile.toURL(), m_schemaFile.toURL(), //
     * m_xmlSchemaFile.toURL(), m_configFile.toURL()); }catch(WdkModelException
     * e){logger.info("ERROR ERROR : -------" + e.toString());}
     * catch(MalformedURLException e){logger.info("ERROR ERROR : -------" +
     * e.toString());}logger.info(
     * "_______________________________________________________________________"
     * ); if(wdkModel != null ) logger.info("Model is not Null!!! it is " +
     * wdkModel.getName()); WdkModelBean model = new WdkModelBean(wdkModel);
     * logger.info("---------Model Loading Completed-----------"); return model;
     * 
     * }//end of loadmodel
     */
    private void initial() {
        synchronized (lock) {
            if (modelName2Model == null) {
                modelName2Model = new HashMap<String, WdkModelBean>();
                int i = 0;
                for (String modelName : modelNames) { // Start the Model
                    // FileName Loop
                    logger.info("===================ModelName = " + modelName);
                    try {
                        // logger.info("------------intial---------------");
                        logger.info("===================GUS_HOME = "
                                + gus_homes[i]);
                        // loadConfig(modelName, gus_homes[i]);
                        // logger.info("------------Config
                        // Loaded---------------");
                        // logger.info(m_modelFile.toURL().toString()+"\n"+m_modelPropFile.toURL().toString()+"\n"+m_configFile.toURL().toString()+"\n"+m_schemaFile.toURL().toString()+"\n"+m_xmlSchemaFile.toURL().toString());

                        ModelXmlParser parser = new ModelXmlParser(gus_homes[i]);
                        WdkModel myModel = parser.parseModel(modelName);
                        WdkModelBean mb = new WdkModelBean(myModel);
                        logger.info("===================Model Loaded Was  "
                                + mb.getModel().getProjectId());
                        modelName2Model.put(siteNames[i], mb);
                        // logger.info("------------Model
                        // Loaded----------------");
                    } catch (Exception ex) {
                        logger.info("ERROR : " + ex.toString());
                    }
                    i++;
                }// End the Model FileName Loop

            }
        }
    }

    // private void CheckFiles() {
    // logger.info("-----------------Checking the Files fro Read
    // Permissions---------------");
    // if (!m_modelFile.canRead()) {
    // logger.info(m_modelFile + " Cannot be Read!!!");
    // }
    // if (!m_modelPropFile.canRead()) {
    // logger.info(m_modelPropFile + " Cannot be Read!!!");
    // }
    // if (!m_configFile.canRead()) {
    // logger.info(m_configFile + " Cannot be Read!!!");
    // }
    // if (!m_schemaFile.canRead()) {
    // logger.info(m_schemaFile + " Cannot be Read!!!");
    // }
    // if (!m_xmlSchemaFile.canRead()) {
    // logger.info(m_xmlSchemaFile + " Cannot be Read!!!");
    // }
    // logger.info("------------DONE-------------");
    // }

    private boolean validateSingleValues(AbstractEnumParam p, String value)
            throws WdkModelException, NoSuchAlgorithmException, SQLException,
            JSONException, WdkUserException {
        String[] conVocab = p.getVocab();
        // initVocabMap();
        for (String v : conVocab) {
            if (value.equalsIgnoreCase(v)) return true;
        }
        return false;
    }

    private String[][] handleEnumParameters(Param p, String[] ordCols) throws WdkModelException,
            NoSuchAlgorithmException, SQLException, JSONException,
            WdkUserException {
        logger.info("Function to Handle a Enum Parameter in WdkQueryPlugin");
        EnumParam eParam = (EnumParam) p;
        Map<String, String> termDisp = eParam.getDisplayMap();
        Set<String> terms = termDisp.keySet();
		int tI = 0;
		int iI = 0;
		int i = 0;
		for(String c : ordCols){
			logger.info("current Column = " + c + " ,,,, i = " + i);
			if(c.equals("term")){
			 	tI = i;
			} else if(c.equals("internal") || c.equals("display")){
				iI = i;
			}
			i++;
		}
		logger.info("OrderedColumns.length = " + ordCols.length + ", term = " + tI + ", Internal = " + iI);
        String[][] ePValues = new String[terms.size()][ordCols.length];
        int index = 0;
        for (String term : terms) {
			for(int j=0;j<ePValues[index].length;j++)
				ePValues[index][j] = "N/A";
            String disp = termDisp.get(term);
            ePValues[index][tI] = term;
            ePValues[index][iI] = disp;
            logger.info("Term = " + term + ",     Display = " + disp);
            index++;
        }
        return ePValues;
    }
}
