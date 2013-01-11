/**
 *Version 2.0.0 --
 * Updated to work with the new Wdk Model.  The loading subroutine was updated to call parse() correctly for the new code in teh WDK
 * 2/27/2008 -- Removed valiadtion of the columns and inserted code to insert "N/A" into the result is a column does not exists on a component site
 * 6/2/2010  -- if a querySet such as SharedVQ is received, and not found, the site will check sharedParams paramSet.
 *              This will allow to access enums from a flatvocab in portal (only way to access a site) --cris
 */
package org.apidb.apicomplexa.wsfplugin.wdkquery;

import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.gusdb.wdk.model.ModelXmlParser;
import org.gusdb.wdk.model.Utilities;
import org.gusdb.wdk.model.WdkModel;
import org.gusdb.wdk.model.WdkModelException;
import org.gusdb.wdk.model.WdkUserException;
import org.gusdb.wdk.model.dbms.ResultList;
import org.gusdb.wdk.model.jspwrap.EnumParamBean;
import org.gusdb.wdk.model.jspwrap.WdkModelBean;
import org.gusdb.wdk.model.query.Column;
import org.gusdb.wdk.model.query.ProcessQuery;
import org.gusdb.wdk.model.query.ProcessQueryInstance;
import org.gusdb.wdk.model.query.Query;
import org.gusdb.wdk.model.query.SqlQuery;
import org.gusdb.wdk.model.query.SqlQueryInstance;
import org.gusdb.wdk.model.query.param.AbstractEnumParam;
import org.gusdb.wdk.model.query.param.FlatVocabParam;
import org.gusdb.wdk.model.query.param.Param;
import org.gusdb.wdk.model.query.param.StringParam;
import org.gusdb.wdk.model.question.Question;
import org.gusdb.wdk.model.user.User;
import org.gusdb.wsf.plugin.AbstractPlugin;
import org.gusdb.wsf.plugin.WsfRequest;
import org.gusdb.wsf.plugin.WsfResponse;
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
public class WdkQueryPlugin extends AbstractPlugin {

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
    private WdkModel wdkModel = null;
    // private static File m_modelFile = null;
    // private static File m_modelPropFile = null;
    // private static File m_schemaFile = null;
    // private static File m_configFile = null;
    // private static File m_xmlSchemaFile = null;
    private  String[] modelNames;
    private  String[] gus_homes;
    private  String[] siteNames;
    private static Map<String, WdkModelBean> modelName2Model = null;
    private static Object lock = new Object();

    public WdkQueryPlugin() throws WsfServiceException {
        super(PROPERTY_FILE);
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
        // do nothing in this plugin
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
     */
    public WsfResponse execute(WsfRequest request) throws WsfServiceException {

        logger.info("WdkQueryPlugin Version : " + WdkQueryPlugin.VERSION);
        // logger.info("Invoking WdkQueryPlugin......");
        String[][] componentResults = null;
        int resultSize = 1;
        ResultList results = null;
        Map<String, String> params = request.getParams();
        Map<String, String> context = request.getContext();

        // when running a param search, the question should hold the question
        // that has the query to which the param belongs; the param should hold
        // the flatVocab param name; the query is the name of the vocab query.

        // when running a id search, the question holds the name of the search,
        // the param should be empty, and the query holds the name of the id
        // query.
        String questionName = context.get(Utilities.QUERY_CTX_QUESTION);
        String paramName = context.get(Utilities.QUERY_CTX_PARAM);
        String queryName = context.get(Utilities.QUERY_CTX_QUERY);

        if (params.containsKey("Query")) params.remove("Query");
        String siteModel = params.remove(SITE_MODEL);
        wdkModel = modelName2Model.get(siteModel).getModel();
        // logger.info("QueryName = "+ invokeKey);

        // Map<String,Object>SOParams = convertParams(params);
        // logger.info("Parameters were processed");

        logger.debug("context question: '" + questionName + "', param: '"
                + paramName + "', query: '" + queryName + "'");

        // Variable to maintain the order of columns in the result... maintains
        // order given by Federation Plugin
        String[] orderedColumns = request.getOrderedColumns();
        Integer[] colindicies = new Integer[orderedColumns.length];
        try {
            if (paramName != null) {
                // get param
                Param param;
                if (questionName != null) {
                    // context question is defined, should get the param from
                    // question
                    Question question = wdkModel.getQuestion(questionName);
                    String partName = paramName.substring(paramName.indexOf(".") + 1);
                    param = question.getParamMap().get(partName);

                    logger.debug("got param from question: " + (param != null));

                    // param doesn't exist in the context question, try to get
                    // it from model.
                    if (param == null)
                    // param = (Param) wdkModel.resolveReference(paramName);
                        throw new WdkModelException("parameter " + paramName
                                + " does not exist in question " + questionName);
                } else {
                    logger.debug("got param from model.");

                    // context question is not defined, get original param from
                    // model.
                    param = (Param) wdkModel.resolveReference(paramName);
                }
                logger.debug("Parameter found : " + param.getFullName());
                if (param instanceof FlatVocabParam)
                    logger.debug("param query: "
                            + ((FlatVocabParam) param).getQuery().getFullName());

                String[][] paramValues;
                if (param instanceof AbstractEnumParam) {
                    paramValues = handleVocabParams((AbstractEnumParam) param,
                            params, orderedColumns);
                } else { // for other record types, return empty array
                    paramValues = new String[0][0];
                }
                WsfResponse wsfResponse = new WsfResponse();
                wsfResponse.setResult(paramValues);
                return wsfResponse;

            }

            // check if question is set
            Query query;
            if (questionName != null) {
                Question question = wdkModel.getQuestion(questionName);
                query = question.getQuery();
            } else {
                query = (Query) wdkModel.resolveReference(queryName);
            }

            // get the user
            String userSignature = context.get(Utilities.QUERY_CTX_USER);
            User user = wdkModel.getUserFactory().getUser(userSignature);

            // converting from internal values to dependent values
            Map<String, String> SOParams = convertParams(user, params,
                    query.getParams());// getParamsFromQuery(q));

            // validateQueryParams(params,q);
            // logger.info("Parameters Validated...");
            // validateQueryColumns(orderedColumns,q);
            // logger.info("Columns Validated...");

            // Get the indicies of the correct columns for the component Query

            int i = 0;
            for (String oCol : orderedColumns) {
                int value = findColumnIndex(query, oCol);
                Integer iValue = new Integer(value);
                colindicies[i] = iValue;
                i++;
            }

            // WS Query processing
            if (query instanceof ProcessQuery) {
                logger.info("Processing WSQuery ...");
                ProcessQuery wsquery = (ProcessQuery) query;
                // assign the weight to 0 here, since the assigned weight will
                // be applied on the portal when it's being cached.
                ProcessQueryInstance wsqi = (ProcessQueryInstance) wsquery.makeInstance(
                        user, SOParams, true, 0, context);
                results = wsqi.getResults();
            }
            // SQL Query Processing
            else {
                logger.info("Process SqlQuery ...");
                SqlQuery sqlquery = (SqlQuery) query;
                // assign the weight to 0 here, since the assigned weight will
                // be applied on the portal when it's being cached.
                SqlQueryInstance sqlqi = (SqlQueryInstance) sqlquery.makeInstance(
                        user, SOParams, true, 0, context);
                results = sqlqi.getResults();
            }
            logger.info("Results set was filled");
            componentResults = results2StringArray(query.getColumns(), results);
            logger.info("Results have been processed.... "
                    + componentResults.length);

        }
        catch (WdkModelException ex) {
            logger.info("WdkMODELexception in execute()" + ex.toString());
            // String msg = ex.toString();
            String msg = ex.formatErrors();
            logger.info("Message = " + msg);
            // if(msg.matches("Invalid value"){}
            if (msg.indexOf("Please choose value(s) for parameter") != -1) {
                resultSize = 0;
            } else if (msg.contains("No value supplied for param")) {
                resultSize = 0;
                // isolate query on crypto/plasmo with only param values for
                // plasmo
            } else if (msg.contains("does not exist")) {
                resultSize = 0;
            } else if (msg.indexOf("does not contain") != -1) {
                resultSize = -2; // query set or query doesn't exist
								//		} else if (msg.indexOf("encountered an invalid term") != -1) {
								//    resultSize = 0; // parameter value relates to a different comp site
            } else if (msg.indexOf("does not include") != -1) {
                resultSize = -2; // query set or query doesn't exist
            } else if (msg.contains("datasets value '' has an error: Missing the value")) {
                resultSize = 0;
            } else if (msg.contains("Invalid term")) {
                resultSize = 0;
            } else {
                ex.printStackTrace();
                resultSize = -1; // actual error, can't handle
            }
				} catch(WdkUserException ex){
						logger.info("WdkUSERexception IN execute()" + ex.toString());
						String msg = ex.formatErrors();
            logger.info("Message = " + msg);
						if (msg.indexOf("encountered an invalid term") != -1) {
								   resultSize = 0; // parameter value relates to a different comp site
						}
						else {
                ex.printStackTrace();
                resultSize = -1; // actual error, can't handle
						}

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

        WsfResponse wsfResult = new WsfResponse();
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
     * @throws WdkModelException 
     */

    // private String convertDatasetId2DatasetChecksum(String sig_id)
    // throws Exception {
    // String[] parts = sig_id.split(":");
    // String sig = parts[0];
    // String id = parts[1];
    // UserFactory userfactory = model.getModel().getUserFactory();
    // User user = userfactory.getUser(sig);
    // Integer datasetId = new Integer(id);
    // DatasetFactory datasetFactory = model.getModel().getDatasetFactory();
    // Connection connection =
    // model.getModel().getUserPlatform().getDataSource().getConnection();
    // try {
    // int userDatasetId = datasetFactory.getUserDatasetId(connection,
    // user, datasetId);
    // return Integer.toString(userDatasetId);
    // } finally {
    // connection.close();
    // }
    // }

    private Map<String, String> convertParams(User user, Map<String, String> p,
            Param[] q) throws WdkModelException {
        Map<String, String> ret = new HashMap<String, String>();
        for (String key : p.keySet()) {
            String value = p.get(key);
            for (Param param : q) {
                if (key.equals(param.getName())
                        || key.indexOf(param.getName()) != -1) {
                    // Jerric - no longer need to convert it with the
                    // noTranslation flag to true.

                    // if (param instanceof DatasetParam) {
                    // logger.info("Working on a DatasetParam");
                    // try {
                    // String compId = user.getSignature() + ":" + o;
                    // compId = convertDatasetId2DatasetChecksum(compId);
                    // o = compId;
                    // logger.info("full input ======== " + compId);
                    // ret.put(param.getName(), compId);
                    // } catch (Exception e) {
                    // logger.info(e);
                    // }
                    // } else
                    if (param instanceof AbstractEnumParam) {
                        String valList = (String) value;
                        AbstractEnumParam abParam = (AbstractEnumParam) param;
                        Param dependedParam = abParam.getDependedParam();
                        String dependedParamValue = (dependedParam == null ? null : p.get(dependedParam.getName()));
                        EnumParamBean abParamBean = new EnumParamBean(abParam);
                        abParamBean.setDependedValue(dependedParamValue);
                        if ((param instanceof FlatVocabParam || param.isAllowEmpty())
                                && valList.length() == 0) {
                            try {
                                valList = abParamBean.getDefault();
                            }
                            catch (Exception e) {
                                logger.info("error using default value.");
                            }
                        }
                        
                        // Code to specifically work around a specific problem
                        // created by the OrthologPattern Question
                        if (param.getName().equalsIgnoreCase(
                                "phyletic_indent_map")) valList = "ARCH";
                        if (param.getName().equalsIgnoreCase(
                                "phyletic_term_map")) valList = "rnor";
                        // end workaround

                        String[] vals;
                        Boolean multipick = abParamBean.getMultiPick();
                        if (multipick) {
                            vals = valList.split(",");
                        } else {
                            vals = new String[1];
                            vals[0] = valList;
                        }
                        String newVals = "";
                        for (String mystring : vals) {
                            // unescape each individual term.
                            mystring = unescapeValue(mystring,
                                    abParamBean.getQuote());
                            try {
                                logger.info("ParamName = " + param.getName()
                                        + " ------ Value = " + mystring);
                                if (validateSingleValues(abParamBean,
                                        mystring.trim())) {
                                    // ret.put(param.getName(), o);
                                    newVals = newVals + "," + mystring.trim();
                                    logger.info("validated-------------\n ParamName = "
                                            + param.getName()
                                            + " ------ Value = " + mystring);
                                } else {
                                    logger.warn("param validation failed: "
                                            + "param=" + param.getName()
                                            + ", value='" + mystring + "'");
                                }
                            }
                            catch (Exception e) {
                                logger.info(e);
                            }
                        }

                        if (newVals.length() != 0) newVals = newVals.substring(1);
                        else newVals = "\u0000";
                        logger.info("validated values string -------------"
                                + newVals);
                        value = newVals;
                    } else { // other types, unescape the whole thing
                        boolean quoted = true;
                        if (param instanceof StringParam)
                            quoted = !((StringParam) param).isNumber();
                        value = unescapeValue(value, quoted);
                    }
                    ret.put(param.getName(), value);
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
                if (obj == null) val = "N/A";
                else if (obj instanceof String) val = (String) obj;
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
                    }
                    catch (Exception ex) {
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

    private boolean validateSingleValues(EnumParamBean p, String value)
            throws WdkModelException, NoSuchAlgorithmException, SQLException,
            JSONException, WdkUserException {
        String[] conVocab = p.getVocab();
        logger.info("conVocab.length = " + conVocab.length);
        if (p.isSkipValidation()) return true;
        // initVocabMap();
        for (String v : conVocab) {
            logger.info("value: " + value + " | vocabTerm: " + v);
            if (value.equalsIgnoreCase(v)) return true;
        }
        return false;
    }

    private String[][] handleVocabParams(AbstractEnumParam vocabParam,
            Map<String, String> ps, String[] ordCols) throws WdkModelException,
            NoSuchAlgorithmException, SQLException, JSONException,
            WdkUserException {
        logger.debug("Function to Handle a vocab param in WdkQueryPlugin: "
                + vocabParam.getFullName());
        EnumParamBean paramBean = new EnumParamBean(vocabParam);
        // set depended value if needed
        Param dependedParam = vocabParam.getDependedParam();
        if (dependedParam != null) {
            String dependedValue = ps.get(dependedParam.getName());
            paramBean.setDependedValue(dependedValue);
            logger.debug(dependedParam.getName() + " ==== " + dependedValue);
        }
        Map<String, String> displayMap = paramBean.getDisplayMap();
        Map<String, String> parentMap = paramBean.getParentMap();
        int termColumnIndex = -1;
        int displayColumnIndex = -1;
        int internalColumnIndex = -1;
        int parentColumnIndex = -1;
        for (int i = 0; i < ordCols.length; i++) {
            String column = ordCols[i];
            logger.debug("current Column = " + column + " ,,,, i = " + i);
            if (column.equals(FlatVocabParam.COLUMN_TERM)) {
                termColumnIndex = i;
            } else if (column.equals(FlatVocabParam.COLUMN_DISPLAY)) {
                displayColumnIndex = i;
            } else if (column.equals(FlatVocabParam.COLUMN_INTERNAL)) {
                internalColumnIndex = i;
            } else if (column.equals(FlatVocabParam.COLUMN_PARENT_TERM)) {
                parentColumnIndex = i;
            }
        }

        logger.debug("OrderedColumns.length = " + ordCols.length + ", term = "
                + termColumnIndex + ", display = " + displayColumnIndex
                + ", internal = " + internalColumnIndex + ", parent = "
                + parentColumnIndex);

        // term & internal has to exist
        if (termColumnIndex < 0 || internalColumnIndex < 0)
            throw new WdkModelException("The wsf call for param "
                    + paramBean.getFullName()
                    + " doesn't specify term & internal columns.");
        String[][] result = new String[displayMap.size()][ordCols.length];
        int index = 0;
        for (String term : displayMap.keySet()) {
            for (int j = 0; j < result[index].length; j++) {
                if (j == termColumnIndex) {
                    result[index][j] = term;
                } else if (j == internalColumnIndex) {
                    // always return term as internal
                    result[index][j] = term;
                } else if (j == displayColumnIndex) {
                    result[index][j] = displayMap.get(term);
                } else if (j == parentColumnIndex) {
                    result[index][j] = parentMap.get(term);
                } else {
                    result[index][j] = "N/A";
                }
            }
            index++;
        }
        return result;
    }

    @Override
    protected String[] defineContextKeys() {
        return null;
    }

    private String unescapeValue(String value, boolean quoted) {
        if (value == null || value.length() == 0) return value;

        // will first remove the wrapping quote
        if (quoted) {
            if (value.charAt(0) == '\'') value = value.substring(1);
            if (value.charAt(value.length() - 1) == '\'')
                value = value.substring(0, value.length() - 1);
        }

        // then replace double single-quotes to a single quote
        value = value.replace("''", "'");

        return value;
    }
}
