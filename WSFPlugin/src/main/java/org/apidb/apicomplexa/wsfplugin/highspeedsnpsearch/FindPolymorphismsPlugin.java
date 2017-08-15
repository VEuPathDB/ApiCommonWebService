package org.apidb.apicomplexa.wsfplugin.highspeedsnpsearch;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.gusdb.wsf.plugin.PluginModelException;
import org.gusdb.wsf.plugin.PluginUserException;

/**
 * @author steve
 */
public class FindPolymorphismsPlugin extends FindPolymorphismsAbstractPlugin {

    @SuppressWarnings("unused")
     private static final Logger logger = Logger.getLogger(FindPolymorphismsPlugin.class);
    
    // required parameter definition

    // required result column definition
    public static final String COLUMN_PERCENT_OF_POLYMORPHISMS = "PercentMinorAlleles";
    public static final String COLUMN_PERCENT_OF_KNOWNS = "PercentIsolateCalls";
    public static final String COLUMN_PHENOTYPE = "Phenotype";
    
    @SuppressWarnings("unused")
        private static final String JOBS_DIR_PREFIX = "hsssFindPolymorphisms.";
    
    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#getColumns()
     */

    private static final String propertyFile = "highSpeedSnpSearch-config.xml";

  public FindPolymorphismsPlugin() {
      super(propertyFile);
  }

  @Override
      protected String getPARAM_STRAIN_LIST() {
      return "ngsSnp_strain_meta";
  }


    @Override
        protected String getJobsDirPrefix() {
        return "hsssFindPolymorphisms.";
    }

 @Override
     protected List<String> makeCommandToCreateBashScript(File jobDir, Map<String, String> params,
                                                          File organismDir) throws PluginUserException, PluginModelException {

        List<String> command = super.makeCommandToCreateBashScript(jobDir, params,organismDir);
        String suffix = "NULL";
        command.add(suffix);
        
        return command;
    }    

    /**
     * @throws WsfPluginException
     * @throws PluginModelException
     */

 
}
