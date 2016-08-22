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
public class FindChipPolymorphismsPlugin extends FindPolymorphismsAbstractPlugin {

    private static final Logger logger = Logger.getLogger(FindChipPolymorphismsPlugin.class);
    
    // required parameter definition
    public static final String PARAM_ASSAY_TYPE = "snp_assay_type";

    // required result column definition
    public static final String COLUMN_PERCENT_OF_POLYMORPHISMS = "PercentMinorAlleles";
    public static final String COLUMN_PERCENT_OF_KNOWNS = "PercentIsolateCalls";
    public static final String COLUMN_PHENOTYPE = "Phenotype";

    @SuppressWarnings("unused")
    private static final String JOBS_DIR_PREFIX = "hsssFindChipPolymorphisms.";
    
    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#getColumns()
     */

    private static final String propertyFile = "highSpeedChipSnpSearch-config.xml";
    
    public FindChipPolymorphismsPlugin() {
    super(propertyFile);
  }

  @Override
        protected String getSearchDir() {
        return  "/highSpeedChipSnpSearch";
  }

  @Override
      protected String getPARAM_STRAIN_LIST() {
      return "ref_samples_filter_metadata";
  }

  @Override
  protected String getReconstructCmdName() {
     return "hsssReconstructChipSnpId";
  }

    @Override
        protected String getJobsDirPrefix() {
        return "hsssChipFindPolymorphisms.";
    }

    @Override
        protected String getResultsFileBaseName() {
        return "results";
    }

    @Override
     protected List<String> makeCommandToCreateBashScript(File jobDir, Map<String, String> params,
                                                          File organismDir) throws PluginUserException, PluginModelException {

        List<String> command = super.makeCommandToCreateBashScript(jobDir, params,organismDir);
        String type = params.get(PARAM_ASSAY_TYPE);
        logger.info("PARAM_ASSAY_TYPE = \"" + type + "\""); 
        String suffix = type.replace("Broad_",".");
        command.add(suffix);       
        logger.info("running command " + command.toString() + " from CHip Polymorph"); 
        return command;
    }
    

    /**
     * @throws WsfPluginException
     * @throws PluginModelException
     */

 
}
