package org.apidb.apicomplexa.wsfplugin.wublast;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.gusdb.wsf.plugin.WsfServiceException;
import org.apidb.apicomplexa.wsfplugin.BlastPlugin;

/**
 * 
 */

/**
 * @author Jerric
 * @created Nov 2, 2005
 */


public class WuBlastPlugin extends BlastPlugin {

    private static final String PROPERTY_FILE = "wuBlast-config.xml";

    // column definitions
    public static final String COLUMN_ID = "Identifier";
    public static final String COLUMN_HEADER = "Header";
    public static final String COLUMN_FOOTER = "Footer";
    public static final String COLUMN_ROW = "TabularRow";
    public static final String COLUMN_BLOCK = "Alignment";
    public static final String COLUMN_PROJECT_ID = "ProjectId";

    // required parameter definitions
    public static final String PARAM_ALGORITHM = "BlastAlgorithm";
    public static final String PARAM_SEQUENCE = "BlastQuerySequence";
    //  public static final String PARAM_QUERY_TYPE = "BlastQueryType";
    public static final String PARAM_DATABASE_ORGANISM = "BlastDatabaseOrganism";
    public static final String PARAM_DATABASE_TYPE = "BlastDatabaseType";

    // field definitions in the config file
    private static final String FIELD_APP_PATH = "AppPath";
    private static final String FIELD_DATA_PATH = "DataPath";
    private static final String FIELD_TIMEOUT = "Timeout";
    private static final String FIELD_TEMP_PATH = "TempPath";
    private static final String FIELD_USE_PROJECT_ID = "UseProjectId";
    private static final String FIELD_FILE_PATH_PATTERN = "FilePathPattern";

    private static final String FIELD_SOURCE_ID_REGEX_PREFIX = "SourceIdRegex_";
    private static final String FIELD_ORGANISM_REGEX_PREFIX = "OrganismRegex_";
    private static final String FIELD_SCORE_REGEX = "ScoreRegex";

    private static final String URL_MAP_PREFIX = "UrlMap_";
    private static final String FIELD_URL_MAP_OTHER = URL_MAP_PREFIX + "Others_";
    private static final String PROJECT_MAP_PREFIX = "ProjectMap_";
    private static final String FIELD_PROJECT_MAP_OTHER = PROJECT_MAP_PREFIX + "Others_";

    // in BlastPlugin some are protected instead of private so they can be used in a derived class
    private String appPath;
    private String dataPath;
    private String tempPath;
    private long timeout;
    private String filePathPattern;
    private boolean useProjectId;
    private String sourceIdRegex;
    private String organismRegex;
    private String scoreRegex;
    private String urlMapOthers;
    private String projectMapOthers;


    /*
     dbType: BlastDatabaseType: 
     // dbType is used for:(in this order)
     // -- get to know if the hit source id in defline is in second or third position (sourceregex)
     // -- select blast program, depending also on query type (=sequence type:dna or protein)
     // -- build filename: orgParam + dbType (eg., ChominisCDS), to access correct dataset
     // -- insert correct links to correct URLs (depending on dbType to link to different recordClass pages)
     
     
     EXTERNAL name                    {INTERNAL name} 
     (as defined in model.xml) 
     --external name is displayed in question page parameter
     --internal name is used here, to generate filenames, link to appropiate recordclasses, etc
     
     Transcripts                       {Transcripts}                
     Translated Transcripts            {Transcripts Translated}     
     
     Protein                           {Proteins}                   
     ORF (Protein)                     {ORF}                        
     
     EST                               {EST}                        
     Translated EST                    {EST Translated}             
     
     Genome                            {Genomics}                   
     Translated Genome                 {Genomics Translated}        
    */

 
    /**
     * @throws WsfServiceException
     * @throws IOException
     * @throws InvalidPropertiesFormatException
     * 
     */
    public WuBlastPlugin() throws WsfServiceException {
        super(PROPERTY_FILE);
    }


    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getRequiredParameters()
     */
    @Override
    protected String[] getRequiredParameterNames() {
        return new String[] {PARAM_ALGORITHM, PARAM_SEQUENCE
			      //, PARAM_DATABASE_ORGANISM in BlastPlugin, we have it (like dbType) in validateParameters()
			      //  because we have BlastDatabaseTypeGene, BlastDatabaseTypeGenome, etc 
			      //  with diff vocab for Genes, Genomic Sequences, ESTs, etc
	};
    }


    //-------------------------------------------------------------------------
    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
     */
    @Override
    protected String[][] execute(String invokeKey, Map<String, String> params,
            String[] orderedColumns) throws WsfServiceException {

	String pluginName = getClass().getSimpleName();
        logger.info("\n\nWB execute():  Invoking " + pluginName + "...");

	File seqFile = null;
        File outFile = null;
        try {
            // create temporary files for input sequence and output report
	    File dir = new File(tempPath);
	    seqFile = File.createTempFile(pluginName + "_", "in",dir);
	    outFile = File.createTempFile(pluginName + "_", "out",dir);

	    // get database type parameter
	    String dbType = null;
	    String dbTypeName = null;
            for (String paramName : params.keySet()) {
                if (paramName.startsWith(PARAM_DATABASE_TYPE)) {
                    dbTypeName = paramName;
                    dbType = params.get(paramName);
                    break;
                }
            }
            params.remove(dbTypeName);
	    logger.info("\n\nWB execute(): dbType is: " + dbType + "\n\n");


	    // Get regex for source id in defline (second or third position depending on dbtype)
	    // BlastPlugin  assumes every possible value of dbType (included Translated) listed in config file
	    // We limit the choices in config file to TWO: 
	    //    Transcripts in third position, or Genomics in second position
	    String suffix;
	    if ( dbType.contains("Genomics") || dbType.contains("EST") ) suffix="Genomics"; 
	    else suffix="Transcripts";
	    if ( dbType.contains("ORF")) suffix="Transcripts";

	    // BlastPlugin uses dbType instead
	    organismRegex = getProperty(FIELD_ORGANISM_REGEX_PREFIX + suffix);

            urlMapOthers = getProperty(FIELD_URL_MAP_OTHER + suffix);
	    //logger.info("\n\nWB execute(): urlMapOthers is: " + urlMapOthers + "\n\n");

            projectMapOthers = getProperty(FIELD_PROJECT_MAP_OTHER);
	    //logger.info("\n\nWB execute(): projectMapOthers is: " + projectMapOthers + "\n\n");

	    sourceIdRegex = getProperty(FIELD_SOURCE_ID_REGEX_PREFIX + suffix);

            dataPath = getProperty(FIELD_DATA_PATH); 

	    // get sequence
            String seq = params.get(PARAM_SEQUENCE);
            params.remove(PARAM_SEQUENCE);

            // write the sequence into the temporary fasta file,
            // do not reformat the sequence - easy to introduce problem
            PrintWriter out = new PrintWriter(new FileWriter(seqFile));
            if (!seq.startsWith(">")) out.println(">MySeq1");
            out.println(seq);
            out.flush();
            out.close();


	  

            // prepare the arguments
            String[] command = prepareParameters(params, seqFile, outFile, dbType);
	    logger.info("WB execute(): Command prepared: " + printArray(command));
  
            // invoke the command
            String output = invokeCommand(command, timeout);

	    // exitValue is int, defined in WsfPlugin.java
	    // we want to show the stderr to the user (not in BlastPLugin)
	    /*
            if (exitValue != 0)
                throw new WsfServiceException("\nThe invocation is failed: "
                        + output);
	    */


	    // if the invocation succeeds, prepare the result; otherwise,
            // prepare results for failure scenario
            logger.info("WB execute(): calling prepareResult()\n\n");
            String[][] result = prepareResult(orderedColumns, outFile, dbType);
	    logger.info("\n\n\nWB execute(): result RETURNED!!!\n\n");

	    // insertBookmark(result, orderedColumns);   in BlastPlugin
	    //   we insert the bookmark in prepareResult
            logger.debug(printArray(result));
            return result;
        } catch (IOException ex) {
            logger.error("\n\nWB execute() ERROR preparing the result:" + ex);
            throw new WsfServiceException(ex);
        } 
	
	/*
	finally {
            if (seqFile != null) seqFile.delete();
            if (outFile != null) outFile.delete();
        }
	*/
	//remove the finally part if you want to have access to the temporary files

    }


    //-------------------------------------------------------------------------
    protected String[] prepareParameters(Map<String, String> params, File seqFile,
            File outFile, String dbType) throws IOException,
            WsfServiceException {

	Vector<String> cmds = new Vector<String>();

	//	String qType = params.get(PARAM_QUERY_TYPE);
	//	params.remove(PARAM_QUERY_TYPE);
	
	String dbOrgs = null;
	String dbOrgName = null;
	for (String paramName : params.keySet()) {
	    if (paramName.startsWith(PARAM_DATABASE_ORGANISM)) {
		dbOrgName = paramName;
		dbOrgs = params.get(paramName);
		break;
	    }
	}
	params.remove(dbOrgName);
	
	String blastApp = params.get(PARAM_ALGORITHM);
	params.remove(PARAM_ALGORITHM);
	//    String blastApp = getBlastProgram(qType, dbType);

	// so database name is built correctly for the Translated cases
	if (dbType.contains("Translated")) {
            if ( dbType.contains("Transcripts") ) dbType="Transcripts";
	    else if ( dbType.contains("Genomics") ) dbType="Genomics";
	    else if (dbType.contains("EST")) dbType="EST";
	}

        // now prepare the commandline
	cmds.add(appPath + "/" + blastApp);

	String blastDbs = getBlastDatabase(dbType, dbOrgs);
	//logger.info("\n\nWB prepareParameters(): FULL DATAPATH is: " + blastDbs + "\n");

        cmds.add(blastDbs);
	cmds.add(seqFile.getAbsolutePath());
	cmds.add("O=" + outFile.getAbsolutePath());

	for (String param : params.keySet()) {
	    if( !( param.contains("filter") && params.get(param).equals("no") ) ) {
		if( param.contains("filter") && params.get(param).equals("yes") )
		    params.put(param, "seg");
		cmds.add(param);
		cmds.add(params.get(param));
	    }

	}
	logger.info("\n\nWB prepareParameters(): " + blastDbs + " inferred from (" + dbType + ", '" + dbOrgs + "')");
        logger.info("\n\nWB prepareParameters(): " + blastApp);// + " inferred from (" + qType + ", " + dbType  + ")");
        String[] cmdArray = new String[cmds.size()];
        cmds.toArray(cmdArray);
        return cmdArray;

	
    }


    //-------------------------------------------------------------------------
    // it used to call insertLinkToBlock(),  now it uses insertBookmark() --called in execute()
    // it used to call insertUrl(),  now it uses insertIdUrl() --needs to be fixed to add projectId
    // it used to call extractField(), now it uses FindField() 

    protected String[][] prepareResult(String[] orderedColumns, File outFile, String dbType)
            throws IOException {

	//so database name is built correctly for the Translated cases
	if (dbType.contains("Translated")) {
            if ( dbType.contains("Transcripts") ) dbType="Transcripts";
	    else if ( dbType.contains("Genomics") ) dbType="Genomics";
	    else if (dbType.contains("EST")) dbType="EST";
	}

        // create a map of <column/position>
        Map<String, Integer> columns = new HashMap<String, Integer>(
                orderedColumns.length);
        for (int i = 0; i < orderedColumns.length; i++) {
            columns.put(orderedColumns[i], i);
        }



        // open output file, and read it
	// Header; if there is a  WARNING before the line "Sequences", it will be added to the header
        String line;
        BufferedReader in = new BufferedReader(new FileReader(outFile));
        StringBuffer header = new StringBuffer();
        do {
            line = in.readLine();
            if (line == null)
                throw new IOException("Invalid BLAST output format");
            header.append(line + newline);
	    logger.debug("\nWB prepareResult(): HEADER: " + line + "\n");
        } while (   (!line.startsWith("Sequence")) &&  (!line.startsWith("FATAL"))    );

	// show stderr
	if(line.startsWith("FATAL")) {
	    line = in.readLine();
	    header.append(line + newline);
	    this.message = header.toString();
	    return new String[0][columns.size()];
	}


        // Tabular Rows; which starts after the ONE empty line
        line = in.readLine(); // skip an empty line

	// Why is rows a Map instead of a simple String[]?
        Map<String, String> rows = new HashMap<String, String>();

	// wublast truncates deflines in tabular lines, that is why in ORF fasta files the source ids are sometimes truncated
	// we introduce these variables to link score to correct alignment block, without id (with a counter instead)
	Integer counter = 0;
	String counterstring;

	// Loop on Tabular Rows
        while ((line = in.readLine()) != null) {
	    //logger.info("\nWB prepareResult() Unless no hits, this should be a tabular row line: " + line + "\n");


            if (line.trim().length() == 0) {
		logger.info("\nWB prepareResult(): Line length 0!!, we finished with tabular rows \n -------------------------\n");
		break;}
	    
	    // insert bookmark: link score to alignment block name=#counter
	    // blastplugin does this at the end of execute() --insertBookmark()
	    //	    line = insertLinkToBlock(line,counter);

	    //logger.info("WB prepareResult(): \nif dbType is not ORF, we insert URL in the line: " + line + "\n");
	    // insert link to gene page, in source_id, only if dbType IS NOT ORF
	    if ( !dbType.contains("ORF")) line = insertUrl(line,dbType);
            logger.info("insertURL Returned Results : " + line);

	    counterstring = counter.toString();
	    rows.put(counterstring, line);
	    counter++;

        }//end while

	// We need to deal with a possible WARNING between tabular rows and alignments: move it to header
        line = in.readLine(); // skip an empty line
	//logger.info("\nWB prepareResult() This line is supposed to be empty or could have a WARNING or keyword NONE: " + line+"\n");
	header.append(newline + line + newline);

	if ( line.indexOf("NONE") >= 0 )    {
	    this.message = header.toString();
	    return new String[0][columns.size()];
	}

	if (line.trim().startsWith("WARNING")) {
	    line = in.readLine(); // get next line
	    //logger.info("\nWB prepareResult() This line is continuation of warning line: " + line+"\n");
            header.append(line + newline);
	    line = in.readLine(); // get next line
	    //logger.info("\nWB prepareResult() This line is supposed to be empty: " + line+"\n");
            header.append(line + newline);
	    line = in.readLine(); // get next line
	    //logger.info("\nWB prepareResult() This line is supposed to be empty: " + line+"\n");
	}


        // Extract alignment blocks
	String hit_organism, hit_projectId = "", hit_sourceId = "";

        List<String[]> blocks = new ArrayList<String[]>();
        StringBuffer block = null;
        String[] alignment = null;
	StringBuffer warnings = new StringBuffer();
	counter = 0;

        while ((line = in.readLine()) != null) {
	    // found a warning before parameters
            if (line.trim().startsWith("WARNING")) {
		
		logger.info("\nWB prepareResult() Found WARNING: " + line + "\n");
		warnings.append(line + newline);
		line = in.readLine(); // get next line
		warnings.append(line + newline);
		line = in.readLine(); // get next line
		warnings.append(line + newline);
		line = in.readLine(); // get next line
		warnings.append(line + newline);
	    }

            // reach the footer part
            if (line.trim().startsWith("Parameters")) {
		logger.info("\nWB prepareResult(): FOOTER (found line Parameters): " + line + "\n");
                // output the last block, if have
                if (alignment != null) {
                    alignment[columns.get(COLUMN_BLOCK)] = block.toString();
                    blocks.add(alignment);
                }
                break;
            }

            // reach a new start of alignment block
            if (line.length() > 0 && line.charAt(0) == '>') {
		logger.info("\n\n\n-----------------\nWB prepareResult() This should be a new block: " + line + "\n");

                // output the previous block, if have
                if (alignment != null) {
                    alignment[columns.get(COLUMN_BLOCK)] = block.toString();
                    blocks.add(alignment);
                }
                // create a new alignment and block
                alignment = new String[orderedColumns.length];
                block = new StringBuffer();

		/*
                // obtain the ID of it, which is the rest of this line
		hit_sourceId = extractField(line,sourceIdRegex);
		//logger.info("\nWB prepareResult() Back from extractField() hit_sourceId : " + hit_sourceId + "\n");
 		hit_organism = extractField(line, organismRegex);
		//logger.info("\nWB prepareResult() Back from extractField(organismRegex): organism: " + hit_organism+"\n");
		*/

		// get source id
		int[] sourceIdPos = findField(line, sourceIdRegex);
		hit_sourceId = line.substring(sourceIdPos[0], sourceIdPos[1]);
		
		// get organism
		int[] organismPos = findField(line, organismRegex);
		hit_organism = line.substring(organismPos[0], organismPos[1]);
		logger.debug("Organism extracted from defline is: " + hit_organism);


		if (useProjectId) {
		    hit_projectId = getProjectId(hit_organism);
		    //logger.info("\nWB prepareResult() Back from getProjectId(): projectId : " + hit_projectId+"\n\n");
		    alignment[columns.get(COLUMN_PROJECT_ID)] = hit_projectId;
		}

		logger.info("WB prepareResult(): alignments: to insert URL in: " + line + "\n");
		// Insert link to gene page, in source_id
		// ncbi plugin does not do this
		line = insertIdUrl(line,dbType);


		// Insert <a name="source_id"></a> in the beginning of the line		
		// line = "<a name=\"" + hit_sourceId + "\"></a>" + line;
		// not neededif we use insertBookmark() in BlastPlugin
		//		line = "<a name=\"" + counter + "\"></a>" + line;
		counter++;
		alignment[columns.get(COLUMN_ID)] = hit_sourceId; 
		
            }
            // add the line to the block
            block.append(line + newline);
	    
        }//end while

        // get the rest as the footer part
        StringBuffer footer = new StringBuffer();
	if ( warnings.length() != 0 ) footer.append( warnings.toString() );
        footer.append(line + newline);
        while ((line = in.readLine()) != null) {
	    if( line.contains("Database") || line.contains("Title") ) {
		String[] singleDbs = line.split(";");
		for (int i = 0; i < singleDbs.length; i++) {
		    footer.append(singleDbs[i] + newline);
		}
	    }
            else footer.append(line + newline);
        }

	logger.info("\n\n\n--------------------------------------------\n\nWB prepareResult(): Information stored in Stringbuffers and tables, now copy info into results[][]\n\n");

        // now reconstruct the result
        int size = Math.max(1, blocks.size());
	//logger.info("\n---------WB prepareResult(): size is of Results is: " + size + "\n");

        String[][] results = new String[size][orderedColumns.length];
        for (int i = 0; i < blocks.size(); i++) {
	    counter = i;
	    counterstring = counter.toString();
            alignment = blocks.get(i);

            // copy ID
            int idIndex = columns.get(COLUMN_ID);
            results[i][idIndex] = alignment[idIndex];

	    if (useProjectId) {
		// copy PROJECT_ID
		int projectIdIndex = columns.get(COLUMN_PROJECT_ID);
		results[i][projectIdIndex] = alignment[projectIdIndex];
	    }
	    //logger.info("\n---------WB prepareResult(): copied Identifier\n");

            // copy block
            int blockIndex = columns.get(COLUMN_BLOCK);
            results[i][blockIndex] = alignment[blockIndex];
	    //logger.info("\nWB prepareResult(): copied block\n");

            // copy tabular row
            int rowIndex = columns.get(COLUMN_ROW);
            for (String id : rows.keySet()) {
		//if (alignment[idIndex].startsWith(id)) {
		if ( id.equalsIgnoreCase(counterstring) ) {
                    results[i][rowIndex] = rows.get(id);
		    //logger.info("\nWB prepareResult(): copied tabular row\n");
                    break;
                }
            }
        }
        // copy the header and footer
	results[0][columns.get(COLUMN_HEADER)] = header.toString();
        results[size - 1][columns.get(COLUMN_FOOTER)] = footer.toString();
	//logger.info("\nWB prepareResult(): copied header and footer\n");	

        return results;
    }



    //----------- PRIVATE METHODS  -----------------------------------------------

    protected String getBlastProgram(String qType, String dbType) throws WsfServiceException {

        String bp = null;
	
        if ("dna".equalsIgnoreCase(qType)) {
            if (dbType.contains("Proteins") || dbType.contains("ORF") ) {
		bp = "blastx";
	    } else if (dbType.contains("Translated")) {
                bp = "tblastx";
	    } else { bp = "blastn"; }
	    
	} else if ("protein".equalsIgnoreCase(qType)) {
            if ( dbType.contains("Proteins") || dbType.contains("ORF") ) {
		bp = "blastp";
            } else { bp = "tblastn"; }
	}

	if (bp != null) return bp;
        else throw new WsfServiceException(
					   "invalid blast query or database types (" + qType + ", "
					   + dbType + ")");
	
    }


    protected String getBlastDatabase(String dbType, String dbOrgs) {
        // the dborgs is a multipick value, containing several organisms,
        // separated by a comma
        String[] organisms = dbOrgs.split(",");

	// for apidb
	String seqType = "n/";
        if (dbType.equals("Proteins") || dbType.equals("ORF")){ seqType = "p/";}
	
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < organisms.length; i++) {
            // construct file path pattern
            String path = filePathPattern.replaceAll("\\$\\$Organism\\$\\$", organisms[i]);
            path = path.replaceAll("\\$\\$DbType\\$\\$", dbType);
	    
	    // for apidb will need to add /p/ or /n/ dependieng on data type
	    //  if (apidb) {   sb.append(dataPath + seqType + "/" + path + " "); }
	    sb.append(dataPath + "/" + path + " "); 
        }
        // sb.append("\"");
        return sb.toString().trim();
    }
    

    // SAME AS IN BLASTPLUGIN
    // organism is the hit_organism, from defline: includes string in first position only until _
    protected String getProjectId(String organism) {

        String mapKey = PROJECT_MAP_PREFIX + organism;
logger.info("\n\nWB getProjectId() mapKey is: *" + mapKey + "*\n");
        String projectId = getProperty(mapKey);
logger.info("\n\nWB getProjectId() projectId is: " + projectId + "\n");
        if (projectId == null) projectId = projectMapOthers;
	//logger.info("\n\nWB getProjectId() if projectId was null, now it is: " + projectId + "\n");
        return projectId;
    }

  
    private String extractField(String defline, String regex) {
	//logger.info("\n\nWB extractField() defline is: " + defline + " and regex is: " + regex + "\n");
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(defline);
        if (matcher.find()) {
	    //logger.info("\n\nWB extractField() FOUND source id!!: " + matcher.group(1) + "\n");
            // the match is located at group 1
            return matcher.group(1);
        } else return null;
    }
    

    private String insertUrl(String defline, String dbType) {
	//logger.info("\n\nWB insertUrl() defline is: " + defline);
	//logger.info("\n\nWB insertUrl() dbType is: " + dbType + "\n");

        // extract organism from the defline
	String hit_sourceId = extractField(defline, sourceIdRegex);
	//logger.info("\n\nWB insertUrl() hit_sourceId : " + hit_sourceId + "\n");
	String hit_organism = extractField(defline, organismRegex);
	//logger.info("\n\nWB insertUrl() hit_organism : " + hit_organism + "\n");

	String hit_projectId = getProjectId(hit_organism);
	//logger.info("\n\nWB insertUrl() hit_projectId : " + hit_projectId + "\n");

	String linkedSourceId = getSourceUrl(hit_organism,hit_projectId,hit_sourceId,dbType);
        logger.info("getSourceId Completed successfully with : " + linkedSourceId);

	// replace the url into the defline
	Pattern pattern = Pattern.compile(sourceIdRegex);
	Matcher matcher = pattern.matcher(defline);
	if (matcher.find()) {
	    // the organism is located at group 1
	    int start = matcher.start(1);
	    int end = matcher.end(1);
	    
	    // insert a link tag into the data
	    StringBuffer sb = new StringBuffer(defline.substring(0, start));
	    sb.append(linkedSourceId);
	    sb.append(defline.substring(end));
	    return sb.toString();
	} else return defline;


    }
    


    private String getSourceUrl(String organism, String projectId, String sourceId, String dbType) {
        String sourceUrl = sourceId + " - (no link)";   
	String mapkey = URL_MAP_PREFIX + organism + "_" + dbType;
        String mapurl = getProperty(mapkey);
        logger.info("\n\nWB getSourceUrl(): mapkey=" + mapkey + ", mapurl=" + mapurl + "\n");

	if (mapurl == null) mapurl = urlMapOthers; // use default url
	logger.info("\n\nWB getSourceUrl() if it was null, mapurl=" + mapurl + "\n");

	mapurl = mapurl.trim().replaceAll("\\$\\$source_id\\$\\$", sourceId);
	logger.info("source_id replaced " + mapurl);
        mapurl = mapurl.trim().replaceAll("\\$\\$project_id\\$\\$", projectId);
        logger.info("project_id replaced " + mapurl);
	sourceUrl = ("<a href=\"" + mapurl + "\" >" + sourceId + "</a>");
	return sourceUrl;
	
    }

    
    private String insertLinkToBlock(String defline,int counter) {
	logger.debug("\n\nWB insertLinkToBlock() defline is: " + defline + "\n");
	
        // extract organism from the defline
        String hit_score = extractField(defline, scoreRegex);
	logger.debug("WB insertLinkToBlock() score is " + hit_score + "\n");
	
        // replace the url into the defline
        Pattern pattern = Pattern.compile(scoreRegex);
        Matcher matcher = pattern.matcher(defline);
        if (matcher.find()) {
            // the organism is located at group 1
            int start = matcher.start(1);
            int end = matcher.end(1);
 
            // insert a link tag into the data
            StringBuffer sb = new StringBuffer(defline.substring(0, start));
	    sb.append("<a href=\"#");
            //sb.append(hit_sourceId);
	    sb.append(counter);
            sb.append("\">");
            sb.append(hit_score);
            sb.append("</a>");
            sb.append(defline.substring(end));
            return sb.toString();
        } else return defline;
    }



} //end of java class
