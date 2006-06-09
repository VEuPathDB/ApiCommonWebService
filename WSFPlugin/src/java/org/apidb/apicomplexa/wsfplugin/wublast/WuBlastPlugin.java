package org.apidb.apicomplexa.wsfplugin.wublast;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gusdb.wsf.plugin.WsfPlugin;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * 
 */

/**
 * @author Jerric
 * @created Nov 2, 2005
 */
public class WuBlastPlugin extends WsfPlugin {

    private static final String PROPERTY_FILE = "wuBlast-config.xml";

    // column definitions
    public static final String COLUMN_ID = "Identifier";
    public static final String COLUMN_HEADER = "Header";
    public static final String COLUMN_FOOTER = "Footer";
    public static final String COLUMN_ROW = "TabularRow";
    public static final String COLUMN_BLOCK = "Alignment";
    public static final String COLUMN_PROJECT_ID = "ProjectId";

    // required parameter definitions
    public static final String PARAM_SEQUENCE = "BlastQuerySequence";
    public static final String PARAM_QUERY_TYPE = "BlastQueryType";
    public static final String PARAM_DATABASE_ORGANISM = "BlastDatabaseOrganism";
    public static final String PARAM_DATABASE_TYPE = "BlastDatabaseTypeGene";

    // field definitions in the config file
    private static final String FIELD_APP_PATH = "AppPath";
    private static final String FIELD_DATA_PATH = "DataPath";
    private static final String FIELD_TIMEOUT = "Timeout";
    private static final String FIELD_TEMP_PATH = "TempPath";
    private static final String FIELD_USE_PROJECT_ID = "UseProjectId";

    //ncbi seems to have these dependent on dbtype
    private static final String FIELD_SOURCE_ID_REGEX = "SourceIdRegex";
    private static final String FIELD_ORGANISM_REGEX = "OrganismRegex";
    private static final String FIELD_SCORE_REGEX = "ScoreRegex";

    private static final String TEMP_FILE_PREFIX = "wuBlastPlugin";

    private static String appPath;
    private static String dataPath;
    private static String tempPath;
    private static long timeout;
    private static boolean useProjectId;
    private static String sourceIdRegex;
    private static String organismRegex;
    private static String scoreRegex;

    //parameter: database type (CDS,Proteins,Genomic)
    private static String dbType;
    // private static String orgParam;
    


    /**
     * @throws WsfServiceException
     * @throws IOException
     * @throws InvalidPropertiesFormatException
     * 
     */
    public WuBlastPlugin() throws WsfServiceException {
        super(PROPERTY_FILE);

        // load properties
        appPath = getProperty(FIELD_APP_PATH);
        dataPath = getProperty(FIELD_DATA_PATH);
        tempPath = getProperty(FIELD_TEMP_PATH);
	sourceIdRegex = getProperty(FIELD_SOURCE_ID_REGEX);
	organismRegex = getProperty(FIELD_ORGANISM_REGEX);
	scoreRegex = getProperty(FIELD_SCORE_REGEX);

	if (appPath == null || dataPath == null || tempPath == null)
            throw new WsfServiceException(
                    "The required fields in property file are missing: "
                            + FIELD_APP_PATH + ", " + FIELD_DATA_PATH + ", " + FIELD_TEMP_PATH);

	String max = getProperty(FIELD_TIMEOUT);
        if (max == null) timeout = 60; // by default, set timeout as 60 seconds
        else timeout = Integer.parseInt(max);

	String useProject = getProperty(FIELD_USE_PROJECT_ID);
        useProjectId = (useProject != null && useProject.equalsIgnoreCase("yes"));
    }


    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getRequiredParameters()
     */
    @Override
    protected String[] getRequiredParameterNames() {
        return new String[] { PARAM_QUERY_TYPE, PARAM_SEQUENCE, PARAM_DATABASE_ORGANISM, PARAM_DATABASE_TYPE };
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#getColumns()
     */
    @Override
    protected String[] getColumns() {
	if (useProjectId) return new String[]{COLUMN_HEADER,COLUMN_PROJECT_ID,COLUMN_ID,
					      COLUMN_ROW,COLUMN_BLOCK,COLUMN_FOOTER};
        else return new String[]{COLUMN_HEADER,COLUMN_ID,COLUMN_ROW,COLUMN_BLOCK,COLUMN_FOOTER};
     }



    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.plugin.WsfPlugin#validateParameters(java.util.Map)
     */
    @Override
    protected void validateParameters(Map<String, String> params)
            throws WsfServiceException {
    // do nothing in this plugin ?? check sequence empty or too short?
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.gusdb.wsf.WsfPlugin#execute(java.util.Map, java.lang.String[])
     */
    @Override
    protected String[][] execute(Map<String, String> params,
            String[] orderedColumns) throws WsfServiceException {
        logger.info("WB execute\n\n");
        logger.info("orderedcolumns are: " + orderedColumns[0] + "," + orderedColumns[1] + "," + orderedColumns[2] + "," + orderedColumns[3] + "," + orderedColumns[4] + "," + orderedColumns[5] + "\n\n");

        try {
            // create temporary files for input sequence and output report
	    File dir = new File(tempPath);
            File seqFile = File.createTempFile(TEMP_FILE_PREFIX, "in",dir);
            File outFile = File.createTempFile(TEMP_FILE_PREFIX, "out",dir);

            // prepare the arguments
            String command = prepareParameters(params, seqFile, outFile);
            logger.info("\n\nWB execute: command prepared: " + command+"\n");

            // invoke the command
            String output = invokeCommand(command, timeout);
            if (exitValue != 0)
                throw new WsfServiceException("\nThe invocation is failed: "
                        + output);

            // if the invocation succeeds, prepare the result; otherwise,
            // prepare results for failure scenario
            logger.info("\n\nWB preparing the result\n");
            String[][] result = prepareResult(orderedColumns, outFile);

	    logger.info("\n  WB result RETURNED\n\n");
            return result;
        } catch (IOException ex) {
            logger.error("\n\nERROR preparing the result:" + ex);
            throw new WsfServiceException(ex);
        }
    }

    private String prepareParameters(Map<String, String> params, File seqFile,
            File outFile) throws IOException {

        // get parameters
        String seq = params.get(PARAM_SEQUENCE);
        dbType = params.get(PARAM_DATABASE_TYPE);
        String orgParam = params.get(PARAM_DATABASE_ORGANISM);
	String qType = params.get(PARAM_QUERY_TYPE);

        String blastApp = getBlastProgram(qType, dbType);

	String seqType = "n/";
        if (dbType.equals("Proteins")) {
               seqType = "p/";
        }       

	/*
	//ncbi plugin does this differently...
        // output sequence in fasta format, with sequence wrapped for every 60
        // characters
        PrintWriter out = new PrintWriter(new FileWriter(seqFile));
	if (qType.equals("protein"))  out.println(">Protein Sequence");
	else  out.println(">DNA Sequence");
        int pos = 0;
        while (pos < seq.length()) {
            int end = Math.min(pos + 60, seq.length());
            out.println(seq.substring(pos, end));
            pos = end;
        }
	*/

	// write the sequence into the temporary fasta file,
	// do not reformat the sequence - easy to introduce problem
	PrintWriter out = new PrintWriter(new FileWriter(seqFile));
	if (!seq.startsWith(">")) out.println(">MySeq1");
	out.println(seq);


        out.flush();
        out.close();

        //Parse-out any blast options
        StringBuffer bv = new StringBuffer();
        String blastVariables = bv.toString();
       
        // now prepare the commandline
        StringBuffer sb = new StringBuffer();
        sb.append(appPath + blastApp);
	sb.append(" " + dataPath + seqType + orgParam + dbType + "/" + orgParam + dbType);
        sb.append(" " + seqFile.getAbsolutePath());

        for (String param : params.keySet()) {
            if (!param.equals(PARAM_QUERY_TYPE)
                    && !param.equals(PARAM_DATABASE_ORGANISM)
                    && !param.equals(PARAM_DATABASE_TYPE)
                    && !param.equals(PARAM_SEQUENCE)) {
                sb.append(" " + param + "=" + params.get(param));
            }
        }
        sb.append(" O=" + outFile.getAbsolutePath() + blastVariables);
        return sb.toString();
    }

    private String[][] prepareResult(String[] orderedColumns, File outFile)
            throws IOException {

	String hit_sourceId = "";

        // create a map of <column/position>
        Map<String, Integer> columns = new HashMap<String, Integer>(
                orderedColumns.length);
        for (int i = 0; i < orderedColumns.length; i++) {
            columns.put(orderedColumns[i], i);
        }

        // open output file, and read it
        String line;
        BufferedReader in = new BufferedReader(new FileReader(outFile));
        StringBuffer header = new StringBuffer();
        do {
            line = in.readLine();
            if (line == null)
                throw new IOException("Invalid BLAST output format");
            header.append(line + newline);
        } while (!line.startsWith("Sequence"));

        // read tabular part, which starts after the ONE empty line
        line = in.readLine(); // skip an empty line
	logger.info("\nWB prepareResult: Line skipped: " + line+"\n");
	

        Map<String, String> rows = new HashMap<String, String>();
        while ((line = in.readLine()) != null) {
	    logger.info("\nWB prepareResult: Unless no hits, this should be a tabular row line: " + line+"\n");
            if (line.trim().length() == 0) {
		logger.info("\nWB prepareResult: Line length 0!!, we finished with tabular rows: " + line+"\n");
		break;}

	    //TODO: before adding the line to rows, insert:
	    //-  link to gene page, in source_id, and
	    //- link to alignment block name=#source_id, in score
	    hit_sourceId = extractID(line);
	    line = insertLinkToBlock(line);
	    line = insertUrl(line);
            rows.put(hit_sourceId, line);

        }//end while

	//we need to deal with WARNINGs
        line = in.readLine(); // skip an empty line
	logger.info("\nWB prepareResult: This line is supposed to be empty or could have a WARNING or NONE: " + line+"\n");

	if (line.indexOf("NONE") >= 0)    return new String[0][columns.size()];

	if (line.trim().startsWith("WARNING")) {
	    line = in.readLine(); // skip
	    logger.info("\nWB prepareResult: This line is continuation of warning line: " + line+"\n");
	    line = in.readLine(); // skip
	    logger.info("\nWB prepareResult: This line is supposed to be empty: " + line+"\n");
	    line = in.readLine(); // skip
	    logger.info("\nWB prepareResult: This line is supposed to be empty: " + line+"\n");
	}


        // extract alignment blocks
	String hit_organism, hit_projectId = "";

        List<String[]> blocks = new ArrayList<String[]>();
        StringBuffer block = null;
        String[] alignment = null;
        while ((line = in.readLine()) != null) {
	    // found a warning before parameters
            if (line.trim().startsWith("WARNING")) {
		logger.info("\nWB prepareResult: Found WARNING, skip: " + line+"\n");
		line = in.readLine(); // skip
		line = in.readLine(); // skip
		line = in.readLine(); // skip
	    }

            // reach the footer part
            if (line.trim().startsWith("Parameters")) {
		logger.info("\nWB prepareResult: Found Parameters: " + line+"\n");
                // output the last block, if have
                if (alignment != null) {
                    alignment[columns.get(COLUMN_BLOCK)] = block.toString();
                    blocks.add(alignment);
                }
                break;
            }

            // reach a new start of alignment block
            if (line.length() > 0 && line.charAt(0) == '>') {
		logger.info("\nWB prepareResult: This should be a new block: " + line+"\n");

                // output the previous block, if have
                if (alignment != null) {
                    alignment[columns.get(COLUMN_BLOCK)] = block.toString();
                    blocks.add(alignment);
                }
                // create a new alignment and block
                alignment = new String[orderedColumns.length];
                block = new StringBuffer();

                // obtain the ID of it, which is the rest of this line
		hit_sourceId = extractID(line);
		logger.info("\nWB prepareResult: hit_sourceId : " + hit_sourceId+"\n");
 
		//as long we cannot select more than one database we dont need to use regex...
		//hit_organism = orgParam; 
		//hit_organism = extractOrganism(line);
		hit_organism = extractField(line, organismRegex);
		logger.info("\n         organism : " + hit_organism+"\n");
		hit_projectId = getProjectId(hit_organism);
		logger.info("\n         projectId : " + hit_projectId+"\n\n");
		alignment[columns.get(COLUMN_PROJECT_ID)] = hit_projectId;


		//TODO: insert <a name="source_id"></a> in the beginning of the line
		// and insert link to gene page, in source_id,	   
		line = insertUrl(line);
		line = "<a name=\"" + hit_sourceId + "\"></a>" + line;
		alignment[columns.get(COLUMN_ID)] = hit_sourceId; 

		
		
            }
            // add the line to the block
            block.append(line + newline);
	    
        }//end while

        // get the rest as the footer part
        StringBuffer footer = new StringBuffer();
        footer.append(line + newline);
        while ((line = in.readLine()) != null) {
            footer.append(line + newline);
        }

        // now reconstruct the result
        int size = Math.max(1, blocks.size());

        String[][] results = new String[size][orderedColumns.length];
        for (int i = 0; i < blocks.size(); i++) {
            alignment = blocks.get(i);
            // copy ID
            int idIndex = columns.get(COLUMN_ID);
            results[i][idIndex] = alignment[idIndex];
	    // copy PROJECT_ID
            int projectIdIndex = columns.get(COLUMN_PROJECT_ID);
            results[i][projectIdIndex] = alignment[projectIdIndex];
            // copy block
            int blockIndex = columns.get(COLUMN_BLOCK);
            results[i][blockIndex] = alignment[blockIndex];

            // copy tabular row
            int rowIndex = columns.get(COLUMN_ROW);
            for (String id : rows.keySet()) {
		if (alignment[idIndex].startsWith(id)) {
                    results[i][rowIndex] = rows.get(id);
                    break;
                }
            }
        }
        // copy the header and footer
	results[0][columns.get(COLUMN_HEADER)] = header.toString();
        results[size - 1][columns.get(COLUMN_FOOTER)] = footer.toString();
	
        return results;
    }



    //----------------------------------------------------------

    private String getBlastProgram(String qType, String dbType) {
              // throws WsfServiceException{
        String bp = null;
        if ("dna".equalsIgnoreCase(qType)) {
            if ("CDS".equals(dbType) || "Genomic".equals(dbType)
                    || "dna".equals(dbType)) {
                bp = "blastn";
            } else if (dbType.toLowerCase().indexOf("translated") >= 0) {
                bp = "tblastx";
            } else if ("Proteins".equals(dbType)) {
                bp = "blastx";
            }
        } else if ("protein".equalsIgnoreCase(qType)) {
            if ("CDS".equals(dbType) || "Genomic".equals(dbType)
                    || dbType.toLowerCase().indexOf("translated") >= 0) {
                bp = "tblastn";
            } else if ("Proteins".equals(dbType)) {
                bp = "blastp";
            }
        }
                                                                                                                             
        //if (bp == null) {
	//  throw new WsfServiceException("invalid blast query or database types ("
	//     + qType + ", " + dbType + ")");
        //}

       return bp;
    }


    private String getProjectId(String organism) {

        String projectId = "apiDb";
	/*
        if (organism.startsWith("C")) {
           projectId = "cryptodb";
        }
        else if (organism.startsWith("P")) {
           projectId = "plasmodb";
        }
        else if (organism.startsWith("T")) {
           projectId = "toxodb";
        }
	*/
	//with current organismRegex organism could start with > in block line
	if (organism.contains("Crypto")) {
	    projectId = "cryptodb";
        }
        else if (organism.contains("Plasmo")) {
	    projectId = "plasmodb";
        }
        else if (organism.contains("Toxo")) {
	    projectId = "toxodb";
        }

        return projectId;
    }

    private String extractID(String row) {
        String[] pieces = tokenize(row);

//added to deal with Pvivax until we get rid of this method using sourceregex....
        String hit_org = pieces[0];

        String hit_srcid = pieces[2];
        if (dbType == "Genomic") {
           hit_srcid = pieces[1];
        }
        if (hit_org.contains("vivax")) {
            hit_srcid = pieces[3];
        }
        return hit_srcid;

    }

   private String extractOrganism(String row) {
        String[] pieces = tokenize(row);
        String hit_org = pieces[0];
        return hit_org;
    }


    private String getSourceUrl(String projectId, String sourceId) {
        String sourceUrl = sourceId + " - (no link)";   

     if ("cryptodb".equals(projectId)) {
         sourceUrl = ("<a href=\"http://cryptodb.org/cryptodb/showRecord.do?name=GeneRecordClasses.GeneRecordClass&projectId=" + projectId + "&primary_key=" + sourceId + "\" target=\"_blank\" >" + sourceId + "</a>");
       }
       else if ("plasmodb".equals(projectId)) {
           sourceUrl = ("<a href=\"http://v5-0.plasmodb.org/plasmo/showRecord.do?name=GeneRecordClasses.GeneRecordClass&projectId=" + projectId + "&primary_key=" + sourceId + "\" target=\"_blank\" >" + sourceId + "</a>");
       }
       else if ("toxodb".equals(projectId)) {
           sourceUrl = ("<a href=\"http://v4-0.toxodb.org/toxo-release4-0/showRecord.do?name=GeneRecordClasses.GeneRecordClass&projectId=" + projectId + "&primary_key=" + sourceId + "\" target=\"_blank\" >" + sourceId + "</a>");
       }

    return sourceUrl;
      
    }


 private String extractField(String defline, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(defline);
        if (matcher.find()) {
            // the match is located at group 1
            return matcher.group(1);
        } else return null;
    }
 
private String insertUrl(String defline) {
        // extract organism from the defline
        String hit_sourceId = extractField(defline, sourceIdRegex);
	String hit_organism = extractField(defline, organismRegex);
        String hit_projectId = getProjectId(hit_organism);
        String linkedSourceId = getSourceUrl(hit_projectId,hit_sourceId);
 
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


private String insertLinkToBlock(String defline) {
    logger.info("\n\nWB insertLinkToBlock: defline is: " + defline + "\n");

        // extract organism from the defline
        String hit_score = extractField(defline, scoreRegex);
	logger.info("WB insertLinkToBlock:score is " + hit_score + "\n");
	String hit_sourceId = extractField(defline, sourceIdRegex);
 
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
            sb.append(hit_sourceId);
            sb.append("\">");
            sb.append(hit_score);
            sb.append("</a>");


            sb.append(defline.substring(end));
            return sb.toString();
        } else return defline;
    }


}
