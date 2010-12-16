package org.apidb.apicomplexa.wsfplugin.wublast;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apidb.apicomplexa.wsfplugin.BlastPlugin;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * 
 */

/**
 * @author Jerric
 * @created Nov 2, 2005
 */

public class WuBlastPlugin extends BlastPlugin {

    private static final String PROPERTY_FILE = "wuBlast-config.xml";

    /**
     * @throws WsfServiceException
     * @throws IOException
     * @throws InvalidPropertiesFormatException
     * 
     */
    public WuBlastPlugin() throws WsfServiceException {
        super(PROPERTY_FILE);
    }

    // -------------------------------------------------------------------------
    protected String[] prepareParameters(Map<String, String> params,
            File seqFile, File outFile, String dbType) throws IOException,
            WsfServiceException {

        Vector<String> cmds = new Vector<String>();

        // String qType = params.get(PARAM_QUERY_TYPE);
        // params.remove(PARAM_QUERY_TYPE);

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

        // String blastApp = getBlastProgram(qType, dbType);

        String blastApp = params.get(PARAM_ALGORITHM);
        params.remove(PARAM_ALGORITHM);
        // so database name is built correctly for the Translated cases
        if (dbType.contains("Translated")) {
            if (dbType.contains("Transcripts")) dbType = "Transcripts";
            else if (dbType.contains("Genomics")) dbType = "Genomics";
            else if (dbType.contains("EST")) dbType = "EST";
        }

        // now prepare the commandline
        cmds.add(appPath + "/" + blastApp);

        String blastDbs = getBlastDatabase(dbType, dbOrgs);
        // logger.info("\n\nWB prepareParameters(): FULL DATAPATH is: " +
        // blastDbs + "\n");

        cmds.add(blastDbs);
        cmds.add(seqFile.getAbsolutePath());
        cmds.add("O=" + outFile.getAbsolutePath());

        for (String param : params.keySet()) {
            if (!(param.contains("filter") && params.get(param).equals("no"))) {
                if (param.contains("filter") && params.get(param).equals("yes"))
                    params.put(param, "seg");
                cmds.add(param);
                cmds.add(params.get(param));
            }

        }
        // logger.info("\n\nWB prepareParameters(): " + blastDbs + " INFERRED
        // from (" + dbType + ", '" + dbOrgs + "')");
        // logger.info("\n\nWB prepareParameters(): " + blastApp + " inferred
        // from (" + qType + ", " + dbType + ")");
        String[] cmdArray = new String[cmds.size()];
        cmds.toArray(cmdArray);
        return cmdArray;

    }

    // -------------------------------------------------------------------------
    // it used to call insertLinkToBlock(), now it uses insertBookmark() ----called in execute()
    // it used to call insertUrl(), now it uses insertIdUrl() 
    // it used to call extractField(), now it uses FindField()

    protected String[][] prepareResult(String[] orderedColumns, File outFile,
            String dbType, StringBuffer message) throws IOException {

        // so database name is built correctly for the Translated cases
        if (dbType.contains("Translated")) {
            if (dbType.contains("Transcripts")) dbType = "Transcripts";
            else if (dbType.contains("Genomics")) dbType = "Genomics";
            else if (dbType.contains("EST")) dbType = "EST";
        }

        // create a map of <column/position>
        Map<String, Integer> columns = new HashMap<String, Integer>(
                orderedColumns.length);
        for (int i = 0; i < orderedColumns.length; i++) {
            columns.put(orderedColumns[i], i);
        }

        // open output file, and read it
        // Header; if there is a WARNING before the line "Sequences", it will be
        // added to the header
        String line;
        BufferedReader in = new BufferedReader(new FileReader(outFile));
        StringBuffer header = new StringBuffer();
        do {
            line = in.readLine();
	    logger.debug("\nWB prepareResult(): HEADER: " + line + "\n");
	    if (line == null) {
		message.append(header.toString());
		return new String[0][columns.size()];
	    }
	    //  throw new IOException("Invalid BLAST output format");

            // if this is a WARNING complaining about the numbers in some fasta
            // files, do not append this line and the two following
            if (!(line.contains("invalid") && line.contains("code"))) header.append(line
                    + newline);
            else {
                line = in.readLine();
                line = in.readLine();
            }

            //logger.debug("\nWB prepareResult(): HEADER: " + line + "\n");
        } while ((!line.startsWith("Sequence")) && (!line.startsWith("FATAL")));

        // show stderr
        if (line.startsWith("FATAL")) {
	    // in BlastPlugin we append the output to the message, even when it is not empty. 
	    //     The output, when FATAL, contains the important info, so we do not need to keep adding lines to the message.
	    // line = in.readLine();
	    // header.append(line + newline);
            message.append(header.toString());
	    //logger.debug("\nWB prepareResult(): message is: *******************\n" +  message + "\n");
            return new String[0][columns.size()];
        }

        // Tabular Rows; which starts after the ONE empty line
        line = in.readLine(); // skip an empty line

        // Why is rows a Map instead of a simple String[]?
        Map<String, String> rows = new HashMap<String, String>();

        // wublast truncates deflines in tabular lines, that is why in ORF fasta
        // files the source ids are sometimes truncated -- now it is the organism...
        // we introduce these variables to link score to correct alignment
        // block, without id (with a counter instead)
        Integer counter = 0;
        String counterstring;
	String rowline; //to rewrite the tabular row line with a link

        // Loop on Tabular Rows
        while ((line = in.readLine()) != null) {
            logger.debug("\nWB prepareResult() Unless no hits, this should be a tabular row line: " + line + "\n");

            if (line.trim().length() == 0) {
                logger.debug("\nWB prepareResult(): ***********Line length 0!!, END OF tabular rows\n");
		logger.info("\n\n ********** NUMBER OF HITS: "+ rows.size() +"\n\n");
                break;
            }

            // insert bookmark: link score to alignment block name=#counter
            // blastplugin does this at the end of execute() --insertBookmark()
            // line = insertLinkToBlock(line,counter);

            // insert link to record page, in source_id, when block is read,
	    //       so we do not have the problem of truncated deflines
            // line = insertIdUrl(line, dbType);

            counterstring = counter.toString();
            rows.put(counterstring, line);
            counter++;

        }// end while

	// END OF READING TABULAR ROWS -- start reading alignments

        // We need to deal with a possible WARNING between tabular rows and
        // alignments: move it to header
        line = in.readLine(); // skip an empty line
        // logger.info("\nWB prepareResult() This line is supposed to be empty
        // or could have a WARNING or keyword NONE: " + line+"\n");
        header.append(newline + line + newline);

        if (line.indexOf("NONE") >= 0) {
            message.append(header.toString());
            return new String[0][columns.size()];
        }

        if (line.trim().startsWith("WARNING")) {
            line = in.readLine(); // get next line
            // logger.info("\nWB prepareResult() This line is continuation of
            // warning line: " + line+"\n");
            header.append(line + newline);
            line = in.readLine(); // get next line
            // logger.info("\nWB prepareResult() This line is supposed to be
            // empty: " + line+"\n");
            header.append(line + newline);
            line = in.readLine(); // get next line
            // logger.info("\nWB prepareResult() This line is supposed to be
            // empty: " + line+"\n");
        }

        // Extract alignment blocks
        String hit_organism, hit_projectId = "", hit_sourceId = "";


	//alignment will have the following []:
	// COLUMN_ID,COLUMN_PROJECT_ID,COLUMN_ROW,COLUMN_BLOCK,COLUMN_HEADER,COLUMN_FOOTER
        String[] alignment = null;
	// block will be copied into alignment[COLUMN_BLOCK]
        StringBuffer block = null;
	// miniblock is used to concatenate the lines that make up the full defline in each hit
	StringBuffer miniblock = null;

	// blocks will contain the alignments
        List<String[]> blocks = new ArrayList<String[]>();

        StringBuffer warnings = new StringBuffer();
        counter = 0;

	String myLink; // for gbrowse link

        while ((line = in.readLine()) != null) {
            // found a warning before parameters
            if (line.trim().startsWith("WARNING")) {

                logger.debug("\nWB prepareResult() Found WARNING: " + line
                        + "\n");
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
                logger.info("\nWB prepareResult(): FOOTER (found line Parameters): "
                        + line + "\n");
                // output the last block, if have
                if (alignment != null) {
                    alignment[columns.get(COLUMN_BLOCK)] = block.toString();

		    // add gbrowse link here START
		    if (dbType.equals("Genomics")){
			String alignLine= block.toString();
			Integer indx = alignLine.indexOf("Score");
			String subline = alignLine.substring(indx);
			Integer indx1;

			Pattern pattern;
			Matcher matcher;

			String[] x = Pattern.compile("Strand =").split(subline);
			for (int i=1; i<x.length; i++) {
			    String hspStart="";
			    String hspEnd="";

			    // discard lines between HSPs
			    x[i] = x[i].replaceAll("\\s*Minus Strand HSPs(.*)\\z","");
			    x[i] = x[i].replaceAll("\\s*Plus Strand HSPs(.*)\\z","");

			    // get start of HSP
			    pattern = Pattern.compile("Sbjct:\\s*\\d+");
			    matcher = pattern.matcher(x[i].trim());
			    if (matcher.find()) { 
				hspStart = matcher.group();
				hspStart = hspStart.split("Sbjct:\\s*")[1];
			    }			    
			    // get end of HSP
			    x[i] = x[i].replaceAll("\\s*Identities(.*)\\z","");
			    x[i] = x[i].replaceAll("\\s*Score(.*)\\z","");
			    x[i] = x[i].replaceAll("\\s*Minus Strand HSPs:(.*)\\z","");
			    x[i] = x[i].replaceAll("\\s*Plus Strand HSPs:(.*)\\z","");
			    pattern = Pattern.compile("\\d+\\z");
			    matcher = pattern.matcher(x[i].trim());
			    if (matcher.find()) { 
				hspEnd = matcher.group();
			    }
			    //add gbrowse link just before the 'Strand = Minus / Plus' in HSP hit
			    if (hspStart.length() > 0 || hspEnd.length() > 0){ 
				myLink = insertGbrowseLink(hit_sourceId, hspStart, hspEnd, hit_projectId);
				indx1 = block.indexOf(x[i]);
				block.insert(indx1 - 8, myLink);
				alignment[columns.get(COLUMN_BLOCK)] = block.toString();
			    } else {
				logger.info("prepareResult() hspStart/hspEnd not found in " + x[i] + "\n");
			    }
			}
		    }
		    // add gbrowse link here END

                    blocks.add(alignment);
                }
                break;
            }

            // reach a new start of alignment block
            if (line.length() > 0 && line.charAt(0) == '>') {
                logger.debug("\n\n\n-----------------\nWB prepareResult() This should be a new block: "
                        + line + "\n");
	
                // output the previous block, if have
                if (alignment != null) {
                    alignment[columns.get(COLUMN_BLOCK)] = block.toString();

		    // add gbrowse link here START
		    if (dbType.equals("Genomics")){
			String alignLine= block.toString();
			Integer indx = alignLine.indexOf("Score");
			String subline = alignLine.substring(indx);
			Integer indx1;

			Pattern pattern;
			Matcher matcher;

			String[] x = Pattern.compile("Strand =").split(subline);
			for (int i=1; i<x.length; i++) {
			    String hspStart="";
			    String hspEnd="";

			    // discard lines between HSPs
			    x[i] = x[i].replaceAll("\\s*Minus Strand HSPs(.*)\\z","");
			    x[i] = x[i].replaceAll("\\s*Plus Strand HSPs(.*)\\z","");

			    // get start of HSP
			    pattern = Pattern.compile("Sbjct:\\s*\\d+");
			    matcher = pattern.matcher(x[i].trim());
			    if (matcher.find()) { 
				hspStart = matcher.group();
				hspStart = hspStart.split("Sbjct:\\s*")[1];
			    }
			    // get end of HSP
			    x[i] = x[i].replaceAll("\\s*Identities(.*)\\z","");
			    x[i] = x[i].replaceAll("\\s*Score(.*)\\z","");
			    x[i] = x[i].replaceAll("\\s*Minus Strand HSPs:(.*)\\z","");
			    x[i] = x[i].replaceAll("\\s*Plus Strand HSPs:(.*)\\z","");
			    pattern = Pattern.compile("\\d+\\z");
			    matcher = pattern.matcher(x[i].trim());
			    if (matcher.find()) { 
				hspEnd = matcher.group();
			    }
			    //add gbrowse link just before the 'Strand = Minus / Plus' in HSP hit
			    if (hspStart.length() > 0 && hspEnd.length() > 0){ 
				myLink = insertGbrowseLink(hit_sourceId, hspStart, hspEnd, hit_projectId);
				indx1 = block.indexOf(x[i]);
				block.insert(indx1 - 8, myLink); // 8 is length of "Strand =" pattern
				alignment[columns.get(COLUMN_BLOCK)] = block.toString();
			    } else {
				logger.info("prepareResult() hspStart/hspEnd not found in " + x[i] + "\n");
			    }
			}
		    }
		    // add gbrowse link here END

                    blocks.add(alignment);
                }
                // create a new alignment and block
                alignment = new String[orderedColumns.length];
                block = new StringBuffer();

		// for deflines where source id is TOO long so
		//     the keyword "organism" in blast report appears in the second line :
		// concatenate lines of the defline so we can find "organism"
		//     with the regex provided in config file
		miniblock = new StringBuffer();
		while ( !(line.trim().startsWith("Length ="))   ) {
		    //logger.debug("\nWB prepareResult() concatenating defline: " + line + "\n");
		    miniblock.append(line.trim() + " ");
		    line = in.readLine(); // get next line
		}
		miniblock.append("\n" + line);
		line=miniblock.toString();

		//---------------   Making the link to record page
                // get source id
                int[] sourceIdPos = findField(line, sourceIdRegex);
                hit_sourceId = line.substring(sourceIdPos[0], sourceIdPos[1]);

                // get organism
                int[] organismPos = findField(line, organismRegex);
                hit_organism = line.substring(organismPos[0], organismPos[1]);
                //logger.debug("\nWB prepareResult() Organism extracted from defline is: " + hit_organism);

                if (useProjectId) {
                    hit_projectId = getProjectId(hit_organism);
                    //logger.debug("\nWB prepareResult() projectId : " + hit_projectId+"\n\n");
                    alignment[columns.get(COLUMN_PROJECT_ID)] = hit_projectId;
                }
	
                //logger.info("\n\n\n\n\nWB prepareResult(): to insert URL in alignments: line is: " + line + "\n");
                // Insert link to gene page, in source_id
                // ncbi plugin does not do this
                line = insertIdUrl(line, dbType, hit_organism);

		//--------------
		// insert link in tabular row now that we know the organism	
		counterstring = counter.toString();	
		rowline = rows.get(counterstring);
		//logger.info("\nWB prepareResult(): alignments: to insert URL in TABROW: "  + rowline + "\n");
		if ( ! (dbType.contains("ORF") && hit_organism.contains("Crypto")) ) 
		    rowline = insertIdUrl(rowline, dbType, hit_organism);
		rows.put(counterstring, rowline);
		counter++;
		//--------------

                alignment[columns.get(COLUMN_ID)] = hit_sourceId;

            }
            // add the line to the block
            block.append(line + newline);

        }// end while

        // get the rest as the footer part
        StringBuffer footer = new StringBuffer();
        if (warnings.length() != 0) footer.append(warnings.toString());
        footer.append(line + newline);
        while ((line = in.readLine()) != null) {
            if (line.contains("Database") || line.contains("Title")) {
                String[] singleDbs = line.split(";");
                for (int i = 0; i < singleDbs.length; i++) {
                    footer.append(singleDbs[i] + newline);
                }
            } else footer.append(line + newline);
        }

        logger.debug("\n\n\n--------------------------------------------\n\nWB prepareResult(): Information stored in Stringbuffers and tables, now copy info into results[][]\n\n");

        // now reconstruct the result
        int size = Math.max(1, blocks.size());
        // logger.info("\n---------WB prepareResult(): size is of Results is: "
        // + size + "\n");

        String[][] results = new String[size][orderedColumns.length];
        for (int i = 0; i < blocks.size(); i++) {
            counter = i;
            counterstring = counter.toString();
            alignment = blocks.get(i);

            // copy ID
            int idIndex = columns.get(COLUMN_ID);
            results[i][idIndex] = alignment[idIndex];

            // copy counter
            int counterIndex = columns.get(COLUMN_COUNTER);
            results[i][counterIndex] = counterstring;

            if (useProjectId) {
                // copy PROJECT_ID
                int projectIdIndex = columns.get(COLUMN_PROJECT_ID);
                results[i][projectIdIndex] = alignment[projectIdIndex];
            }
            // logger.info("\n---------WB prepareResult(): copied
            // Identifier\n");

            // copy block
            int blockIndex = columns.get(COLUMN_BLOCK);
            results[i][blockIndex] = alignment[blockIndex];
            // logger.info("\nWB prepareResult(): copied block\n");

            // copy tabular row
            int rowIndex = columns.get(COLUMN_ROW);
            for (String id : rows.keySet()) {
                // if (alignment[idIndex].startsWith(id)) {
                if (id.equalsIgnoreCase(counterstring)) {
                    results[i][rowIndex] = rows.get(id);
                    // logger.info("\nWB prepareResult(): copied tabular
                    // row\n");
                    break;
                }
            }
        }
        // copy the header and footer
        results[0][columns.get(COLUMN_HEADER)] = header.toString();
        results[size - 1][columns.get(COLUMN_FOOTER)] = footer.toString();
        // logger.info("\nWB prepareResult(): copied header and footer\n");

        return results;
    }

    // ----------- PRIVATE METHODS
    // -----------------------------------------------


    protected String getBlastProgram(String qType, String dbType)
            throws WsfServiceException {

        String bp = null;

        if ("dna".equalsIgnoreCase(qType)) {
            if (dbType.contains("Proteins") || dbType.contains("ORF")) {
                bp = "blastx";
            } else if (dbType.contains("Translated")) {
                bp = "tblastx";
            } else {
                bp = "blastn";
            }

        } else if ("protein".equalsIgnoreCase(qType)) {
            if (dbType.contains("Proteins") || dbType.contains("ORF")) {
                bp = "blastp";
            } else {
                bp = "tblastn";
            }
        }

        if (bp != null) return bp;
        else throw new WsfServiceException(
                "invalid blast query or database types (" + qType + ", "
                        + dbType + ")");

    }

} // end of java class
