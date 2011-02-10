/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.gusdb.wsf.plugin.WsfServiceException;

/**
 * @author Jerric, modified by Cristina 2010 to add DNA motif
 * @created Jan 31, 2006
 */

// geneID could be an ORF or a genomic sequence deending on who uses the plugin
public class DnaMotifPlugin extends MotifSearchPlugin {

    // The property file for dna motif search
    public static final String FIELD_DNA_HEADLINE_REGEX = "DnaHeadlineRegex";

    private static final Logger logger = Logger.getLogger(DnaMotifPlugin.class);

    private Pattern headlinePattern;

    protected DnaMotifPlugin() throws WsfServiceException {
        super();
    }

    @Override
    public void initialize(Map<String, Object> context)
            throws WsfServiceException {
        super.initialize(context);

        String headlineRegex = getProperty(FIELD_DNA_HEADLINE_REGEX);
        if (headlineRegex == null)
            throw new WsfServiceException(FIELD_DNA_HEADLINE_REGEX
                    + " is not defined in the config file.");
        this.headlinePattern = Pattern.compile(headlineRegex);
    }

    @Override
    protected Map<Character, String> getSymbols() {
        Map<Character, String> symbols = new HashMap<Character, String>();
        symbols.put('R', "AG");
        symbols.put('Y', "CT");
        symbols.put('M', "AC");
        symbols.put('K', "GT");
        symbols.put('S', "CG");
        symbols.put('W', "AT");
        symbols.put('B', "CGT");
        symbols.put('D', "AGT");
        symbols.put('H', "ACT");
        symbols.put('V', "ACG");
        symbols.put('N', "ACGT");

        return symbols;
    }

    @Override
    protected void findMatches(Set<Match> matches, String headline,
            Pattern searchPattern, String sequence, String colorCode,
            int contextLength) {
        // parse the headline
        Matcher headlineMatcher = headlinePattern.matcher(headline);
        if (!headlineMatcher.find()) {
            logger.warn("Invalid headline: " + headline);
            return;
        }
        // the sequence id has to be in group(1),
        // strand info has to be in group(2)
        // organsim has to be in group(3),
        String sequenceId = headlineMatcher.group(1);
        String strand = headlineMatcher.group(2);
        String organism = headlineMatcher.group(3);
        String projectId = getProjectId(organism);

        int length = sequence.length();
        boolean reversed = (strand.equals("-"));

        Matcher matcher = searchPattern.matcher(sequence);
        while (matcher.find()) {
            int start = matcher.start();
            int stop = matcher.end();
            Match match = new Match();
            match.projectId = projectId;
            match.matchCount = 1;
            match.locations = getLocation(length, reversed, start, stop);
            match.sourceId = sequenceId + ":" + match.locations + "(" + strand
                    + ")";

            // create matching context
            StringBuilder context = new StringBuilder();
            int begin = Math.max(0, start - contextLength);
            if (begin > 0) context.append("...");
            if (begin < start)
                context.append(sequence.substring(begin, start));
            context.append("<font color=\"" + colorCode + "\">");
            context.append(sequence.substring(start, stop));
            context.append("</font>");
            int end = Math.min(sequence.length(), stop + contextLength);
            if (end > stop) context.append(sequence.substring(stop, end));
            if (end < sequence.length()) context.append("...");

            match.sequence = context.toString();
            matches.add(match);
        }
    }

    private String getLocation(int length, boolean reversed, int start, int stop) {
        if (reversed) {
            int newStart = length - stop;
            stop = length - start;
            start = newStart;
        }
        return start + "-" + stop;
    }
}
