package org.apidb.apicomplexa.wsfplugin.motifsearch;

import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class DnaStreamingMatchFinderTest {

    @Test
    public void testMatch() throws Exception {
        final Properties properties = new Properties();
        properties.setProperty("ContextLength", "20");
        DnaMatchFinder matchFinder = new DnaMatchFinder(new MotifConfig(properties, "",
                ">(?:\\w*\\|)*([^|\\s]+)\\s*\\|.*?\\s*strand=\\(([+\\-])\\)\\s*\\|\\s*organism=([^|\\s]+)"));
        List<PluginMatch> matches = new ArrayList<>();
        matchFinder.findMatches(new File("/Users/rooslab/downloads/GenomeDoubleStrand.fasta"),
                Pattern.compile("ATCGGTCCATA"),
                matches::add,
                org -> "project");
        System.out.println("Match count: " + matches.size());
        matches.forEach(match -> {
            System.out.println(match.locations);
        });

//        DnaMatchFinder hmMatchFinder = new DnaMatchFinder(new MotifConfig(properties, "",
//                ">(?:\\w*\\|)*([^|\\s]+)\\s*\\|.*?\\s*strand=\\(([+\\-])\\)\\s*\\|\\s*organism=([^|\\s]+)"));
//        List<PluginMatch> hmMatches = new ArrayList<>();
//        hmMatchFinder.findMatches(new File("/Users/rooslab/workspace/pocs/regex-streaming/tst/data-files/GenomeDoubleStrand.fasta"),
//                Pattern.compile("ATCGGTCCATA"),
//                hmMatches::add,
//                org -> "project");
//        System.out.println("Match count: " + matches.size());
//        matches.forEach(match -> {
//            System.out.println(match.locations);
//        });
    }

//    @Test
//    public void testOldMatch() throws Exception {
//        final Properties properties = new Properties();
//        properties.setProperty("ContextLength", "20");
//        DnaMatchFinder matchFinder = new DnaMatchFinder(new MotifConfig(properties, "",
//                ">(?:\\w*\\|)*([^|\\s]+)\\s*\\|.*?\\s*strand=\\(([+\\-])\\)\\s*\\|\\s*organism=([^|\\s]+)"));
//        List<PluginMatch> matches = new ArrayList<>();
//        matchFinder.findMatches(new File("/Users/rooslab/workspace/pocs/regex-streaming/tst/data-files/GenomeDoubleStrand.fasta"),
//                Pattern.compile("ATCGGTCCATA"),
//                matches::add,
//                org -> "project");
//        System.out.println("Match count: " + matches.size());
//    }

}
