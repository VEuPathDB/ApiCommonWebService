package org.apidb.apicomplexa.wsfplugin.motifsearch.poc;

import org.apidb.apicomplexa.wsfplugin.motifsearch.*;
import org.gusdb.fgputil.FormatUtil;
import org.gusdb.fgputil.Timer;

import java.io.File;
import java.io.FileReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.gusdb.fgputil.FormatUtil.NL;

public class PocPerf {

    public static void main(String[] args) throws Exception {
        // report statistics gathered
        PocPerf stats = new PocPerf();

        // parse args; some minimal validation
        System.err.println("Args: " + FormatUtil.arrayToString(args, ", "));
        if (args.length != 3) usageAndExit();
        String pattern = args[0].trim();
        if (pattern.isEmpty()) usageAndExit();
        File file = new File(args[1]);
        FileReader reader = new FileReader(file);
        if (!file.isFile() || !file.canRead()) {
            System.err.println(file.getAbsolutePath() + " is not a readable file.");
            System.exit(2);
        }
        int bufferSize = Integer.parseInt(args[2]);

        if (!file.getPath().endsWith(".motif")) {
            final String collapsedFilePath = Paths.get(System.getProperty("java.io.tmpdir"), Path.of(args[1]).getFileName().toString() + ".motif").toString();
            SequencesToSingleLine.toSingleLine(reader, collapsedFilePath);
            file = new File(collapsedFilePath);
        }

        try (final SequenceFileStreamer sequenceFileStreamer = new SequenceFileStreamer(file)) {
            do {
                final Optional<SequenceFileStreamer.FastaReader> input = sequenceFileStreamer.nextSequence();
                if (input.isEmpty()) {
                    System.out.println("No more inputs.");
                    break;
                }
                System.out.println("Reading input.");
                TileMatcher.match(input.get(), AbstractMotifPlugin.translateExpression(pattern, DnaMotifPlugin.SYMBOL_MAP), 20, stats::nextMatch, bufferSize);
            } while (true);
        }
        stats.report();
    }

    private static void usageAndExit() {
        System.err.println("USAGE: fgpJava " + org.apidb.apicomplexa.wsfplugin.motifsearch.MotifSearchPerfCli.class.getName() + " <pattern> <fasta_file>");
        System.exit(1);
    }

    private Timer _timer = new Timer();
    private String _thisSequence = null;
    private long _numSequencesWithMatches = 0;
    private long _numTotalMatches = 0;
    private long _totalLength = 0;

    public void nextMatch(MatchWithContext match) {
        if (!match.getSequenceId().equals(_thisSequence)) {
            _numSequencesWithMatches++;
            _thisSequence = match.getSequenceId();
        }
        _numTotalMatches++;
        _totalLength += match.getMatch().length();
    }

    private void report() {
        long runtimeMillis = _timer.getElapsed();
        double avgMatchesPerSequence = (double) _numTotalMatches / (double) _numSequencesWithMatches;
        double avgMatchLength = (double) _totalLength / (double) _numTotalMatches;
        long msPerMatch = runtimeMillis / _numTotalMatches;
        Runtime rt = Runtime.getRuntime();
        System.out.println(
                "Statistics:"
                        + NL + "  " + _numSequencesWithMatches + ": Number of sequences with matches"
                        + NL + "  " + _numTotalMatches + ": Number of total matches"
                        + NL + "  " + avgMatchesPerSequence + ": Avg matches per sequence"
                        + NL + "  " + avgMatchLength + ": Avg match length (including context and highlighting HTML)"
                        + NL + "  " + Timer.getDurationString(runtimeMillis) + " total runtime"
                        + NL + "  " + Timer.getDurationString(msPerMatch) + " per match found"
                        + NL + "  " + rt.totalMemory() / 1024 + "kb memory allocated to complete this task"
        );
    }
}
