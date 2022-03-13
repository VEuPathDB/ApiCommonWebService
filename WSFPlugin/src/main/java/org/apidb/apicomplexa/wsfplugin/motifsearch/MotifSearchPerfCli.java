package org.apidb.apicomplexa.wsfplugin.motifsearch;

import static org.gusdb.fgputil.FormatUtil.NL;

import java.io.File;
import java.util.Properties;

import org.gusdb.fgputil.FormatUtil;
import org.gusdb.fgputil.Timer;

public class MotifSearchPerfCli {

  public static void main(String[] args) throws Exception {
    System.err.println("Args: " + FormatUtil.arrayToString(args, ", "));
    if (args.length != 2) usageAndExit();
    String pattern = args[0].trim();
    if (pattern.isEmpty()) usageAndExit();
    File file = new File(args[1]);
    if (!file.isFile() || !file.canRead()) {
      System.err.println(file.getAbsolutePath() + " is not a readable file.");
      System.exit(2);
    }
    MotifConfig config = new MotifConfig(new Properties(),
        DnaMotifPlugin.FIELD_REGEX, DnaMotifPlugin.DEFAULT_REGEX);
    MotifSearchPerfCli stats = new MotifSearchPerfCli();
    new DnaMatchFinder(config).findMatches(file,
        AbstractMotifPlugin.translateExpression(pattern, DnaMotifPlugin.SYMBOL_MAP),
        stats::nextMatch,
        org -> "PlasmoDB");
    stats.report();
  }

  private static void usageAndExit() {
    System.err.println("USAGE: fgpJava " + MotifSearchPerfCli.class.getName() + " <pattern> <fasta_file>");
    System.exit(1);
  }

  private Timer _timer = new Timer();
  private String _thisSequence = null;
  private long _numSequencesWithMatches = 0;
  private long _numTotalMatches = 0;
  private long _totalLength = 0;

  public void nextMatch(Match match) {
    if (!match.sequenceId.equals(_thisSequence)) {
      _numSequencesWithMatches++;
      _thisSequence = match.sequenceId;
    }
    _numTotalMatches++;
    _totalLength += match.sequence.length();
  }

  private void report() {
    long runtimeMillis = _timer.getElapsed();
    double avgMatchesPerSequence = (double)_numTotalMatches / (double)_numSequencesWithMatches;
    double avgMatchLength = (double)_totalLength / (double)_numTotalMatches;
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
