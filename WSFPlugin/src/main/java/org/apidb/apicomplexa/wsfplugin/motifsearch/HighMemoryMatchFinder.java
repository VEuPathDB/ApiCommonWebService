package org.apidb.apicomplexa.wsfplugin.motifsearch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.regex.Pattern;

import org.apidb.apicomplexa.wsfplugin.motifsearch.AbstractMotifPlugin.MatchFinder;
import org.gusdb.fgputil.functional.FunctionalInterfaces.ConsumerWithException;
import org.gusdb.fgputil.functional.FunctionalInterfaces.FunctionWithException;

public abstract class HighMemoryMatchFinder implements MatchFinder {

  protected abstract void findMatchesInSequence(String defLine, Pattern searchPattern, String sequence,
      ConsumerWithException<Match> consumer, FunctionWithException<String, String> orgToProjectId) throws Exception;

  protected final MotifConfig _config;

  public HighMemoryMatchFinder(MotifConfig config) {
    _config = config;
  }

  @Override
  public void findMatches(
      File datasetFile,
      Pattern searchPattern,
      ConsumerWithException<Match> consumer,
      FunctionWithException<String, String> orgToProjectId) throws Exception {

    BufferedReader in = new BufferedReader(new FileReader(datasetFile));

    // read header of the first sequence
    String headline = null, line;
    StringBuilder sequence = new StringBuilder();
    try {
      while ((line = in.readLine()) != null) {
        line = line.trim();
        if (line.length() == 0) continue;

        if (line.charAt(0) == '>') {
          // starting of a new sequence, process the previous sequence if present
          if (sequence.length() > 0) {
            findMatchesInSequence(headline, searchPattern, sequence.toString(), consumer, orgToProjectId);

            // clear the sequence buffer to be ready for the next one
            sequence = new StringBuilder();
          }
          headline = line;
        } else {
          sequence.append(line);
        }
      }
    } finally {
      in.close();
    }

    // process the last sequence, if it hasn't been processed
    if (headline != null && sequence.length() > 0) {
      findMatchesInSequence(headline, searchPattern, sequence.toString(), consumer, orgToProjectId);
    }
  }
}
