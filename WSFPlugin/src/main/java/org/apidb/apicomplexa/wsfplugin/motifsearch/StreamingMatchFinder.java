package org.apidb.apicomplexa.wsfplugin.motifsearch;

import org.apidb.apicomplexa.wsfplugin.motifsearch.AbstractMotifPlugin.MatchFinder;
import org.apidb.apicomplexa.wsfplugin.motifsearch.SequenceReaderProvider.FastaReader;
import org.gusdb.fgputil.functional.FunctionalInterfaces.ConsumerWithException;
import org.gusdb.fgputil.functional.FunctionalInterfaces.FunctionWithException;

import java.io.File;
import java.io.Reader;
import java.util.Optional;
import java.util.regex.Pattern;

public abstract class StreamingMatchFinder implements MatchFinder {

    protected final MotifConfig _config;

    public StreamingMatchFinder(MotifConfig config) {
        _config = config;
    }

    protected abstract void findMatchesInSequence(String defLine, Pattern searchPattern, Reader sequence,
                                                  ConsumerWithException<PluginMatch> consumer,
                                                  FunctionWithException<String, String> orgToProjectId) throws Exception;

    @Override
    public void findMatches(File fastaFile,
                            Pattern searchPattern,
                            ConsumerWithException<PluginMatch> consumer,
                            FunctionWithException<String, String> orgToProjectId) throws Exception {
        try (SequenceReaderProvider sequenceProvider = new SequenceReaderProvider(fastaFile, _config.getDeflinePattern())) {
            Optional<FastaReader> fastaReader = sequenceProvider.nextSequence();
            while (!fastaReader.isEmpty()) {
                final FastaReader reader = fastaReader.get();
                findMatchesInSequence(reader.getDefline(), searchPattern, reader, consumer, orgToProjectId);
                fastaReader = sequenceProvider.nextSequence();
            }
        }
    }
}
