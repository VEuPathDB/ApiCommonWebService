package org.apidb.apicomplexa.wsfplugin.motifsearch.poc;

import java.io.*;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Takes a .fasta file as input and serially returns FastaReader objects that contain sequence metadata parsed from def
 * lines and return raw sequence data when read.
 *
 * Note that this can also take a file with alternating lines of sequence data and def lines.
 */
public class SequenceReaderProvider implements AutoCloseable {
    private static final int BUFFER_SIZE = 65536;
    private static final Pattern DEF_LINE_PATTERN = Pattern.compile(">([A-Za-z0-9_-]+) \\| strand=(.*) \\| organism=(.+) \\| version=(.+) \\| length=(\\d+) \\| SO=(.+)");
    private static final char DEF_LINE_START_INDICATOR = '>';

    private final FastaReader currentStream = null;
    private final char[] buffer = new char[BUFFER_SIZE];
    private final FileReader fileReader;
    private int currentPos = BUFFER_SIZE;
    private int limit = BUFFER_SIZE;

    public SequenceReaderProvider(File input) throws FileNotFoundException {
        this.fileReader = new FileReader(input);
    }

    /**
     * Provides the next sequence from the file, or empty if the end of the file is reached. Sequences can only be
     * retrieved in sequence. An exception is thrown if this is called before the current stream is consumed.
     *
     * @return The next sequence from the {@link fileReader}.
     * @throws IllegalStateException if the current sequence has not been fully consumed.
     */
    public Optional<FastaReader> nextSequence() throws IOException {
        if (currentStream != null && !currentStream.endReached) {
            throw new IllegalStateException("Cannot provide the next sequence until previous sequence is consumed.");
        }
        final String defLine = readLine();
        if (defLine == null) {
            return Optional.empty();
        }
        currentStream = new FastaReader(defLine);
        return Optional.of(currentStream);
    }

    public void fillBuffer() throws IOException {
        if (currentPos >= buffer.length) {
            limit = fileReader.read(buffer);
            currentPos = 0;
        }
    }

    public String readLine() throws IOException {
        StringBuilder defLine = new StringBuilder();
        while (true) {
            if (currentPos > limit) {
                return null;
            }
            if (currentPos >= buffer.length) {
                fillBuffer();
            }
            if (buffer[currentPos] == '\n') {
                currentPos++;
                return defLine.toString();
            }
            defLine.append(buffer[currentPos]);
            currentPos++;
        }
    }

    @Override
    public void close() throws Exception {
        fileReader.close();
    }

    public class FastaReader extends Reader {
        private String currentSequenceId;
        private String currentStrand;
        private String currentOrganism;
        private boolean closed;
        private boolean endReached;

        public FastaReader(String defLine) {
            final Matcher defLineMatcher = DEF_LINE_PATTERN.matcher(defLine);
            if (!defLineMatcher.find()) {
                throw new RuntimeException("Cannot read definition line " + defLine);
            }
            this.currentSequenceId = defLineMatcher.group(1);
            this.currentStrand = defLineMatcher.group(2);
            this.currentOrganism = defLineMatcher.group(3);
            this.endReached = false;
            this.closed = false;
        }

        public String getCurrentSequenceId() {
            return currentSequenceId;
        }

        public String getCurrentStrand() {
            return currentStrand;
        }

        public String getCurrentOrganism() {
            return currentOrganism;
        }

        @Override
        public int read(char[] cbuf, int off, int len) throws IOException {
            if (closed) {
                throw new IOException("Reader has already been closed.");
            }
            if (off + len > cbuf.length) {
                throw new IllegalArgumentException("Offset + Length cannot exceed size of buffer");
            }
            if (endReached) {
                return -1;
            }
            int charsRead = 0;
            int i = off;
            while (i < off + len) {
                if (currentPos >= buffer.length) {
                    fillBuffer();
                }
                if (buffer[currentPos] == DEF_LINE_START_INDICATOR || currentPos > limit) {
                    endReached = true;
                    return charsRead;
                } else if (buffer[currentPos] != '\n') {
                    cbuf[i] = buffer[currentPos];
                    charsRead++;
                    i++;
                }
                currentPos++;
            }
            return charsRead;
        }

        @Override
        public void close() throws IOException {
            this.closed = true;
        }
    }
}