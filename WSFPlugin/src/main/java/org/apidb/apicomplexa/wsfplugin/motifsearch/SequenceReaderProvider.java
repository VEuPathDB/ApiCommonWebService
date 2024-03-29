package org.apidb.apicomplexa.wsfplugin.motifsearch;

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
    private static final char DEF_LINE_START_INDICATOR = '>';

    private final char[] buffer = new char[BUFFER_SIZE];
    private final FileReader fileReader;
    private final Pattern deflinePattern;
    private FastaReader currentStream = null;
    private int currentPos = BUFFER_SIZE;

    /**
     * The current limit of the buffer. The maximum of buffer size or the number of chars left to the end of the file.
     */
    private int limit = BUFFER_SIZE;

    public SequenceReaderProvider(File input, Pattern defLinePattern) throws FileNotFoundException {
        this.fileReader = new FileReader(input);
        this.deflinePattern = defLinePattern;
    }

    /**
     * Provides the next sequence from the file, or empty if the end of the file is reached. Sequences can only be
     * retrieved and consumed serially. An exception is thrown if called before the most recently returned stream is
     * consumed.
     *
     * @return The next sequence from the {@link SequenceReaderProvider#fileReader}.
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

    private void fillBuffer() throws IOException {
        if (currentPos >= buffer.length) {
            limit = fileReader.read(buffer);
            currentPos = 0;
        }
    }

    private String readLine() throws IOException {
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
        private String defline;
        private boolean closed;
        private boolean endReached;

        public FastaReader(String defline) {
            final Matcher defLineMatcher = deflinePattern.matcher(defline);
            if (!defLineMatcher.find()) {
                throw new RuntimeException("Cannot read definition line " + defline);
            }
            this.defline = defline;
            this.endReached = false;
            this.closed = false;
        }

        public String getDefline() {
            return defline;
        }

        /**
         * Reads {@param len} characters offset into {@param cbuf} array starting at {@param off} index of the array.
         * The characters are read from a buffer managed by the outer class. If the buffer is consumed, it is refilled
         * from the outer class's {@link SequenceReaderProvider#fileReader} member variable.
         *
         * The stream ends once a new Fasta defline in the outer class's {@link SequenceReaderProvider#fileReader} is
         * encountered.
         */
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