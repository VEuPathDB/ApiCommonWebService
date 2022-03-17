package org.apidb.apicomplexa.wsfplugin.motifsearch.poc;

import java.io.*;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SequenceFileStreamer implements AutoCloseable {
    private static int BUFFER_SIZE = 8192;
    private static final Pattern DEF_LINE_PATTERN = Pattern.compile("\\>([A-Za-z0-9_-]+) \\| strand=(.*) \\| organism=(.+) \\| version=(.+) \\| length=(\\d+) \\| SO=(.+)");

    private FastaReader currentStream;
    private FileReader fileReader;
    private char[] buffer = new char[BUFFER_SIZE];
    private int currentPos = BUFFER_SIZE;
    private int limit = BUFFER_SIZE;

    public SequenceFileStreamer(File input) throws FileNotFoundException {
        this.fileReader = new FileReader(input);
    }

    public Optional<FastaReader> nextSequence() throws IOException {
        final String defLine = readUntilNewLine();
        if (defLine == null) {
            return Optional.empty();
        }
        currentStream = new FastaReader(defLine);
        return Optional.of(currentStream);
    }

    public void fill() throws IOException {
        if (currentPos >= buffer.length) {
            limit = fileReader.read(buffer);
            currentPos = 0;
        }
    }

    public String readUntilNewLine() throws IOException {
        StringBuilder defLine = new StringBuilder();
        while (true) {
            if (currentPos > limit) {
                return null;
            }
            if (currentPos >= buffer.length) {
                fill();
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
        private boolean endReached;

        public FastaReader(String defLine) {
            final Matcher defLineMatcher = DEF_LINE_PATTERN.matcher(defLine);
            if (!defLineMatcher.find()) {
                throw new RuntimeException("Cannot read definition line " + defLine);
            }
            this.currentSequenceId = defLineMatcher.group(1);
            this.currentStrand = defLineMatcher.group(2);
            this.currentOrganism = defLineMatcher.group(3);
            endReached = false;
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
            if (off + len > cbuf.length) {
                throw new IllegalArgumentException("Offset + Length cannot exceed size of buffer");
            }
            if (endReached) {
                return -1;
            }
            if (currentPos >= buffer.length) {
                fill();
            }
            int charsRead = 0;
            for (int i = off; i < off + len; i++) {
                if (currentPos >= buffer.length) {
                    fill();
                }
                if (buffer[currentPos] == '\n' || currentPos > limit) {
                    endReached = true;
                    charsRead++;
                    currentPos++;
                    return charsRead;
                } else {
                    cbuf[i] = buffer[currentPos];
                    charsRead++;
                }
                currentPos++;
            }
            return charsRead;
        }

        @Override
        public void close() throws IOException {

        }
    }
}
