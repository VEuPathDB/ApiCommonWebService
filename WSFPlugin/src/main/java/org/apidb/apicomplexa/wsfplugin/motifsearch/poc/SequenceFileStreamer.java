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
    private BufferedReader bufferedReader;

    public SequenceFileStreamer(File input) throws FileNotFoundException {
        this.fileReader = new FileReader(input);
        this.bufferedReader = new BufferedReader(fileReader);
    }

    public Optional<FastaReader> nextSequence() throws IOException {
        final String defLine = bufferedReader.readLine();
        if (defLine == null) {
            return Optional.empty();
        }
        currentStream = new FastaReader(bufferedReader, defLine);
        return Optional.of(currentStream);
    }

    @Override
    public void close() throws Exception {
        fileReader.close();
        bufferedReader.close();
    }

    public class FastaReader extends Reader {
        private String currentSequenceId;
        private String currentStrand;
        private String currentOrganism;
        private BufferedReader bufferedReader;
        private int currentByte;

        public FastaReader(BufferedReader bufferedReader, String defLine) {
            this.bufferedReader = bufferedReader;
            final Matcher defLineMatcher = DEF_LINE_PATTERN.matcher(defLine);
            if (!defLineMatcher.find()) {
                throw new RuntimeException("Cannot read definition line " + defLine);
            }
            this.currentSequenceId = defLineMatcher.group(1);
            this.currentStrand = defLineMatcher.group(2);
            this.currentOrganism = defLineMatcher.group(3);
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
            int charsRead = 0;
            if (currentByte == -1) {
                return -1;
            }
            for (int i = off; i < off + len; i++) {
                currentByte = bufferedReader.read();
                if (currentByte == '\n') {
                    currentByte = -1;
                    return charsRead;
                }
                cbuf[i] = (char) currentByte;
                charsRead++;
            }
            return charsRead;
        }

        @Override
        public void close() throws IOException {

        }
    }
}
