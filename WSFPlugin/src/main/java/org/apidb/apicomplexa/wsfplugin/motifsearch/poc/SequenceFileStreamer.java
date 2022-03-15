package main.java.org.apidb.apicomplexa.wsfplugin.motifsearch.poc;

import java.io.*;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SequenceFileStreamer implements AutoCloseable {
    private static final Pattern DEF_LINE_PATTERN = Pattern.compile("\\>([A-Za-z0-9_-]+) \\| strand=(.*) \\| organism=(.+) \\| version=(.+) \\| length=(\\d+) \\| SO=(.+)");

    private FastaInputStream currentStream;
    private FileReader fileReader;
    private BufferedReader bufferedReader;

    public SequenceFileStreamer(File input) throws FileNotFoundException {
        this.fileReader = new FileReader(input);
        this.bufferedReader = new BufferedReader(fileReader);
    }

    public Optional<FastaInputStream> nextSequence() throws IOException {
        final String defLine = bufferedReader.readLine();
        System.out.println("Defline: " + defLine);
        if (defLine == null) {
            return Optional.empty();
        }
        currentStream = new FastaInputStream(bufferedReader, defLine);
        return Optional.of(currentStream);
    }

    @Override
    public void close() throws Exception {
        fileReader.close();
        bufferedReader.close();
    }

    public static class FastaInputStream extends InputStream {
        private String currentSequenceId;
        private String currentStrand;
        private String currentOrganism;
        private BufferedReader bufferedReader;
        private int currentByte;

        public FastaInputStream(BufferedReader bufferedReader, String defLine) {
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

        /**
         * Ideally, all read methods would be functions of this method. This is challenging with all sequences
         * in the same file since we don't want to accidentally buffer a new line.
         */
        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return super.read(b, off, len);
        }

        @Override
        public int read() throws IOException {
            if (currentByte == -1) {
                return -1;
            }
            currentByte = bufferedReader.read();
            if (currentByte == '\n') {
                currentByte = -1;
                System.out.println("Finished sequence id " + currentSequenceId);
                close();
                return currentByte;
            }
            return currentByte;
        }
    }
}
