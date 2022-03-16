package org.apidb.apicomplexa.wsfplugin.motifsearch.poc;

import java.io.*;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import java.io.*;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TileMatcher {
    private static int BUFFER_SIZE = 8192;
    private static int MAX_MATCH_LENGTH = 1024;

    public static List<MatchWithContext> match(SequenceFileStreamer.FastaInputStream sequenceInput,
                                               Pattern pattern,
                                               int contextLength,
                                               Consumer<MatchWithContext> matchConsumer) throws Exception {
        boolean first = true;
        boolean reachedNewline = false;
        final List<MatchWithContext> matches = new ArrayList<>();
        final SequenceBuffer sequenceBuffer = new SequenceBuffer(MAX_MATCH_LENGTH, contextLength);
        int bytesRead;
        try (final Reader streamReader = new InputStreamReader(sequenceInput);
             final BufferedReader bufferedReader = new BufferedReader(streamReader, sequenceBuffer.getTotalBufferSize())) {
            do {
                if (first) {
                    first = false;
                } else {
                    // Shift the buffer backward to make space for a BUFFER_SIZE and an overlap window worth of new data
                    sequenceBuffer.shiftBuffers();
                }

                // Read data into shifted buffer until buffer is full.
                do {
                    bytesRead = bufferedReader.read(sequenceBuffer.getCurrentBuffer());
                } while (sequenceBuffer.getCurrentBuffer().remaining() != 0 && bytesRead != -1);

                String subsequence = sequenceBuffer.readCurrentSubsequence();
//                if (subsequence.contains(System.lineSeparator())) {
//                    subsequence = subsequence.split(System.lineSeparator())[0];
//                    reachedNewline = true;
//                }

                final Matcher matcher = pattern.matcher(subsequence);
                while (matcher.find()) {
                    boolean atEnd = bytesRead == -1 || reachedNewline;
                    if (matcher.start() > BUFFER_SIZE + sequenceBuffer.getOverlapWindow() && !atEnd) {
                        break;
                    }
                    if (matcher.group().length() > MAX_MATCH_LENGTH) {
                        throw new MotifTooLongException("Motif match cannot exceed " + MAX_MATCH_LENGTH + " chars.");
                    }
                    final String trailingContext = subsequence.substring(matcher.end(), Math.min(subsequence.length(), matcher.end() + contextLength));
                    final String leadingContext = matcher.start() > contextLength
                            ? subsequence.substring(matcher.start() - contextLength, matcher.start())
                            : sequenceBuffer.getLeadingContext(matcher, contextLength);
                    matchConsumer.accept(new MatchWithContext(
                            leadingContext, matcher.group(), trailingContext, sequenceInput.getCurrentSequenceId()));
                }
            } while (bytesRead != -1 && !reachedNewline);
        }
        return matches;
    }


    /**
     * Utility class used for "Shifting" a CharBuffer without re-creating it for memory efficiency.
     */
    private static class SequenceBuffer {
        private CharBuffer buffer1;
        private CharBuffer buffer2;
        private CharBuffer currentBuffer;
        private CharBuffer contextBuffer;

        private int overlapWindow;
        private int totalBufferSize;
        private int contextLength;
        private boolean hasLeadingContext;

        public SequenceBuffer(int maxLength, int contextLength) {
            this.overlapWindow =  2 * maxLength; // The leading overlap window only may only need to be equal to contextLength.
            this.contextLength = contextLength;
            this.totalBufferSize = 2 * overlapWindow + BUFFER_SIZE;
            buffer1 = CharBuffer.allocate(totalBufferSize);
            buffer2 = CharBuffer.allocate(totalBufferSize);
            contextBuffer = CharBuffer.allocate(contextLength);
            currentBuffer = buffer1;
        }

        public String getLeadingContext(Matcher matcher, int contextLength) {
            if (!hasLeadingContext) {
                return "";
            }
            contextBuffer.position(contextLength - (contextLength - matcher.start()));
            return contextBuffer.toString();
//            final int position = currentBuffer.position();
//            final int limit = currentBuffer.limit();
//            currentBuffer.position((overlapWindow - contextLength + matcher.start()));
//            currentBuffer.limit((overlapWindow - contextLength + matcher.start()) + contextLength);
//            String leadingContext = currentBuffer.toString();
//            currentBuffer.limit(limit);
//            currentBuffer.position(position);
//            return leadingContext;
        }

        public String readCurrentSubsequence() {
            // Reset to mark to only read starting from the end of the first overlap window.
            int limit = totalBufferSize - currentBuffer.remaining();
            currentBuffer.position(0);
            currentBuffer.limit(limit);
            return currentBuffer.toString();
        }

        public CharBuffer getCurrentBuffer() {
            return currentBuffer;
        }

        public int getOverlapWindow() {
            return overlapWindow;
        }

        public int getTotalBufferSize() {
            return totalBufferSize;
        }

        public CharBuffer shiftBuffers() {
            if (buffer1 == currentBuffer) {
                contextBuffer.position(0);
                buffer2.put(buffer1.array(), BUFFER_SIZE + overlapWindow, overlapWindow);
                contextBuffer.position(0);
                contextBuffer.put(buffer1.array(), totalBufferSize - contextLength, contextLength);
                currentBuffer = buffer2;
            } else {
                contextBuffer.position(0);
                buffer1.put(buffer2.array(), BUFFER_SIZE + overlapWindow, overlapWindow);
                contextBuffer.position(0);
                contextBuffer.put(buffer2.array(), totalBufferSize - contextLength, contextLength);
                currentBuffer = buffer1;
            }
            hasLeadingContext = true;
            currentBuffer.position(overlapWindow);
            return currentBuffer;
        }
    }
}
