package org.apidb.apicomplexa.wsfplugin.motifsearch.poc;

import java.io.*;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TileMatcher {
    private static int MAX_MATCH_LENGTH = 1024;

    public static List<MatchWithContext> match(SequenceFileStreamer.FastaReader sequenceInput,
                                               Pattern pattern,
                                               int contextLength,
                                               Consumer<MatchWithContext> matchConsumer,
                                               int bufferSize) throws Exception {
        boolean first = true;
        boolean reachedNewline = false;
        final List<MatchWithContext> matches = new ArrayList<>();
        final Set<Integer> startPositions = new HashSet<>();
        final SequenceBuffer sequenceBuffer = new SequenceBuffer(MAX_MATCH_LENGTH, contextLength, bufferSize);
        int bytesRead;
            do {
                if (first) {
                    first = false;
                } else {
                    // Shift the buffer backward to make space for a bufferSize and an overlap window worth of new data
                    sequenceBuffer.shiftBuffers();
                }
                bytesRead = sequenceBuffer.read(sequenceInput);
                String subsequence = sequenceBuffer.readCurrentSubsequence();
                final Matcher matcher = pattern.matcher(subsequence);
                while (matcher.find()) {
                    boolean atEnd = bytesRead == -1 || reachedNewline;
                    if (matcher.start() > bufferSize + sequenceBuffer.getOverlapWindow() && !atEnd) {
                        break;
                    }
                    if (startPositions.contains(matcher.start() + sequenceBuffer.getSequencePosition())) {
                        continue;
                    }
                    if (matcher.group().length() > MAX_MATCH_LENGTH) {
                        throw new MotifTooLongException("Motif match cannot exceed " + MAX_MATCH_LENGTH + " chars.");
                    }
                    final String trailingContext = subsequence.substring(matcher.end(), Math.min(subsequence.length(), matcher.end() + contextLength));
                    final String leadingContext = matcher.start() > contextLength
                            ? subsequence.substring(matcher.start() - contextLength, matcher.start())
                            : sequenceBuffer.getLeadingContext(matcher, contextLength);
                    startPositions.add(matcher.start() + sequenceBuffer.getSequencePosition());
                    matchConsumer.accept(new MatchWithContext(
                            leadingContext, matcher.group(), trailingContext, sequenceInput.getCurrentSequenceId(),
                            matcher.start() + sequenceBuffer.getSequencePosition(),
                            matcher.end() + sequenceBuffer.getSequencePosition()));
                }
            } while (bytesRead != -1 && !reachedNewline);
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

        private final int overlapWindow;
        private final int totalBufferSize;
        private final int contextLength;
        private final int bufferSize;

        private boolean hasLeadingContext;
        private int sequencePosition = 0;

        public SequenceBuffer(int maxLength, int contextLength, int bufferSize) {
            this.bufferSize = bufferSize;
            this.overlapWindow =  2 * maxLength; // The leading overlap window only may only need to be equal to contextLength.
            this.contextLength = contextLength;
            this.totalBufferSize = 2 * overlapWindow + bufferSize;
            buffer1 = CharBuffer.allocate(totalBufferSize);
            buffer2 = CharBuffer.allocate(totalBufferSize);
            contextBuffer = CharBuffer.allocate(contextLength);
            currentBuffer = buffer1;
        }

        public int read(Reader reader) throws IOException {
            int bytesRead;
            do {
                bytesRead = reader.read(currentBuffer);
            } while (currentBuffer.remaining() != 0 && bytesRead != -1);
            return  bytesRead;
        }

        public String getLeadingContext(Matcher matcher, int contextLength) {
            if (!hasLeadingContext) {
                return "";
            }
            contextBuffer.position(contextLength - (contextLength - matcher.start()));
            return contextBuffer.toString();
        }

        public String readCurrentSubsequence() {
            // Reset to mark to only read starting from the end of the first overlap window.
            int limit = totalBufferSize - currentBuffer.remaining();
            currentBuffer.position(0);
            currentBuffer.limit(limit);
            return currentBuffer.toString();
        }

        public int getSequencePosition() {
            return sequencePosition;
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
                buffer2.put(buffer1.array(), bufferSize + overlapWindow, overlapWindow);
                contextBuffer.position(0);
                contextBuffer.put(buffer1.array(), totalBufferSize - contextLength, contextLength);
                currentBuffer = buffer2;
            } else {
                contextBuffer.position(0);
                buffer1.put(buffer2.array(), bufferSize + overlapWindow, overlapWindow);
                contextBuffer.position(0);
                contextBuffer.put(buffer2.array(), totalBufferSize - contextLength, contextLength);
                currentBuffer = buffer1;
            }
            sequencePosition += bufferSize + overlapWindow;
            hasLeadingContext = true;
            currentBuffer.position(overlapWindow);
            return currentBuffer;
        }
    }
}
