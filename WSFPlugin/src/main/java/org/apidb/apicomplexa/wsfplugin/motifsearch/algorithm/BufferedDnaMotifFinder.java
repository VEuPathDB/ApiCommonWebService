package org.apidb.apicomplexa.wsfplugin.motifsearch.algorithm;

import org.apidb.apicomplexa.wsfplugin.motifsearch.exception.MotifTooLongException;

import java.io.*;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Finds bounded-length motifs in a DNA sequence. This bounds the amount of memory used for scalability, as practically
 * we do not need to support large motifs.
 */
public class BufferedDnaMotifFinder {
    /**
     *
     * @param sequenceInput A FastaReader containing exclusively sequence data.
     * @param pattern Pattern to match against the sequenceInput.
     * @param contextLength The amount of context returned on either end of the match.
     * @param bufferSize The total size that will be buffered into memory at once.
     * @throws MotifTooLongException If the motif match exceeds {@param maxMatchLength} characters in length.
     */
    public static List<MotifMatch> match(Reader sequenceInput,
                                         Pattern pattern,
                                         int contextLength,
                                         int bufferSize,
                                         int maxMatchLength) throws MotifTooLongException, IOException {
        boolean first = true;
        boolean reachedNewline = false;
        final List<MotifMatch> motifMatches = new ArrayList<>();
        final Set<Integer> startPositions = new HashSet<>();
        final SequenceBuffer sequenceBuffer = new SequenceBuffer(maxMatchLength, contextLength, bufferSize);
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
                // With the overlapping buffers, it's possible for us to come across the same match twice.
                if (startPositions.contains(matcher.start() + sequenceBuffer.getSequencePosition())) {
                    continue;
                }
                if (matcher.group().length() > maxMatchLength) {
                    throw new MotifTooLongException("Motif match cannot exceed " + maxMatchLength + " chars.");
                }
                final String trailingContext = subsequence.substring(matcher.end(), Math.min(subsequence.length(), matcher.end() + contextLength));
                final String leadingContext = matcher.start() > contextLength
                        ? subsequence.substring(matcher.start() - contextLength, matcher.start())
                        : sequenceBuffer.getLeadingContext(matcher, contextLength);
                startPositions.add(matcher.start() + sequenceBuffer.getSequencePosition());
                motifMatches.add(new MotifMatch.Builder()
                        .match(matcher.group())
                        .startPos(matcher.start() + sequenceBuffer.getSequencePosition())
                        .endPos(matcher.end() + sequenceBuffer.getSequencePosition())
                        .leadingContext(leadingContext)
                        .trailingContext(trailingContext)
                        .build());
            }
        } while (bytesRead != -1 && !reachedNewline);
        return motifMatches;
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
            this.overlapWindow =  2 * maxLength; // The leading overlap window only may only need to be equal to contextLength.
            this.contextLength = contextLength;
            this.bufferSize = bufferSize;
            this.totalBufferSize = 2 * overlapWindow + bufferSize;
            this.buffer1 = CharBuffer.allocate(totalBufferSize);
            this.buffer2 = CharBuffer.allocate(totalBufferSize);
            this.contextBuffer = CharBuffer.allocate(contextLength);
            this.currentBuffer = buffer1;
        }

        public int read(Reader reader) throws IOException {
            int bytesRead;
            do {
                bytesRead = reader.read(currentBuffer);
            } while (currentBuffer.remaining() != 0 && bytesRead != -1);
            return  bytesRead;
        }

        /**
         * Returns leading context from a dedicated buffer that keeps track of previously seen characters.
         */
        public String getLeadingContext(Matcher matcher, int contextLength) {
            if (!hasLeadingContext) {
                return "";
            }
            contextBuffer.position(contextLength - (contextLength - matcher.start()));
            return contextBuffer.toString();
        }

        public String readCurrentSubsequence() {
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
