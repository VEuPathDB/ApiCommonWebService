package org.apidb.apicomplexa.wsfplugin.motifsearch.poc;


public class MatchWithContext {
    private String leadingContext;
    private String match;
    private String trailingContext;
    private String sequenceId;
    private int startPos;
    private int endPos;

    public MatchWithContext(String leadingContext,
                            String match,
                            String trailingContext,
                            String sequenceId,
                            int startPos,
                            int endPos) {
        this.leadingContext = leadingContext;
        this.match = match;
        this.trailingContext = trailingContext;
        this.sequenceId = sequenceId;
        this.startPos = startPos;
        this.endPos = endPos;
    }

    private MatchWithContext(Builder builder) {
        leadingContext = builder.leadingContext;
        match = builder.match;
        trailingContext = builder.trailingContext;
        sequenceId = builder.sequenceId;
        startPos = builder.startPos;
        endPos = builder.endPos;
    }

    public String getLeadingContext() {
        return leadingContext;
    }

    public String getMatch() {
        return match;
    }

    public String getTrailingContext() {
        return trailingContext;
    }

    public String getSequenceId() {
        return sequenceId;
    }

    public int getStartPos() {
        return startPos;
    }

    public int getEndPos() {
        return endPos;
    }

    public static final class Builder {
        private String leadingContext;
        private String match;
        private String trailingContext;
        private String sequenceId;
        private int startPos;
        private int endPos;

        public Builder() {
        }

        public Builder leadingContext(String val) {
            leadingContext = val;
            return this;
        }

        public Builder match(String val) {
            match = val;
            return this;
        }

        public Builder trailingContext(String val) {
            trailingContext = val;
            return this;
        }

        public Builder sequenceId(String val) {
            sequenceId = val;
            return this;
        }

        public Builder startPos(int val) {
            startPos = val;
            return this;
        }

        public Builder endPos(int val) {
            endPos = val;
            return this;
        }

        public MatchWithContext build() {
            return new MatchWithContext(this);
        }
    }
}
