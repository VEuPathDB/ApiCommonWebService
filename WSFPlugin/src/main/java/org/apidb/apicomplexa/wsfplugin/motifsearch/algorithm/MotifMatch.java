package org.apidb.apicomplexa.wsfplugin.motifsearch.algorithm;


public class MotifMatch {
    private String leadingContext;
    private String match;
    private String trailingContext;
    private int startPos;
    private int endPos;

    public MotifMatch(String leadingContext,
                      String match,
                      String trailingContext,
                      int startPos,
                      int endPos) {
        this.leadingContext = leadingContext;
        this.match = match;
        this.trailingContext = trailingContext;
        this.startPos = startPos;
        this.endPos = endPos;
    }

    private MotifMatch(Builder builder) {
        leadingContext = builder.leadingContext;
        match = builder.match;
        trailingContext = builder.trailingContext;
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

        public Builder startPos(int val) {
            startPos = val;
            return this;
        }

        public Builder endPos(int val) {
            endPos = val;
            return this;
        }

        public MotifMatch build() {
            return new MotifMatch(this);
        }
    }
}
