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
}
