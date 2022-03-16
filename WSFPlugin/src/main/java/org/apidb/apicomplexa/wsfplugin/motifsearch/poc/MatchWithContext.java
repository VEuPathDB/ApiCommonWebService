package org.apidb.apicomplexa.wsfplugin.motifsearch.poc;

public class MatchWithContext {
    private String leadingContext;
    private String match;
    private String trailingContext;
    private String sequenceId;

    public MatchWithContext(String leadingContext, String match, String trailingContext, String sequenceId) {
        this.leadingContext = leadingContext;
        this.match = match;
        this.trailingContext = trailingContext;
        this.sequenceId = sequenceId;
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
