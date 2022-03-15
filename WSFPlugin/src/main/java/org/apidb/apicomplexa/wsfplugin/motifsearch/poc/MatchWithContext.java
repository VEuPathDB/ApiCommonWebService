package main.java.org.apidb.apicomplexa.wsfplugin.motifsearch.poc;

public class MatchWithContext {
    private String leadingContext;
    private String match;
    private String trailingContext;

    public MatchWithContext(String leadingContext, String match, String trailingContext) {
        this.leadingContext = leadingContext;
        this.match = match;
        this.trailingContext = trailingContext;
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
}
