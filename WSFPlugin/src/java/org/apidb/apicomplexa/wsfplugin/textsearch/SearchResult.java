/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.textsearch;


/**
 * @author John I
 * @created Nov 16, 2008
 */
public class SearchResult {


    private String source_id; 
    private float max_score; 
    private String fields_matched; 
    private String snippet;
 
    public SearchResult(String source_id, float max_score, String fields_matched, String snippet) {
	this.source_id = source_id;
	this.max_score = max_score;
	fields_matched = fields_matched;
	this.snippet = snippet;

    }

    protected void combine(SearchResult other) {
	if (other.getMaxScore() > max_score) {
	    max_score = other.getMaxScore();
	    snippet = other.getSnippet();
	    fields_matched = other.getFieldsMatched() + fields_matched;
	} else {
	    //	    fields_matched.append(other.getFieldsMatched()), if fields_matched were a StringBuffer
	    fields_matched = fields_matched + other.getFieldsMatched();
	}

    }

    protected float getMaxScore() {
	return max_score;
    }

    protected String getSnippet() {
	return snippet;
    }

    protected String getFieldsMatched() {
	return fields_matched;
    }

}
