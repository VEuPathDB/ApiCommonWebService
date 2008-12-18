/**
 * 
 */
package org.apidb.apicomplexa.wsfplugin.textsearch;

/**
 * @author John I
 * @created Nov 16, 2008
 */
public class SearchResult implements Comparable <SearchResult> {


    private String sourceId;
    private String projectId;
    private float maxScore; 
    private String fieldsMatched; 
    private String snippet;

    public SearchResult(String projectId, String sourceId, float maxScore, String fieldsMatched, String snippet) {
	this.sourceId = sourceId;
	this.projectId = projectId;
	this.maxScore = maxScore;
	this.fieldsMatched = fieldsMatched;
	this.snippet = snippet;
    }

    protected float getMaxScore() {
	return maxScore;
    }

    protected String getSnippet() {
	return snippet;
    }

    protected String getSourceId() {
	return sourceId;
    }

    protected String getProjectId() {
	return projectId;
    }

    protected String getFieldsMatched() {
	return fieldsMatched;
    }

    protected void combine(SearchResult other) {
	if (other.getMaxScore() > maxScore) {
	    maxScore = other.getMaxScore();
	    snippet = other.getSnippet();
	    fieldsMatched = other.getFieldsMatched() + fieldsMatched;
	} else {
	    //	    fieldsMatched.append(other.getFieldsMatched()), if fieldsMatched were a StringBuffer
	    fieldsMatched = fieldsMatched + other.getFieldsMatched();
	}

    }

    public int compareTo(SearchResult other) {


	if (other.getMaxScore() > maxScore || (other.getMaxScore() == maxScore && sourceId.compareTo(other.getSourceId()) < 0)) {
	    return -1;
	} else if(other.getMaxScore() < maxScore || (other.getMaxScore() == maxScore && sourceId.compareTo(other.getSourceId()) > 0)) {
	    return 1;
	} else {
	    return 1;
	}
    }

}
