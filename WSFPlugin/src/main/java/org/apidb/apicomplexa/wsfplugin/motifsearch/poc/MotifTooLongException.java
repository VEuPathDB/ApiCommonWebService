package org.apidb.apicomplexa.wsfplugin.motifsearch.poc;

/**
 * Exception indicating that a motif match exceeds the maximum length.
 */
public class MotifTooLongException extends Exception {
    public MotifTooLongException(String message) {
        super(message);
    }
}
