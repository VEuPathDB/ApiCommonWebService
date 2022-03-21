package org.apidb.apicomplexa.wsfplugin.motifsearch.exception;

import org.gusdb.wsf.plugin.PluginModelException;

/**
 * Exception indicating that a motif match exceeds the maximum length.
 */
public class MotifTooLongException extends PluginModelException {
    public MotifTooLongException(String message) {
        super(message);
    }
}
