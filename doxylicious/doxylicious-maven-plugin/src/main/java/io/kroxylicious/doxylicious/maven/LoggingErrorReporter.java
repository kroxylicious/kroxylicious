/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.maven;

import org.apache.maven.plugin.logging.Log;

import io.kroxylicious.doxylicious.doc.ErrorReporter;

/**
 * Implementation of {@link ErrorReporter} using the maven log.
 */
class LoggingErrorReporter implements ErrorReporter {
    private final Log log;
    private int numErrors;

    LoggingErrorReporter(Log log) {
        this.log = log;
        this.numErrors = 0;
    }

    @Override
    public void reportError(String message) {
        log.error(message);
        numErrors += 1;
    }

    @Override
    public int numErrors() {
        return numErrors;
    }
}
