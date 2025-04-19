/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.doc;

/**
 * Abstracts error reporting.
 */
public interface ErrorReporter {

    /**
     * Report an error.
     * @param message The error message.
     */
    void reportError(String message);

    /**
     * @return The number of errors reported so far.
     */
    int numErrors();
}
