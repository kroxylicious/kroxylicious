/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.schema.compiler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used to track errors and warnings emitted by the tool, to avoid fail fast behaviour.
 */
public class Diagnostics {
    private static final Logger LOGGER = LoggerFactory.getLogger(Diagnostics.class);

    private int numFatals = 0;
    private int numErrors = 0;
    private int numWarnings = 0;

    Diagnostics() {

    }

    /**
     * A fatal error is used when the input YAML schema is so malformed it's impossible to continue
     * {@link #reportError(String, Object...)} is preferred
     * @param message
     * @param arguments
     */
    void reportFatal(String message, Object... arguments) {
        LOGGER.error(message, arguments);
        numFatals++;
        throw new FatalException();
    }

    /**
     * An error is used when the input YAML schema is so malformed, but it's impossible to continue.
     * Use {@link #reportFatal(String, Object...)} is it's impossible to continue compilation.
     * @param message
     * @param arguments
     */
    void reportError(String message, Object... arguments) {
        LOGGER.error(message, arguments);
        numErrors++;
    }

    /**
     * A warning is used when the input YAML schema is likely to be wrong (more likely than not), but we think
     * we're generating good code.
     * @param message
     * @param arguments
     */
    void reportWarning(String message, Object... arguments) {
        LOGGER.warn(message, arguments);
        numWarnings++;
    }

    public int getNumErrors() {
        return numErrors;
    }

    public int getNumFatals() {
        return numFatals;
    }

    public int getNumWarnings() {
        return numWarnings;
    }
}
