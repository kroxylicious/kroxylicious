/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms;

/**
 * General exception Facades can throw
 */
public class TestKmsFacadeException extends RuntimeException {
    /**
     * Creates the exception.
     *
     * @param e cause of this exception.
     */
    public TestKmsFacadeException(Exception e) {
        super(e);
    }
}
