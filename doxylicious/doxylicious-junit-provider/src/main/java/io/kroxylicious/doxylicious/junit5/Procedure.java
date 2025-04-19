/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.junit5;

public interface Procedure {

    /**
     * Execute the procedure
     */
    void executeProcedure();

    /**
     * Execute the verification, asserting that it was successful
     */
    void assertVerification();
}
