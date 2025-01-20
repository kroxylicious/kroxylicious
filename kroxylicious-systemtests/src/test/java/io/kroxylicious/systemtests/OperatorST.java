/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.kroxylicious.systemtests.installation.kroxylicious.KroxyliciousOperator;

import static io.kroxylicious.systemtests.TestTags.OPERATOR;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@Tag(OPERATOR)
class OperatorST extends AbstractST {
    protected static KroxyliciousOperator kroxyliciousOperator;

    @Test
    void operatorInstallation() {
        kroxyliciousOperator = new KroxyliciousOperator(Constants.KO_NAMESPACE);
        assertDoesNotThrow(() -> kroxyliciousOperator.deploy());
    }

    @AfterEach
    void afterEach() {
        kroxyliciousOperator.delete();
    }
}
