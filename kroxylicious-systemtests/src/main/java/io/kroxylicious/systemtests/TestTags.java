/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

public final class TestTags {

    private TestTags() {
    }

    /**
     * Tag for test suites that do not depend on the kafka client used.
     */
    public static final String KAFKA_CLIENT_INDEPENDENT = "kafkaClientIndependent";
}
