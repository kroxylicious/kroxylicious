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
     * Tag for test suites that ignores the KAFKA_CLIENT environmental variable.
     */
    public static final String EXTERNAL_KAFKA_CLIENTS = "externalKafkaClients";

    /**
     * Tag for test suites that run operator only related tests (no kafka involved).
     */
    public static final String OPERATOR = "operator";
}
