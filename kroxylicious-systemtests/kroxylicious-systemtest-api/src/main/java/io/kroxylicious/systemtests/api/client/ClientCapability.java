/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.client;

/**
 * Optional operations a {@link KafkaClient} may support beyond the mandatory
 * produce, consume and topic creation.
 * <p>
 * CLI-based drivers typically support only the mandatory operations; in-process drivers
 * support the full surface. Tests that rely on an optional operation should check
 * {@link KafkaClient#supports(ClientCapability)} and skip when it is absent.
 */
public enum ClientCapability {

    /** Producing records carrying headers, see {@link ProduceRecord#headers()}. */
    RECORD_HEADERS,

    /** {@link KafkaClient#produceInTransaction(String, String, java.util.List)}. */
    TRANSACTIONS,

    /** {@link KafkaClient#commitOffsets(String, String, java.util.Map)}. */
    OFFSET_COMMIT
}
