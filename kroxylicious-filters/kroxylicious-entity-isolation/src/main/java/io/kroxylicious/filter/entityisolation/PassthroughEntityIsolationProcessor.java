/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Passthrough entity isolation processor that performs no entity isolation.
 * It exists so that the filter can exhibit fail-closed behaviour when it encounters
 * an API key or API version that is unknown to the filter.
 */
record PassthroughEntityIsolationProcessor<Q extends ApiMessage, S extends ApiMessage>(short minSupportedVersion, short maxSupportedVersion)
        implements EntityIsolationProcessor<Q, S, Void> {}
