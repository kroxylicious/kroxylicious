/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.UUID;

import edu.umd.cs.findbugs.annotations.Nullable;

record KafkaSession(String sessionId, KafkaSessionState currentState) {

    public KafkaSession in(KafkaSessionState newState) {
        return new KafkaSession(this.sessionId, newState);
    }

    KafkaSession(KafkaSessionState currentState) {
        this(null, currentState);
    }

    KafkaSession(@Nullable String sessionId, KafkaSessionState currentState) {
        if (Objects.isNull(sessionId)) {
            this.sessionId = UUID.randomUUID().toString();
        }
        else {
            this.sessionId = sessionId;
        }
        this.currentState = currentState;
    }
}
