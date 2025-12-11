/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

public record KafkaSession(String sessionId, KafkaSessionState currentState) {
    public KafkaSession in(KafkaSessionState newState) {
        return new KafkaSession(this.sessionId, newState);
    }
}
