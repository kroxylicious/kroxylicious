/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

public record KafkaSession(String sessionId, SessionState currentState) {
    public enum SessionState {
        PRE_AUTHENTICATION,
        AUTHENTICATED,
        AUTHORIZED
    }
}
