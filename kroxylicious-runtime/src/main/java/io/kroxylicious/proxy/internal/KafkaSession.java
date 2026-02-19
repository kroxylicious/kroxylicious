/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.UUID;

import io.netty.handler.codec.haproxy.HAProxyMessage;

import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

public class KafkaSession {
    private final String sessionId;
    private KafkaSessionState currentState;

    /**
     * The HAProxy PROXY-protocol message received for this session, if any.
     * Captured by {@link HAProxyMessageHandler} before the SSL/binding handshake
     * completes so it is available when the state machine becomes active.
     *
     * TODO: replace with a parsed "HaProxyConnectionInfo" record holding
     *       sourceAddress, sourcePort, destinationAddress, destinationPort, etc.
     *       (and optionally HAProxy v2 TLV extensions) so we are not storing
     *       a Netty reference-counted object beyond channelRead.
     */
    @Nullable
    private HAProxyMessage haProxyMessage;

    public KafkaSession(KafkaSessionState currentState) {
        this(null, currentState);
    }

    @VisibleForTesting
    KafkaSession(@Nullable String sessionId, KafkaSessionState currentState) {
        if (Objects.isNull(sessionId)) {
            this.sessionId = UUID.randomUUID().toString();
        }
        else {
            this.sessionId = sessionId;
        }
        this.currentState = currentState;
    }

    public KafkaSession transitionTo(KafkaSessionState newState) {
        this.currentState = newState;
        return this;
    }

    public String sessionId() {
        return sessionId;
    }

    public KafkaSessionState currentState() {
        return currentState;
    }

    /**
     * Record the HAProxy PROXY-protocol message for this session.
     * Called by {@link HAProxyMessageHandler} when the PROXY header is decoded.
     *
     * @param msg the decoded HAProxy message
     */
    public void onHaProxyMessage(HAProxyMessage msg) {
        this.haProxyMessage = msg;
    }

    /**
     * @return the HAProxy message received for this session, or {@code null} if none.
     */
    @Nullable
    public HAProxyMessage haProxyMessage() {
        return haProxyMessage;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (KafkaSession) obj;
        return Objects.equals(this.sessionId, that.sessionId) &&
                Objects.equals(this.currentState, that.currentState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, currentState);
    }

    @Override
    public String toString() {
        return "KafkaSession[" +
                "sessionId=" + sessionId + ", " +
                "currentState=" + currentState + ']';
    }

}
