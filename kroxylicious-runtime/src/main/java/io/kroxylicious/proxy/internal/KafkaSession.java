/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.UUID;

import io.netty.handler.codec.haproxy.HAProxyMessage;

import io.kroxylicious.proxy.internal.net.HaProxyContext;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

public class KafkaSession {

    private final String sessionId;
    private KafkaSessionState currentState;

    /**
     * The HaProxy PROXY-protocol message received for this session, if any.
     * Captured by {@link HAProxyMessageHandler} before the SSL/binding handshake
     * completes so it is available when the state machine becomes active.
     */
    @Nullable
    private HaProxyContext haProxyContext;

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
     * Extract connection metadata from the HaProxy PROXY-protocol message
     * and store it as the session's {@link HaProxyContext}.
     * <p>
     * The {@link HaProxyContext} deep-copies all fields (including TLV content)
     * so it remains valid regardless of the raw message's reference-count lifecycle.
     * The raw message is <b>not</b> released here — Netty's pipeline manages that.
     * </p>
     *
     * @param msg the decoded HaProxy message
     */
    public void setHAProxyContext(HAProxyMessage msg) {
        haProxyContext = HaProxyContext.from(msg);
    }

    /**
     * @return the HaProxy message received for this session, or {@code null} if none.
     */
    @Nullable
    public HaProxyContext haProxyContext() {
        return haProxyContext;
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
