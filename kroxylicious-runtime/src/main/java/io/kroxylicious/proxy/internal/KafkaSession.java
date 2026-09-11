/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.UUID;

import io.kroxylicious.proxy.internal.net.HaProxyContext;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Represents a single client's proxy session, identified by a unique session ID and tracking
 * the session's {@link KafkaSessionState} (e.g. authentication progress) over its lifetime.
 */
public class KafkaSession {

    private final String sessionId;
    private KafkaSessionState currentState;

    /**
     * The HaProxy PROXY-protocol message received for this session, if any.
     * Captured by {@link HaProxyMessageHandler} before the SSL/binding handshake
     * completes so it is available when the state machine becomes active.
     */
    @Nullable
    private HaProxyContext haProxyContext;

    /**
     * Creates a session with a randomly generated session ID.
     * @param currentState the initial session state
     */
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

    /**
     * Transitions this session to the given state.
     * @param newState the state to transition to
     * @return this session
     */
    public KafkaSession transitionTo(KafkaSessionState newState) {
        this.currentState = newState;
        return this;
    }

    /**
     * Returns the unique identifier of this session.
     * @return the session ID
     */
    public String sessionId() {
        return sessionId;
    }

    /**
     * Returns the current state of this session.
     * @return the current session state
     */
    public KafkaSessionState currentState() {
        return currentState;
    }

    /**
     * Store the HaProxy PROXY-protocol context for this session.
     *
     * @param context the immutable context extracted from the decoded message
     */
    public void setHaProxyContext(HaProxyContext context) {
        haProxyContext = context;
    }

    /**
     * Returns the HA Proxy context received for this session, or {@code null} if none.
     * @return the HA Proxy context, or {@code null} if no PROXY-protocol message was received
     */
    @Nullable
    public HaProxyContext haProxyContext() {
        return haProxyContext;
    }

    @Override
    @SuppressWarnings("EqualsGetClass") // Internal session value type, and not final. Keeping getClass() means a future subclass can never
    // compare equal to a plain KafkaSession, which preserves the symmetry of equals().
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
