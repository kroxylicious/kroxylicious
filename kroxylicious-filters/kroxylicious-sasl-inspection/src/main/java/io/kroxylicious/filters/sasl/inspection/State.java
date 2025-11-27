/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

/**
 * State machine used that follows SASL handshaking and authentication exchange between client/server.
 *
 * <pre><code>
 *
 * START ───→ RequiringHandshakeRequest      AllowingHandshakeRequest ←────╮
 *                       │                                 │               │
 *                       │      ╭──────────────────────────╯               │
 *                       ↓      ↓                                          │
 *            AwaitingHandshakeResponse                                    │
 *                       │                                                 │
 *                       ↓                                                 │
 *      ╭───→ RequiringAuthenticateRequest                                 │
 *      │                │                                                 │
 *      │                ↓                                                 │
 *      │   AwaitingAuthenticateResponse                                   │
 *      │                │                                                 │
 *      ╰────────────────┼─────────────────────────────────────────────────╯
 *                       ↓
 *          DisallowingAuthenticateRequest
 *
 * </code></pre>
 *
 */
sealed interface State
        permits State.ExpectingHandshakeRequestState, State.AwaitingAuthenticateResponse, State.AwaitingHandshakeResponse, State.DisallowingAuthenticateRequest,
        State.RequiringAuthenticateRequest {

    static RequiringHandshakeRequest start() {
        return new RequiringHandshakeRequest();
    }

    default boolean clientIsAuthenticated() {
        return false;
    }

    sealed interface ExpectingHandshakeRequestState extends State permits RequiringHandshakeRequest, AllowingHandshakeRequest {
        /**
         * Transition to the next state.
         *
         * @param saslObserver sasl observer
         * @return the awaiting handshake response state.
         */
        AwaitingHandshakeResponse nextState(SaslObserver saslObserver);
    }

    /** A SASL handshake request is required. */
    final class RequiringHandshakeRequest implements ExpectingHandshakeRequestState {
        private RequiringHandshakeRequest() {
        }

        /**
         * Transition to the next state.
         *
         * @param saslObserver sasl observer
         * @return the awaiting handshake response state.
         */
        @Override
        public AwaitingHandshakeResponse nextState(SaslObserver saslObserver) {
            return new AwaitingHandshakeResponse(saslObserver, NegotiationType.INITIAL);
        }
    }

    /** We're waiting for a SASL handshake response from the server. */
    final class AwaitingHandshakeResponse extends SaslObserverCarrier implements State {
        private final NegotiationType negotiationType;

        private AwaitingHandshakeResponse(SaslObserver saslObserver, NegotiationType negotiationType) {
            super(saslObserver);
            this.negotiationType = negotiationType;
        }

        /**
         * Transition to the next state.
         *
         * @return the requiring authenticate request state.
         */
        public RequiringAuthenticateRequest nextState() {
            return new RequiringAuthenticateRequest(saslObserver(), negotiationType);
        }

    }

    /**
     * A SASL authenticate request is required.
     */
    final class RequiringAuthenticateRequest extends SaslObserverCarrier implements State {
        private final NegotiationType negotiationType;

        private RequiringAuthenticateRequest(SaslObserver saslObserver, NegotiationType negotiationType) {
            super(saslObserver);
            this.negotiationType = negotiationType;
        }

        /**
         * Transition to the next state.
         *
         * @param authRequestApiSupportsReauth true if the request indicates the broker/client support KIP-368.
         * @return the awaiting authenticate request state.
         */
        public AwaitingAuthenticateResponse nextState(boolean authRequestApiSupportsReauth) {
            // Flag indicating if the client and broker supports re-authentication (KIP-368). If this is not the first
            // authentication request, or the apiVersion is > 0 we know that the client supports reauth.
            var clientSupportsReauthentication = negotiationType == NegotiationType.REAUTH || authRequestApiSupportsReauth;
            return new AwaitingAuthenticateResponse(saslObserver(), negotiationType, clientSupportsReauthentication);
        }
    }

    /**
     * We're waiting for a SASL authenticate response from the server
     */
    final class AwaitingAuthenticateResponse extends SaslObserverCarrier implements State {
        private final NegotiationType negotiationType;
        private final boolean clientSupportsReauthentication;

        private AwaitingAuthenticateResponse(SaslObserver saslObserver, NegotiationType negotiationType, boolean clientSupportsReauthentication) {
            super(saslObserver);
            this.negotiationType = negotiationType;
            this.clientSupportsReauthentication = clientSupportsReauthentication;
        }

        /**
         * Transition to the next state.
         * @param saslFinished true if sasl negotiation finished
         * @return the allowing or disallowing authenticate request state.
         */
        State nextState(boolean saslFinished) {
            if (saslFinished) {
                if (clientSupportsReauthentication) {
                    return new AllowingHandshakeRequest();
                }
                else {
                    return new DisallowingAuthenticateRequest();
                }
            }
            else {
                return new RequiringAuthenticateRequest(saslObserver(), negotiationType);
            }
        }
    }

    /**
     * Authentication has been successful and a future SASL authenticate request is allowed for reauthentication.
     * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-368%3A+Allow+SASL+Connections+to+Periodically+Re-Authenticate">KIP-368</a>.
     */
    final class AllowingHandshakeRequest implements ExpectingHandshakeRequestState {
        private AllowingHandshakeRequest() {
        }

        @Override
        public boolean clientIsAuthenticated() {
            return true;
        }

        /**
         * Transition to the next state.
         *
         * @param saslObserver sasl observer
         * @return the awaiting handshake response state.
         */
        @Override
        public AwaitingHandshakeResponse nextState(SaslObserver saslObserver) {
            return new AwaitingHandshakeResponse(saslObserver, NegotiationType.REAUTH);
        }

    }

    /**
     * Authentication has been successful, but no future SASL authenticate request is allowed.
     * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-368%3A+Allow+SASL+Connections+to+Periodically+Re-Authenticate">KIP-368</a>.
     */
    final class DisallowingAuthenticateRequest implements State {
        private DisallowingAuthenticateRequest() {
        }
        @Override
        public boolean clientIsAuthenticated() {
            return true;
        }
    }

    enum NegotiationType {
        // the first SASL negotiation
        INITIAL,
        // subsequent SASL negotiations when the client/broker support re-auth per KIP-368
        REAUTH
    }

    abstract class SaslObserverCarrier {
        private final SaslObserver saslObserver;

        SaslObserverCarrier(SaslObserver saslObserver) {
            this.saslObserver = saslObserver;
        }

        final SaslObserver saslObserver() {
            return saslObserver;
        }
    }
}
