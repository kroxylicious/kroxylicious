/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.sasl.termination.mechanism.AuthenticationResult;
import io.kroxylicious.filter.sasl.termination.mechanism.MechanismHandler;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStore;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

class StateTest {

    @Test
    void shouldStartInRequiringHandshakeState() {
        State state = State.start();

        assertThat(state).isInstanceOf(State.RequiringHandshake.class);
        assertThat(state.isAuthenticated()).isFalse();
        assertThat(state.isFailed()).isFalse();
        assertThat(state.isTerminal()).isFalse();
    }

    @Test
    void shouldTransitionFromHandshakeToAuthenticate() {
        State.RequiringHandshake initial = State.start();
        MechanismHandler handler = new TestMechanismHandler("SCRAM-SHA-256");

        State.RequiringAuthenticate next = initial.nextState(handler);

        assertThat(next.mechanismHandler()).isSameAs(handler);
        assertThat(next.isAuthenticated()).isFalse();
        assertThat(next.isFailed()).isFalse();
        assertThat(next.isTerminal()).isFalse();
    }

    @Test
    void shouldStayInAuthenticateForChallenge() {
        State.RequiringHandshake initial = State.start();
        MechanismHandler handler = new TestMechanismHandler("SCRAM-SHA-256");
        State.RequiringAuthenticate authenticating = initial.nextState(handler);

        State.RequiringAuthenticate nextRound = authenticating.nextStateChallenge();

        // Should return same instance (stays in same state)
        assertThat(nextRound).isSameAs(authenticating);
        assertThat(nextRound.mechanismHandler()).isSameAs(handler);
    }

    @Test
    void shouldTransitionToAuthenticatedOnSuccess() {
        State.RequiringHandshake initial = State.start();
        State.RequiringAuthenticate authenticating = initial.nextState(new TestMechanismHandler("SCRAM-SHA-256"));

        State.Authenticated authenticated = authenticating.nextStateSuccess("alice");

        assertThat(authenticated.authorizationId()).isEqualTo("alice");
        assertThat(authenticated.isAuthenticated()).isTrue();
        assertThat(authenticated.isFailed()).isFalse();
        assertThat(authenticated.isTerminal()).isTrue();
    }

    @Test
    void shouldTransitionToFailedOnFailure() {
        State.RequiringHandshake initial = State.start();
        State.RequiringAuthenticate authenticating = initial.nextState(new TestMechanismHandler("SCRAM-SHA-256"));

        State.Failed failed = authenticating.nextStateFailure("Invalid credentials");

        assertThat(failed.errorMessage()).isEqualTo("Invalid credentials");
        assertThat(failed.isAuthenticated()).isFalse();
        assertThat(failed.isFailed()).isTrue();
        assertThat(failed.isTerminal()).isTrue();
    }

    @Test
    void shouldHandleMultiRoundAuthentication() {
        // Simulate SCRAM's multi-round exchange
        State.RequiringHandshake initial = State.start();
        MechanismHandler handler = new TestMechanismHandler("SCRAM-SHA-256");

        // Round 1
        State.RequiringAuthenticate round1 = initial.nextState(handler);
        assertThat(round1.isTerminal()).isFalse();

        // Round 2 (challenge)
        State.RequiringAuthenticate round2 = round1.nextStateChallenge();
        assertThat(round2).isSameAs(round1);
        assertThat(round2.isTerminal()).isFalse();

        // Final round (success)
        State.Authenticated authenticated = round2.nextStateSuccess("alice");
        assertThat(authenticated.isTerminal()).isTrue();
        assertThat(authenticated.authorizationId()).isEqualTo("alice");
    }

    @Test
    void shouldProvideReadableToString() {
        State.RequiringHandshake handshake = State.start();
        assertThat(handshake.toString()).isEqualTo("RequiringHandshake");

        State.RequiringAuthenticate authenticating = handshake.nextState(new TestMechanismHandler("SCRAM-SHA-256"));
        assertThat(authenticating.toString()).contains("RequiringAuthenticate").contains("SCRAM-SHA-256");

        State.Authenticated authenticated = authenticating.nextStateSuccess("alice");
        assertThat(authenticated.toString()).contains("Authenticated").contains("alice");

        State.Failed failed = authenticating.nextStateFailure("Bad password");
        assertThat(failed.toString()).contains("Failed").contains("Bad password");
    }

    @Test
    void shouldHandleNullErrorMessageInFailed() {
        State.RequiringHandshake initial = State.start();
        State.RequiringAuthenticate authenticating = initial.nextState(new TestMechanismHandler("SCRAM-SHA-256"));

        State.Failed failed = authenticating.nextStateFailure(null);

        assertThat(failed.errorMessage()).isNull();
        assertThat(failed.isFailed()).isTrue();
    }

    @Test
    void shouldDistinguishTerminalStates() {
        State.RequiringHandshake handshake = State.start();
        State.RequiringAuthenticate authenticating = handshake.nextState(new TestMechanismHandler("SCRAM-SHA-256"));
        State.Authenticated authenticated = authenticating.nextStateSuccess("alice");
        State.Failed failed = authenticating.nextStateFailure("error");

        // Non-terminal states
        assertThat(handshake.isTerminal()).isFalse();
        assertThat(authenticating.isTerminal()).isFalse();

        // Terminal states
        assertThat(authenticated.isTerminal()).isTrue();
        assertThat(failed.isTerminal()).isTrue();
    }

    // Test mechanism handler implementation
    private static class TestMechanismHandler implements MechanismHandler {
        private final String mechanismName;

        TestMechanismHandler(String mechanismName) {
            this.mechanismName = mechanismName;
        }

        @Override
        @NonNull
        public String mechanismName() {
            return mechanismName;
        }

        @Override
        @NonNull
        public CompletionStage<AuthenticationResult> handleAuthenticate(
                                                                        @NonNull byte[] authBytes,
                                                                        @NonNull ScramCredentialStore credentialStore) {
            return CompletableFuture.completedFuture(
                    AuthenticationResult.success(new byte[0], "test-user"));
        }

        @Override
        public void dispose() {
            // No-op for test
        }
    }
}
