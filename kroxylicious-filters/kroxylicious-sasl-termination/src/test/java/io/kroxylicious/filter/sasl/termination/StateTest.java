/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;

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
        MechanismStateMachine handler = new TestMechanismStateMachine("SCRAM-SHA-256");

        State.RequiringAuthenticate next = initial.nextState(handler, 0L);

        assertThat(next.mechanismStateMachine()).isSameAs(handler);
        assertThat(next.isAuthenticated()).isFalse();
        assertThat(next.isFailed()).isFalse();
        assertThat(next.isTerminal()).isFalse();
    }

    @Test
    void shouldStayInAuthenticateForChallenge() {
        State.RequiringHandshake initial = State.start();
        MechanismStateMachine handler = new TestMechanismStateMachine("SCRAM-SHA-256");
        State.RequiringAuthenticate authenticating = initial.nextState(handler, 0L);

        State.RequiringAuthenticate nextRound = authenticating.nextStateChallenge();

        // Should return same instance (stays in same state)
        assertThat(nextRound).isSameAs(authenticating);
        assertThat(nextRound.mechanismStateMachine()).isSameAs(handler);
    }

    @Test
    void shouldTransitionToAuthenticatedOnSuccess() {
        State.RequiringHandshake initial = State.start();
        State.RequiringAuthenticate authenticating = initial.nextState(new TestMechanismStateMachine("SCRAM-SHA-256"), 0L);

        State.Authenticated authenticated = authenticating.nextStateSuccess("alice", "SCRAM-SHA-256", null);

        assertThat(authenticated.authorizationId()).isEqualTo("alice");
        assertThat(authenticated.isAuthenticated()).isTrue();
        assertThat(authenticated.isFailed()).isFalse();
        assertThat(authenticated.isTerminal()).isFalse();
    }

    @Test
    void shouldTransitionToFailedOnFailure() {
        State.RequiringHandshake initial = State.start();
        State.RequiringAuthenticate authenticating = initial.nextState(new TestMechanismStateMachine("SCRAM-SHA-256"), 0L);

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
        MechanismStateMachine handler = new TestMechanismStateMachine("SCRAM-SHA-256");

        // Round 1
        State.RequiringAuthenticate round1 = initial.nextState(handler, 0L);
        assertThat(round1.isTerminal()).isFalse();

        // Round 2 (challenge)
        State.RequiringAuthenticate round2 = round1.nextStateChallenge();
        assertThat(round2).isSameAs(round1);
        assertThat(round2.isTerminal()).isFalse();

        // Final round (success)
        State.Authenticated authenticated = round2.nextStateSuccess("alice", "SCRAM-SHA-256", null);
        assertThat(authenticated.isTerminal()).isFalse();
        assertThat(authenticated.authorizationId()).isEqualTo("alice");
    }

    @Test
    void shouldProvideReadableToString() {
        State.RequiringHandshake handshake = State.start();
        assertThat(handshake).hasToString("RequiringHandshake");

        State.RequiringAuthenticate authenticating = handshake.nextState(new TestMechanismStateMachine("SCRAM-SHA-256"), 0L);
        assertThat(authenticating.toString()).contains("RequiringAuthenticate").contains("SCRAM-SHA-256");

        State.Authenticated authenticated = authenticating.nextStateSuccess("alice", "SCRAM-SHA-256", null);
        assertThat(authenticated.toString()).contains("Authenticated").contains("alice");

        State.Failed failed = authenticating.nextStateFailure("Bad password");
        assertThat(failed.toString()).contains("Failed").contains("Bad password");
    }

    @Test
    void shouldHandleNullErrorMessageInFailed() {
        State.RequiringHandshake initial = State.start();
        State.RequiringAuthenticate authenticating = initial.nextState(new TestMechanismStateMachine("SCRAM-SHA-256"), 0L);

        State.Failed failed = authenticating.nextStateFailure(null);

        assertThat(failed.errorMessage()).isNull();
        assertThat(failed.isFailed()).isTrue();
    }

    @Test
    void shouldTransitionFromAuthenticatedToReauthenticate() {
        // Given
        State.RequiringHandshake initial = State.start();
        MechanismStateMachine handler1 = new TestMechanismStateMachine("SCRAM-SHA-256");
        State.Authenticated authenticated = initial.nextState(handler1, 0L).nextStateSuccess("alice", "SCRAM-SHA-256", null);
        MechanismStateMachine handler2 = new TestMechanismStateMachine("SCRAM-SHA-256");

        // When
        State.RequiringAuthenticate reauth = authenticated.nextStateReauthenticate(handler2, 0L);

        // Then
        assertThat(reauth.mechanismStateMachine()).isSameAs(handler2);
        assertThat(reauth.isAuthenticated()).isFalse();
        assertThat(reauth.isTerminal()).isFalse();
        assertThat(reauth.previousAuthorizationId()).isEqualTo("alice");
    }

    @Test
    void shouldHaveNullPreviousAuthorizationIdOnInitialAuth() {
        // Given
        State.RequiringHandshake initial = State.start();

        // When
        State.RequiringAuthenticate authenticating = initial.nextState(new TestMechanismStateMachine("SCRAM-SHA-256"), 0L);

        // Then
        assertThat(authenticating.previousAuthorizationId()).isNull();
    }

    @Test
    void shouldStoreSessionExpiry() {
        // Given
        State.RequiringHandshake initial = State.start();
        Instant expiry = Instant.now().plusSeconds(3600);

        // When
        State.Authenticated authenticated = initial.nextState(new TestMechanismStateMachine("SCRAM-SHA-256"), 0L)
                .nextStateSuccess("alice", "SCRAM-SHA-256", expiry);

        // Then
        assertThat(authenticated.sessionExpiry()).isEqualTo(expiry);
    }

    @Test
    void shouldStoreNullSessionExpiry() {
        // Given
        State.RequiringHandshake initial = State.start();

        // When
        State.Authenticated authenticated = initial.nextState(new TestMechanismStateMachine("SCRAM-SHA-256"), 0L)
                .nextStateSuccess("alice", "SCRAM-SHA-256", null);

        // Then
        assertThat(authenticated.sessionExpiry()).isNull();
    }

    @Test
    void shouldDistinguishTerminalStates() {
        State.RequiringHandshake handshake = State.start();
        State.RequiringAuthenticate authenticating = handshake.nextState(new TestMechanismStateMachine("SCRAM-SHA-256"), 0L);
        State.Authenticated authenticated = authenticating.nextStateSuccess("alice", "SCRAM-SHA-256", null);
        State.Failed failed = authenticating.nextStateFailure("error");

        // Non-terminal states
        assertThat(handshake.isTerminal()).isFalse();
        assertThat(authenticating.isTerminal()).isFalse();
        assertThat(authenticated.isTerminal()).isFalse();

        // Terminal states
        assertThat(failed.isTerminal()).isTrue();
    }

    // Test mechanism handler implementation
    private static class TestMechanismStateMachine implements MechanismStateMachine {
        private final String mechanismName;

        TestMechanismStateMachine(String mechanismName) {
            this.mechanismName = mechanismName;
        }

        @Override
        public String mechanismName() {
            return mechanismName;
        }

        @Override
        public int maxAuthBytes() {
            return 4 * 1024;
        }

        @Override
        public CompletionStage<RoundResult> evaluateRound(byte[] authBytes) {
            return CompletableFuture.completedFuture(
                    RoundResult.success(new byte[0], "test-user"));
        }

        @Override
        public void dispose() {
            // No-op for test
        }
    }
}
