/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.kafka.common.message.SaslAuthenticateResponseData;
import io.kroxylicious.kafka.common.message.SaslHandshakeResponseData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

class MdsReauthenticationFailureTest extends MdsReauthenticationTestSupport {
    private void establishSession() {
        token("old", 30);
        success(30000);
        request().join();
        time(25);
    }

    @Test
    void neverFallsBackToTheOldSessionWhenMdsFails() {
        // Given
        establishSession();
        tokens.add(CompletableFuture.failedStage(new IllegalStateException("MDS unavailable")));

        // When
        var result = request().join();
        var subsequent = request().join();

        // Then
        assertThat(result.closeConnection()).isTrue();
        assertThat(result.message()).isNull();
        assertThat(subsequent.closeConnection()).isTrue();
        assertThat(subsequent.message()).isNull();
        assertThat(sent).hasSize(3);
        assertThat(users).containsExactly("alice", "alice");
    }

    @Test
    void failsClosedWhenBrokerRejectsTheRenewedToken() {
        // Given
        establishSession();
        token("fresh", 60);
        success(30000);
        var handshake = replies.remove();
        replies.clear();
        replies.add(handshake);
        replies.add(CompletableFuture.completedStage(new SaslAuthenticateResponseData().setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code())));

        // When
        var result = request().join();

        // Then
        assertThat(result.closeConnection()).isTrue();
        assertThat(result.message()).isNull();
        assertThat(sent).hasSize(5);
    }

    @Test
    @SuppressWarnings("removal")
    void cannotSwitchPrincipalWhenRenewing() {
        // Given
        establishSession();
        when(context.authenticatedSubject()).thenReturn(new Subject(new User("bob")));

        // When
        var result = request().join();

        // Then
        assertThat(result.closeConnection()).isTrue();
        assertThat(users).containsExactly("alice");
        assertThat(sent).hasSize(3);
    }

    @Test
    void lateTokenDoesNotAuthenticateAfterAnotherRequestClosesTheConnection() {
        // Given
        establishSession();
        var token = new CompletableFuture<MdsToken>();
        tokens.add(token);
        var pending = request();
        filter.onRequest(ApiKeys.SASL_HANDSHAKE, (short) 1, header, request, context).toCompletableFuture().join();

        // When
        token.complete(new MdsToken("fresh", START.plusSeconds(60)));

        // Then
        assertThat(pending.join().closeConnection()).isTrue();
        assertThat(pending.join().message()).isNull();
        assertThat(sent).hasSize(3);
    }

    @ParameterizedTest
    @ValueSource(longs = { -1, 4000, 5500 })
    void rejectsInvalidOrUnusablyShortBrokerSessions(long lifetime) {
        // Given
        establishSession();
        token("fresh", 60);
        success(lifetime);

        // When
        var result = request().join();

        // Then
        assertThat(result.closeConnection()).isTrue();
        assertThat(result.message()).isNull();
        assertThat(sent).hasSize(5);
    }

    @Test
    void discardsATokenThatExpiresWhileTheSaslReplyIsDelayed() {
        // Given
        establishSession();
        token("fresh", 60);
        success(30000);
        var handshake = replies.remove();
        replies.clear();
        replies.add(handshake);
        var auth = new CompletableFuture<SaslAuthenticateResponseData>();
        replies.add(auth);
        var pending = request();
        time(60);

        // When
        auth.complete(new SaslAuthenticateResponseData().setSessionLifetimeMs(30000));

        // Then
        assertThat(pending.join().closeConnection()).isTrue();
        assertThat(pending.join().message()).isNull();
    }

    @Test
    void closesWhenTheRenewalHandshakeTimesOut() {
        // Given
        establishSession();
        token("fresh", 60);
        var handshake = new CompletableFuture<SaslHandshakeResponseData>();
        replies.add(handshake);
        var pending = request();

        // When
        handshake.completeExceptionally(new java.util.concurrent.TimeoutException());

        // Then
        assertThat(pending.join().closeConnection()).isTrue();
        assertThat(pending.join().message()).isNull();
        assertThat(sent).hasSize(4);
    }
}
