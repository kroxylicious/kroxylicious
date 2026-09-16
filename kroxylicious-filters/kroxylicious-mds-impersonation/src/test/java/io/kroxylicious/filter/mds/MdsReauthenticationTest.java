/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateResponseData;
import io.kroxylicious.kafka.common.message.SaslHandshakeResponseData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

import static org.assertj.core.api.Assertions.assertThat;

class MdsReauthenticationTest extends MdsReauthenticationTestSupport {
    @ParameterizedTest
    @ValueSource(longs = { 25, 90 })
    void renewsBeforeForwardingIncludingAfterAnIdleSessionExpires(long requestTime) {
        // Given
        token("old", 30);
        success(60000);
        request().join();
        token("fresh", requestTime + 30);
        success(60000);
        time(requestTime);

        // When
        var result = request().join();

        // Then
        assertThat(result.closeConnection()).isFalse();
        assertThat(result.message()).isSameAs(request);
        assertThat(users).containsExactly("alice", "alice");
        assertThat(sent).extracting(ApiMessage::apiKey).containsExactly(ApiKeys.API_VERSIONS.id, ApiKeys.SASL_HANDSHAKE.id, ApiKeys.SASL_AUTHENTICATE.id,
                ApiKeys.SASL_HANDSHAKE.id, ApiKeys.SASL_AUTHENTICATE.id);
        assertThat(new String(((SaslAuthenticateRequestData) sent.getLast()).authBytes(), StandardCharsets.UTF_8))
                .isEqualTo("n,,\u0001auth=Bearer fresh\u0001\u0001");
    }

    @Test
    void sharesOnePendingRenewalAndHoldsTrafficThroughBothSaslRounds() {
        // Given
        token("old", 30);
        success(30000);
        request().join();
        var token = new CompletableFuture<MdsToken>();
        var handshake = new CompletableFuture<SaslHandshakeResponseData>();
        var authenticate = new CompletableFuture<SaslAuthenticateResponseData>();
        tokens.add(token);
        replies.add(handshake);
        replies.add(authenticate);
        time(25);
        var first = request();
        var second = request();
        boolean bothHeldForToken = !first.isDone() && !second.isDone() && sent.size() == 3;
        token.complete(new MdsToken("fresh", START.plusSeconds(60)));
        boolean bothHeldForHandshake = !first.isDone() && !second.isDone() && sent.size() == 4;
        handshake.complete(new SaslHandshakeResponseData().setMechanisms(List.of("OAUTHBEARER")));
        boolean bothHeldForAuthentication = !first.isDone() && !second.isDone() && sent.size() == 5;

        // When
        authenticate.complete(new SaslAuthenticateResponseData().setSessionLifetimeMs(30000));

        // Then
        assertThat(bothHeldForToken).isTrue();
        assertThat(bothHeldForHandshake).isTrue();
        assertThat(bothHeldForAuthentication).isTrue();
        assertThat(first.join().closeConnection()).isFalse();
        assertThat(second.join().closeConnection()).isFalse();
        assertThat(users).containsExactly("alice", "alice");
    }

    @Test
    void recalculatesDeadlineForEveryBrokerSession() {
        // Given
        token("first", 300);
        success(30000);
        request().join();
        token("second", 300);
        success(15000);
        time(25);
        request().join();
        time(34);
        request().join();
        token("third", 300);
        success(60000);
        time(35);

        // When
        var result = request().join();

        // Then
        assertThat(result.closeConnection()).isFalse();
        assertThat(users).containsExactly("alice", "alice", "alice");
        assertThat(sent).hasSize(7);
    }

    @Test
    void leavesNonReauthenticatingBrokersOnTheExistingExpiryPolicy() {
        // Given
        token("first", 30);
        success(0);
        request().join();
        time(25);

        // When
        var result = request().join();

        // Then
        assertThat(result.closeConnection()).isTrue();
        assertThat(result.message()).isNull();
        assertThat(users).containsExactly("alice");
        assertThat(sent).hasSize(3);
    }
}
