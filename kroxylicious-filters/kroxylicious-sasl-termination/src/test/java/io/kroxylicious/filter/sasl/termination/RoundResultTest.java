/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RoundResultTest {

    // --- Challenge ---

    @Test
    void shouldCreateChallenge() {
        // Given
        byte[] bytes = { 1, 2, 3 };

        // When
        var challenge = new RoundResult.Challenge(bytes);

        // Then
        assertThat(challenge.responseBytes()).containsExactly(1, 2, 3);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullResponseBytesForChallenge() {
        assertThatThrownBy(() -> new RoundResult.Challenge(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldDefensivelyCopyInputForChallenge() {
        // Given
        byte[] input = { 1, 2, 3 };

        // When
        var challenge = new RoundResult.Challenge(input);
        input[0] = 99;

        // Then
        assertThat(challenge.responseBytes()).containsExactly(1, 2, 3);
    }

    @Test
    void shouldDefensivelyCopyOutputForChallenge() {
        // Given
        var challenge = new RoundResult.Challenge(new byte[]{ 1, 2, 3 });

        // When
        byte[] out = challenge.responseBytes();
        out[0] = 99;

        // Then
        assertThat(challenge.responseBytes()).containsExactly(1, 2, 3);
    }

    @Test
    void shouldBeEqualForSameContentChallenge() {
        // Given
        var a = new RoundResult.Challenge(new byte[]{ 1, 2, 3 });
        var b = new RoundResult.Challenge(new byte[]{ 1, 2, 3 });

        // Then
        assertThat(a).isEqualTo(b)
                .hasSameHashCodeAs(b);
    }

    @SuppressWarnings("SelfAssertion")
    @Test
    void shouldBeReflexivelyEqualChallenge() {
        // Given
        var c = new RoundResult.Challenge(new byte[]{ 1, 2, 3 });

        // Then
        assertThat(c).isEqualTo(c);
    }

    @Test
    void shouldNotBeEqualForDifferentContentChallenge() {
        // Given
        var a = new RoundResult.Challenge(new byte[]{ 1, 2, 3 });
        var b = new RoundResult.Challenge(new byte[]{ 4, 5, 6 });

        // Then
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void shouldNotBeEqualToNullChallenge() {
        // Given
        var c = new RoundResult.Challenge(new byte[]{ 1, 2, 3 });

        // Then
        assertThat(c).isNotEqualTo(null);
    }

    @Test
    void shouldNotBeEqualToDifferentTypeChallenge() {
        // Given
        var c = new RoundResult.Challenge(new byte[]{ 1, 2, 3 });

        // Then
        assertThat(c).isNotEqualTo("a string");
    }

    @Test
    void shouldReturnOpaqueToStringForChallenge() {
        // Given
        var c = new RoundResult.Challenge(new byte[]{ 1, 2, 3 });

        // Then
        assertThat(c).hasToString("Challenge{}");
    }

    // --- Success ---

    @Test
    void shouldCreateSuccess() {
        // Given
        byte[] bytes = { 10, 20 };

        // When
        var success = new RoundResult.Success(bytes, "alice", Instant.ofEpochMilli(3600));

        // Then
        assertThat(success.responseBytes()).containsExactly(10, 20);
        assertThat(success.authorizationId()).isEqualTo("alice");
        assertThat(success.sessionExpiry()).isEqualTo(Instant.ofEpochMilli(3600));
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullResponseBytesForSuccess() {
        assertThatThrownBy(() -> new RoundResult.Success(null, "alice", null))
                .isInstanceOf(NullPointerException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullAuthorizationIdForSuccess() {
        assertThatThrownBy(() -> new RoundResult.Success(new byte[0], null, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldAcceptNullSessionExpiry() {
        // When
        var success = new RoundResult.Success(new byte[0], "alice", null);

        // Then
        assertThat(success.sessionExpiry()).isNull();
    }

    @Test
    void shouldDefensivelyCopyInputForSuccess() {
        // Given
        byte[] input = { 1, 2, 3 };

        // When
        var success = new RoundResult.Success(input, "alice", null);
        input[0] = 99;

        // Then
        assertThat(success.responseBytes()).containsExactly(1, 2, 3);
    }

    @Test
    void shouldDefensivelyCopyOutputForSuccess() {
        // Given
        var success = new RoundResult.Success(new byte[]{ 1, 2, 3 }, "alice", null);

        // When
        byte[] out = success.responseBytes();
        out[0] = 99;

        // Then
        assertThat(success.responseBytes()).containsExactly(1, 2, 3);
    }

    @Test
    void shouldBeEqualForSameContentSuccess() {
        // Given
        var a = new RoundResult.Success(new byte[]{ 1, 2 }, "alice", Instant.ofEpochMilli(3600));
        var b = new RoundResult.Success(new byte[]{ 1, 2 }, "alice", Instant.ofEpochMilli(3600));

        // Then
        assertThat(a).isEqualTo(b)
                .hasSameHashCodeAs(b);
    }

    @Test
    void shouldNotBeEqualForDifferentResponseBytesSuccess() {
        // Given
        var a = new RoundResult.Success(new byte[]{ 1, 2 }, "alice", Instant.ofEpochMilli(3600));
        var b = new RoundResult.Success(new byte[]{ 3, 4 }, "alice", Instant.ofEpochMilli(3600));

        // Then
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void shouldNotBeEqualForDifferentAuthorizationIdSuccess() {
        // Given
        var a = new RoundResult.Success(new byte[]{ 1, 2 }, "alice", Instant.ofEpochMilli(3600));
        var b = new RoundResult.Success(new byte[]{ 1, 2 }, "bob", Instant.ofEpochMilli(3600));

        // Then
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void shouldNotBeEqualForDifferentSessionExpirySuccess() {
        // Given
        var a = new RoundResult.Success(new byte[]{ 1, 2 }, "alice", Instant.ofEpochMilli(3600));
        var b = new RoundResult.Success(new byte[]{ 1, 2 }, "alice", Instant.ofEpochMilli(7200));

        // Then
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void shouldNotBeEqualToNullSuccess() {
        // Given
        var s = new RoundResult.Success(new byte[]{ 1, 2 }, "alice", null);

        // Then
        assertThat(s).isNotEqualTo(null);
    }

    @Test
    void shouldNotBeEqualToDifferentTypeSuccess() {
        // Given
        var s = new RoundResult.Success(new byte[]{ 1, 2 }, "alice", null);
        var c = new RoundResult.Challenge(new byte[]{ 1, 2 });

        // Then
        assertThat(s).isNotEqualTo(c);
    }

    @Test
    void shouldReturnDescriptiveToStringForSuccess() {
        // Given
        var s = new RoundResult.Success(new byte[]{ 1, 2 }, "alice", Instant.ofEpochMilli(3600));

        // Then
        assertThat(s).hasToString("Success{authorizationId='alice', sessionExpiry=" + Instant.ofEpochMilli(3600) + "}");
    }

    // --- Failure ---

    @Test
    void shouldCreateFailure() {
        // Given
        byte[] bytes = { 7, 8 };
        var exception = new Exception("auth failed");

        // When
        var failure = new RoundResult.Failure(bytes, exception);

        // Then
        assertThat(failure.responseBytes()).containsExactly(7, 8);
        assertThat(failure.exception()).isSameAs(exception);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullResponseBytesForFailure() {
        // Given
        var exception = new Exception("x");

        // When/Then
        assertThatThrownBy(() -> new RoundResult.Failure(null, exception))
                .isInstanceOf(NullPointerException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullExceptionForFailure() {
        assertThatThrownBy(() -> new RoundResult.Failure(new byte[0], null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldDefensivelyCopyInputForFailure() {
        // Given
        byte[] input = { 1, 2, 3 };
        var exception = new Exception("x");

        // When
        var failure = new RoundResult.Failure(input, exception);
        input[0] = 99;

        // Then
        assertThat(failure.responseBytes()).containsExactly(1, 2, 3);
    }

    @Test
    void shouldDefensivelyCopyOutputForFailure() {
        // Given
        var failure = new RoundResult.Failure(new byte[]{ 1, 2, 3 }, new Exception("x"));

        // When
        byte[] out = failure.responseBytes();
        out[0] = 99;

        // Then
        assertThat(failure.responseBytes()).containsExactly(1, 2, 3);
    }

    @Test
    void shouldBeEqualForSameContentFailure() {
        // Given
        var exception = new Exception("auth failed");
        var a = new RoundResult.Failure(new byte[]{ 1, 2 }, exception);
        var b = new RoundResult.Failure(new byte[]{ 1, 2 }, exception);

        // Then
        assertThat(a).isEqualTo(b)
                .hasSameHashCodeAs(b);
    }

    @Test
    void shouldNotBeEqualForDifferentExceptionFailure() {
        // Given
        var a = new RoundResult.Failure(new byte[]{ 1, 2 }, new Exception("auth failed"));
        var b = new RoundResult.Failure(new byte[]{ 1, 2 }, new Exception("auth failed"));

        // Then
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void shouldNotBeEqualForDifferentResponseBytesFailure() {
        // Given
        var exception = new Exception("auth failed");
        var a = new RoundResult.Failure(new byte[]{ 1, 2 }, exception);
        var b = new RoundResult.Failure(new byte[]{ 3, 4 }, exception);

        // Then
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void shouldNotBeEqualToNullFailure() {
        // Given
        var f = new RoundResult.Failure(new byte[]{ 1, 2 }, new Exception("x"));

        // Then
        assertThat(f).isNotEqualTo(null);
    }

    @Test
    void shouldReturnExceptionMessageInToStringForFailure() {
        // Given
        var f = new RoundResult.Failure(new byte[0], new Exception("Something went wrong"));

        // Then
        assertThat(f).hasToString("Failure{exception=Something went wrong}");
    }

    // --- Factory methods ---

    @Test
    void shouldCreateChallengeViaFactoryMethod() {
        // When
        RoundResult result = RoundResult.challenge(new byte[]{ 1, 2, 3 });

        // Then
        assertThat(result).isInstanceOf(RoundResult.Challenge.class);
        assertThat(result.responseBytes()).containsExactly(1, 2, 3);
    }

    @Test
    void shouldCreateSuccessViaTwoArgFactoryMethod() {
        // When
        RoundResult result = RoundResult.success(new byte[]{ 10, 20 }, "alice");

        // Then
        assertThat(result).isInstanceOf(RoundResult.Success.class);
        var success = (RoundResult.Success) result;
        assertThat(success.responseBytes()).containsExactly(10, 20);
        assertThat(success.authorizationId()).isEqualTo("alice");
        assertThat(success.sessionExpiry()).isNull();
    }

    @Test
    void shouldCreateSuccessViaThreeArgFactoryMethod() {
        // When
        RoundResult result = RoundResult.success(new byte[]{ 10, 20 }, "alice", Instant.ofEpochMilli(5000));

        // Then
        assertThat(result).isInstanceOf(RoundResult.Success.class);
        var success = (RoundResult.Success) result;
        assertThat(success.responseBytes()).containsExactly(10, 20);
        assertThat(success.authorizationId()).isEqualTo("alice");
        assertThat(success.sessionExpiry()).isEqualTo(Instant.ofEpochMilli(5000));
    }

    @Test
    void shouldCreateFailureViaFactoryMethod() {
        // Given
        var exception = new Exception("failed");

        // When
        RoundResult result = RoundResult.failure(new byte[]{ 7, 8 }, exception);

        // Then
        assertThat(result).isInstanceOf(RoundResult.Failure.class);
        var failure = (RoundResult.Failure) result;
        assertThat(failure.responseBytes()).containsExactly(7, 8);
        assertThat(failure.exception()).isSameAs(exception);
    }
}
