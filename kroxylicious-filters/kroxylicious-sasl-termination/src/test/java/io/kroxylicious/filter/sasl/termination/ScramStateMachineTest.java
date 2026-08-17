/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.security.sasl.SaslException;

import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.scram.internals.ScramSaslServerProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.scram.credentialstore.CredentialLookupException;
import io.kroxylicious.scram.credentialstore.CredentialServiceUnavailableException;
import io.kroxylicious.scram.credentialstore.ScramCredential;
import io.kroxylicious.scram.credentialstore.ScramCredentialStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ScramStateMachineTest {

    private static final String TEST_USERNAME = "alice";
    private static final String TEST_PASSWORD = "alice-secret";

    private ScramStateMachine handler;
    private ScramCredentialStore credentialStore;

    @BeforeAll
    static void registerProviders() {
        ScramSaslServerProvider.initialize();
    }

    @BeforeEach
    void setUp() {
        credentialStore = mock(ScramCredentialStore.class);
        when(credentialStore.phantomSaltKey()).thenReturn(new byte[32]);
        handler = new ScramStateMachine(ScramMechanism.SCRAM_SHA_256, credentialStore, ScramMechanismConfig.DEFAULT_PHANTOM_ITERATIONS);
    }

    @AfterEach
    void tearDown() {
        if (handler != null) {
            handler.dispose();
        }
    }

    @Test
    void shouldReturnCorrectMechanismNameForSha256() {
        assertThat(handler.mechanismName()).isEqualTo("SCRAM-SHA-256");
    }

    @Test
    void shouldReturnCorrectMechanismNameForSha512() {
        handler = new ScramStateMachine(ScramMechanism.SCRAM_SHA_512, credentialStore, ScramMechanismConfig.DEFAULT_PHANTOM_ITERATIONS);
        assertThat(handler.mechanismName()).isEqualTo("SCRAM-SHA-512");
    }

    @Test
    void shouldReturnChallengeForUnknownUserOnFirstRound() throws Exception {
        // Given
        when(credentialStore.lookupCredential(TEST_USERNAME))
                .thenReturn(CompletableFuture.completedFuture(null));
        byte[] clientFirstMessage = "n,,n=alice,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        RoundResult result = handler.evaluateRound(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Challenge.class);
        String serverFirstMessage = new String(result.responseBytes(), StandardCharsets.UTF_8);
        assertThat(serverFirstMessage)
                .startsWith("r=fyko+d2lbbFgONRv9qkxdawL")
                .contains(",s=")
                .contains(",i=");
        verify(credentialStore).lookupCredential(TEST_USERNAME);
    }

    @Test
    void shouldFailForUnknownUserOnSecondRound() throws Exception {
        // Given
        when(credentialStore.lookupCredential(TEST_USERNAME))
                .thenReturn(CompletableFuture.completedFuture(null));
        byte[] clientFirstMessage = "n,,n=alice,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);
        handler.evaluateRound(clientFirstMessage).toCompletableFuture().get();

        // When
        byte[] clientFinalMessage = "c=biws,r=fyko+d2lbbFgONRv9qkxdawLsome-server-nonce,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=".getBytes(StandardCharsets.UTF_8);
        RoundResult result = handler.evaluateRound(clientFinalMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Failure.class);
        assertThat(((RoundResult.Failure) result).exception().getMessage()).isEqualTo("Authentication failed");
    }

    @Test
    void shouldFailForMalformedMessage() throws Exception {
        // Given
        byte[] invalidMessage = "not-a-valid-scram-message".getBytes(StandardCharsets.UTF_8);

        // When
        RoundResult result = handler.evaluateRound(invalidMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Failure.class);
        assertThat(((RoundResult.Failure) result).exception()).isInstanceOf(SaslException.class);
    }

    @Test
    void shouldFailForEmptyMessage() throws Exception {
        // Given
        byte[] emptyMessage = new byte[0];

        // When
        RoundResult result = handler.evaluateRound(emptyMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Failure.class);
        assertThat(((RoundResult.Failure) result).exception()).isInstanceOf(SaslException.class);
    }

    @Test
    void shouldFailForMessageWithoutUsername() throws Exception {
        // Given
        byte[] invalidMessage = "n,,r=clientnonce".getBytes(StandardCharsets.UTF_8);

        // When
        RoundResult result = handler.evaluateRound(invalidMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Failure.class);
        assertThat(((RoundResult.Failure) result).exception()).isInstanceOf(SaslException.class);
    }

    @Test
    void shouldAcceptMandatoryExtensionConsistentWithKafka() throws Exception {
        // Given
        when(credentialStore.lookupCredential(TEST_USERNAME))
                .thenReturn(CompletableFuture.completedFuture(null));
        byte[] clientFirstMessage = "n,,m=someext,n=alice,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        RoundResult result = handler.evaluateRound(clientFirstMessage)
                .toCompletableFuture().get();

        // Then — Kafka's ScramSaslServer accepts m= extensions, so we must too for consistency
        assertThat(result).isInstanceOf(RoundResult.Challenge.class);
    }

    @Test
    void shouldExtractUsernameCorrectly() throws Exception {
        // Given
        when(credentialStore.lookupCredential("bob"))
                .thenReturn(CompletableFuture.completedFuture(null));
        byte[] clientFirstMessage = "n,,n=bob,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        handler.evaluateRound(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        verify(credentialStore).lookupCredential("bob");
    }

    @Test
    void shouldFailForOversizedUsername() throws Exception {
        // Given
        String longUsername = "a".repeat(ScramStateMachine.MAX_USERNAME_LENGTH + 1);
        byte[] clientFirstMessage = ("n,,n=" + longUsername + ",r=fyko+d2lbbFgONRv9qkxdawL").getBytes(StandardCharsets.UTF_8);

        // When
        RoundResult result = handler.evaluateRound(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Failure.class);
        assertThat(((RoundResult.Failure) result).exception().getMessage()).contains("username exceeds maximum length");
    }

    @Test
    void shouldAcceptUsernameAtMaxLength() throws Exception {
        // Given
        String maxUsername = "a".repeat(ScramStateMachine.MAX_USERNAME_LENGTH);
        when(credentialStore.lookupCredential(maxUsername))
                .thenReturn(CompletableFuture.completedFuture(null));
        byte[] clientFirstMessage = ("n,,n=" + maxUsername + ",r=fyko+d2lbbFgONRv9qkxdawL").getBytes(StandardCharsets.UTF_8);

        // When
        handler.evaluateRound(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        verify(credentialStore).lookupCredential(maxUsername);
    }

    @Test
    void shouldFailForCredentialLookupException() throws Exception {
        // Given
        when(credentialStore.lookupCredential(anyString()))
                .thenReturn(failedFuture(new CredentialLookupException("Database error")));
        byte[] clientFirstMessage = "n,,n=alice,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        RoundResult result = handler.evaluateRound(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Failure.class);
        assertThat(((RoundResult.Failure) result).exception()).isInstanceOf(CredentialLookupException.class);
        assertThat(((RoundResult.Failure) result).exception().getMessage()).contains("Database error");
    }

    @Test
    void shouldFailForCredentialServiceUnavailable() throws Exception {
        // Given
        when(credentialStore.lookupCredential(anyString()))
                .thenReturn(failedFuture(new CredentialServiceUnavailableException("Service down")));
        byte[] clientFirstMessage = "n,,n=alice,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        RoundResult result = handler.evaluateRound(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Failure.class);
        assertThat(((RoundResult.Failure) result).exception()).isInstanceOf(CredentialServiceUnavailableException.class);
        assertThat(((RoundResult.Failure) result).exception().getMessage()).contains("Service down");
    }

    @Test
    void shouldDisposeIdempotently() {
        // When/Then
        handler.dispose();
        assertThatNoException().isThrownBy(() -> handler.dispose());
    }

    @Test
    void shouldCreateChallengeWithValidCredential() throws Exception {
        // Given
        ScramCredential credential = generateCredential(TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_256);
        when(credentialStore.lookupCredential(TEST_USERNAME))
                .thenReturn(CompletableFuture.completedFuture(credential));
        byte[] clientFirstMessage = "n,,n=alice,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        RoundResult result = handler.evaluateRound(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Challenge.class);
        assertThat(result.responseBytes()).isNotEmpty();
        verify(credentialStore).lookupCredential(TEST_USERNAME);
    }

    @Test
    void shouldGeneratePhantomChallengeForSha512() throws Exception {
        // Given
        handler = new ScramStateMachine(ScramMechanism.SCRAM_SHA_512, credentialStore, ScramMechanismConfig.DEFAULT_PHANTOM_ITERATIONS);
        when(credentialStore.lookupCredential("alice"))
                .thenReturn(CompletableFuture.completedFuture(null));
        byte[] clientFirstMessage = "n,,n=alice,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        RoundResult result = handler.evaluateRound(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Challenge.class);
        String serverFirstMessage = new String(result.responseBytes(), StandardCharsets.UTF_8);
        assertThat(serverFirstMessage).startsWith("r=fyko+d2lbbFgONRv9qkxdawL");
    }

    @Test
    void shouldDecodeEncodedUsername() throws Exception {
        // Given
        when(credentialStore.lookupCredential("alice,bob"))
                .thenReturn(CompletableFuture.completedFuture(null));
        byte[] clientFirstMessage = "n,,n=alice=2Cbob,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        handler.evaluateRound(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        verify(credentialStore).lookupCredential("alice,bob");
    }

    @Test
    void shouldDecodeEqualsInUsername() throws Exception {
        // Given
        when(credentialStore.lookupCredential("alice=bob"))
                .thenReturn(CompletableFuture.completedFuture(null));
        byte[] clientFirstMessage = "n,,n=alice=3Dbob,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        handler.evaluateRound(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        verify(credentialStore).lookupCredential("alice=bob");
    }

    @Test
    void shouldRejectAuthzidDifferentFromUsername() throws Exception {
        // Given
        byte[] clientFirstMessage = "n,a=eve,n=alice,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        RoundResult result = handler.evaluateRound(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Failure.class);
        assertThat(((RoundResult.Failure) result).exception()).isInstanceOf(SaslException.class);
        assertThat(((RoundResult.Failure) result).exception().getMessage()).contains("authorization id");
    }

    @Test
    void shouldAcceptAuthzidMatchingUsername() throws Exception {
        // Given
        when(credentialStore.lookupCredential(TEST_USERNAME))
                .thenReturn(CompletableFuture.completedFuture(null));
        byte[] clientFirstMessage = "n,a=alice,n=alice,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        RoundResult result = handler.evaluateRound(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Challenge.class);
        verify(credentialStore).lookupCredential(TEST_USERNAME);
    }

    @Test
    void shouldRejectNonceWithControlCharacters() throws Exception {
        // Given
        byte[] clientFirstMessage = ("n,,n=alice,r=nonce" + "\u0001" + "bad").getBytes(StandardCharsets.UTF_8);

        // When
        RoundResult result = handler.evaluateRound(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Failure.class);
        assertThat(((RoundResult.Failure) result).exception()).isInstanceOf(SaslException.class);
    }

    @Test
    void shouldRejectInvalidSaslNameEncoding() throws Exception {
        // Given
        byte[] clientFirstMessage = "n,,n=alice=2Dbob,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        RoundResult result = handler.evaluateRound(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Failure.class);
    }

    @Test
    void shouldFailOnInvalidSecondRoundWithValidCredential() throws Exception {
        // Given
        ScramCredential credential = generateCredential(TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_256);
        when(credentialStore.lookupCredential(TEST_USERNAME))
                .thenReturn(CompletableFuture.completedFuture(credential));
        byte[] clientFirstMessage = "n,,n=alice,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);
        handler.evaluateRound(clientFirstMessage).toCompletableFuture().get();

        // When
        byte[] invalidClientFinal = "c=biws,r=invalid,p=invalid".getBytes(StandardCharsets.UTF_8);
        RoundResult result = handler.evaluateRound(invalidClientFinal)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Failure.class);
        assertThat(((RoundResult.Failure) result).exception()).isInstanceOf(SaslException.class);
    }

    private ScramCredential generateCredential(
                                               String username,
                                               String password,
                                               ScramMechanism mechanism) {
        return TestCredentialHelper.generateCredential(
                username,
                password,
                mechanism);
    }

    private <T> CompletionStage<T> failedFuture(Throwable throwable) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(throwable);
        return future;
    }
}
