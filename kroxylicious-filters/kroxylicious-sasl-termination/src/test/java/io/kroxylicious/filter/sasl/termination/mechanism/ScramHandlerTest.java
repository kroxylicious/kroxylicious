/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination.mechanism;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import io.kroxylicious.sasl.credentialstore.CredentialLookupException;
import io.kroxylicious.sasl.credentialstore.CredentialServiceUnavailableException;
import io.kroxylicious.sasl.credentialstore.ScramCredential;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ScramHandlerTest {

    private static final String TEST_USERNAME = "alice";
    private static final String TEST_PASSWORD = "alice-secret";

    private ScramHandler handler;
    private ScramCredentialStore credentialStore;

    @BeforeEach
    void setUp() {
        credentialStore = mock(ScramCredentialStore.class);
        handler = new ScramHandler(ScramMechanism.SCRAM_SHA_256, credentialStore, java.time.Clock.systemUTC());
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
        handler = new ScramHandler(ScramMechanism.SCRAM_SHA_512, credentialStore, java.time.Clock.systemUTC());
        assertThat(handler.mechanismName()).isEqualTo("SCRAM-SHA-512");
    }

    @Test
    void shouldFailForUnknownUser() throws Exception {
        // Given
        when(credentialStore.lookupCredential(TEST_USERNAME))
                .thenReturn(CompletableFuture.completedFuture(null));
        byte[] clientFirstMessage = "n,,n=alice,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        AuthenticationResult result = handler.handleAuthenticate(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result.outcome()).isEqualTo(AuthenticationResult.Outcome.FAILURE);
        assertThat(result.errorMessage()).isEqualTo("Authentication failed");
        assertThat(result.authorizationId()).isNull();
        verify(credentialStore).lookupCredential(TEST_USERNAME);
    }

    @Test
    void shouldFailForMalformedMessage() throws Exception {
        // Given
        byte[] invalidMessage = "not-a-valid-scram-message".getBytes(StandardCharsets.UTF_8);

        // When
        AuthenticationResult result = handler.handleAuthenticate(invalidMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result.outcome()).isEqualTo(AuthenticationResult.Outcome.FAILURE);
        assertThat(result.errorMessage()).contains("Invalid SCRAM message");
    }

    @Test
    void shouldFailForEmptyMessage() throws Exception {
        // Given
        byte[] emptyMessage = new byte[0];

        // When
        AuthenticationResult result = handler.handleAuthenticate(emptyMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result.outcome()).isEqualTo(AuthenticationResult.Outcome.FAILURE);
        assertThat(result.errorMessage()).contains("Invalid SCRAM message");
    }

    @Test
    void shouldFailForMessageWithoutUsername() throws Exception {
        // Given
        byte[] invalidMessage = "n,,r=clientnonce".getBytes(StandardCharsets.UTF_8);

        // When
        AuthenticationResult result = handler.handleAuthenticate(invalidMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result.outcome()).isEqualTo(AuthenticationResult.Outcome.FAILURE);
        assertThat(result.errorMessage()).contains("Invalid SCRAM message");
    }

    @Test
    void shouldExtractUsernameCorrectly() throws Exception {
        // Given
        when(credentialStore.lookupCredential("bob"))
                .thenReturn(CompletableFuture.completedFuture(null));
        byte[] clientFirstMessage = "n,,n=bob,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        handler.handleAuthenticate(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        verify(credentialStore).lookupCredential("bob");
    }

    @Test
    void shouldFailForCredentialLookupException() throws Exception {
        // Given
        when(credentialStore.lookupCredential(anyString()))
                .thenReturn(failedFuture(new CredentialLookupException("Database error")));
        byte[] clientFirstMessage = "n,,n=alice,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        AuthenticationResult result = handler.handleAuthenticate(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result.outcome()).isEqualTo(AuthenticationResult.Outcome.FAILURE);
        assertThat(result.errorMessage()).contains("Database error");
    }

    @Test
    void shouldFailForCredentialServiceUnavailable() throws Exception {
        // Given
        when(credentialStore.lookupCredential(anyString()))
                .thenReturn(failedFuture(new CredentialServiceUnavailableException("Service down")));
        byte[] clientFirstMessage = "n,,n=alice,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        AuthenticationResult result = handler.handleAuthenticate(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result.outcome()).isEqualTo(AuthenticationResult.Outcome.FAILURE);
        assertThat(result.errorMessage()).contains("Credential lookup failed");
    }

    @Test
    void shouldDisposeIdempotently() {
        // When/Then
        handler.dispose();
        handler.dispose();
    }

    @Test
    @Disabled("Full SCRAM server creation requires provider registration - tested in integration tests")
    void shouldCreateChallengeWithValidCredential() throws Exception {
        // Given
        ScramCredential credential = generateCredential(TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_256);
        when(credentialStore.lookupCredential(TEST_USERNAME))
                .thenReturn(CompletableFuture.completedFuture(credential));
        byte[] clientFirstMessage = "n,,n=alice,r=fyko+d2lbbFgONRv9qkxdawL".getBytes(StandardCharsets.UTF_8);

        // When
        AuthenticationResult result = handler.handleAuthenticate(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        assertThat(result.outcome()).isEqualTo(AuthenticationResult.Outcome.CHALLENGE);
        assertThat(result.responseBytes()).isNotEmpty();
        assertThat(result.authorizationId()).isNull();
        verify(credentialStore).lookupCredential(TEST_USERNAME);
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
