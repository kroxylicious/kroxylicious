/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.scram.internals.ScramSaslClientProvider;
import org.apache.kafka.common.security.scram.internals.ScramSaslServerProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.scram.credentialstore.CredentialLookupException;
import io.kroxylicious.scram.credentialstore.CredentialServiceUnavailableException;
import io.kroxylicious.scram.credentialstore.ScramCredential;
import io.kroxylicious.scram.credentialstore.ScramCredentialStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ScramStateMachineTest {

    private static final String TEST_USERNAME = "alice";
    private static final String TEST_PASSWORD = "alice-secret";

    private ScramStateMachine handler;
    private ScramCredentialStore credentialStore;
    private FilterDispatchExecutor filterDispatchExecutor;

    @BeforeAll
    static void registerProviders() {
        ScramSaslServerProvider.initialize();
        ScramSaslClientProvider.initialize();
    }

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        credentialStore = mock(ScramCredentialStore.class);
        when(credentialStore.phantomSaltKey()).thenReturn(new byte[32]);
        filterDispatchExecutor = mock(FilterDispatchExecutor.class);
        when(filterDispatchExecutor.completeOnFilterDispatchThread(any(CompletionStage.class))).thenAnswer(inv -> inv.getArgument(0));
        handler = new ScramStateMachine(ScramMechanism.SCRAM_SHA_256, credentialStore, ScramMechanismConfig.DEFAULT_PHANTOM_ITERATIONS, filterDispatchExecutor);
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
        handler = new ScramStateMachine(ScramMechanism.SCRAM_SHA_512, credentialStore, ScramMechanismConfig.DEFAULT_PHANTOM_ITERATIONS, filterDispatchExecutor);
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedMessages")
    void shouldFailForMalformedMessage(String description, byte[] message) throws Exception {
        // When
        RoundResult result = handler.evaluateRound(message)
                .toCompletableFuture().get();

        // Then
        assertThat(result).isInstanceOf(RoundResult.Failure.class);
        assertThat(((RoundResult.Failure) result).exception()).isInstanceOf(SaslException.class);
    }

    static Stream<Arguments> malformedMessages() {
        return Stream.of(
                Arguments.of("not a valid SCRAM message", "not-a-valid-scram-message".getBytes(StandardCharsets.UTF_8)),
                Arguments.of("empty message", new byte[0]),
                Arguments.of("missing username", "n,,r=clientnonce".getBytes(StandardCharsets.UTF_8)));
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

    @ParameterizedTest(name = "saslName={0} expectedUsername={1}")
    @MethodSource("usernameDecodingCases")
    void shouldExtractAndDecodeUsername(String saslName, String expectedUsername) throws Exception {
        // Given
        when(credentialStore.lookupCredential(expectedUsername))
                .thenReturn(CompletableFuture.completedFuture(null));
        byte[] clientFirstMessage = ("n,,n=" + saslName + ",r=fyko+d2lbbFgONRv9qkxdawL").getBytes(StandardCharsets.UTF_8);

        // When
        handler.evaluateRound(clientFirstMessage)
                .toCompletableFuture().get();

        // Then
        verify(credentialStore).lookupCredential(expectedUsername);
    }

    static Stream<Arguments> usernameDecodingCases() {
        return Stream.of(
                Arguments.of("bob", "bob"),
                Arguments.of("alice=2Cbob", "alice,bob"),
                Arguments.of("alice=3Dbob", "alice=bob"));
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
    void shouldCompleteFullAuthenticationWithValidCredentials() throws Exception {
        // Given
        ScramCredential credential = generateCredential(TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_256);
        when(credentialStore.lookupCredential(TEST_USERNAME))
                .thenReturn(CompletableFuture.completedFuture(credential));

        SaslClient client = Sasl.createSaslClient(
                new String[]{ "SCRAM-SHA-256" },
                null,
                "kafka",
                null,
                Map.of(),
                callbacks -> {
                    for (Callback cb : callbacks) {
                        if (cb instanceof NameCallback nc) {
                            nc.setName(TEST_USERNAME);
                        }
                        else if (cb instanceof PasswordCallback pc) {
                            pc.setPassword(TEST_PASSWORD.toCharArray());
                        }
                    }
                });

        // When — first round
        byte[] clientFirst = client.evaluateChallenge(new byte[0]);
        RoundResult firstResult = handler.evaluateRound(clientFirst)
                .toCompletableFuture().get();

        // Then
        assertThat(firstResult).isInstanceOf(RoundResult.Challenge.class);

        // When — second round
        byte[] clientFinal = client.evaluateChallenge(firstResult.responseBytes());
        RoundResult secondResult = handler.evaluateRound(clientFinal)
                .toCompletableFuture().get();

        // Then
        assertThat(secondResult).isInstanceOf(RoundResult.Success.class);
        assertThat(((RoundResult.Success) secondResult).authorizationId()).isEqualTo(TEST_USERNAME);

        client.dispose();
    }

    @Test
    void shouldGeneratePhantomChallengeForSha512() throws Exception {
        // Given
        handler = new ScramStateMachine(ScramMechanism.SCRAM_SHA_512, credentialStore, ScramMechanismConfig.DEFAULT_PHANTOM_ITERATIONS, filterDispatchExecutor);
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
    void shouldReturnMaxAuthBytesOf4096() {
        assertThat(handler.maxAuthBytes()).isEqualTo(4 * 1024);
    }

    @Test
    void shouldCompleteFullAuthenticationWithSha512() throws Exception {
        // Given
        handler = new ScramStateMachine(ScramMechanism.SCRAM_SHA_512, credentialStore, ScramMechanismConfig.DEFAULT_PHANTOM_ITERATIONS, filterDispatchExecutor);
        ScramCredential credential = generateCredential(TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_512);
        when(credentialStore.lookupCredential(TEST_USERNAME))
                .thenReturn(CompletableFuture.completedFuture(credential));

        SaslClient client = Sasl.createSaslClient(
                new String[]{ "SCRAM-SHA-512" },
                null,
                "kafka",
                null,
                Map.of(),
                callbacks -> {
                    for (Callback cb : callbacks) {
                        if (cb instanceof NameCallback nc) {
                            nc.setName(TEST_USERNAME);
                        }
                        else if (cb instanceof PasswordCallback pc) {
                            pc.setPassword(TEST_PASSWORD.toCharArray());
                        }
                    }
                });

        // When — first round
        byte[] clientFirst = client.evaluateChallenge(new byte[0]);
        RoundResult firstResult = handler.evaluateRound(clientFirst)
                .toCompletableFuture().get();

        // Then
        assertThat(firstResult).isInstanceOf(RoundResult.Challenge.class);

        // When — second round
        byte[] clientFinal = client.evaluateChallenge(firstResult.responseBytes());
        RoundResult secondResult = handler.evaluateRound(clientFinal)
                .toCompletableFuture().get();

        // Then
        assertThat(secondResult).isInstanceOf(RoundResult.Success.class);
        assertThat(((RoundResult.Success) secondResult).authorizationId()).isEqualTo(TEST_USERNAME);

        client.dispose();
    }

    @Test
    void shouldFailWhenCredentialAlgorithmMismatchesMechanism() throws Exception {
        // Given — SHA-256 state machine with a credential generated for SHA-512
        ScramCredential sha512Credential = generateCredential(TEST_USERNAME, TEST_PASSWORD, ScramMechanism.SCRAM_SHA_512);
        when(credentialStore.lookupCredential(TEST_USERNAME))
                .thenReturn(CompletableFuture.completedFuture(sha512Credential));

        SaslClient client = Sasl.createSaslClient(
                new String[]{ "SCRAM-SHA-256" },
                null,
                "kafka",
                null,
                Map.of(),
                callbacks -> {
                    for (Callback cb : callbacks) {
                        if (cb instanceof NameCallback nc) {
                            nc.setName(TEST_USERNAME);
                        }
                        else if (cb instanceof PasswordCallback pc) {
                            pc.setPassword(TEST_PASSWORD.toCharArray());
                        }
                    }
                });

        // When — first round succeeds (mismatch not yet detectable)
        byte[] clientFirst = client.evaluateChallenge(new byte[0]);
        RoundResult firstResult = handler.evaluateRound(clientFirst)
                .toCompletableFuture().get();
        assertThat(firstResult).isInstanceOf(RoundResult.Challenge.class);

        // When — second round fails due to cryptographic mismatch
        byte[] clientFinal = client.evaluateChallenge(firstResult.responseBytes());
        RoundResult secondResult = handler.evaluateRound(clientFinal)
                .toCompletableFuture().get();

        // Then
        assertThat(secondResult).isInstanceOf(RoundResult.Failure.class);
        assertThat(((RoundResult.Failure) secondResult).exception()).isInstanceOf(SaslException.class);

        client.dispose();
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
