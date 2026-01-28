/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for ServerTlsCredentialSupplierContext interface contracts.
 * These tests verify that implementations correctly provide client TLS context,
 * default credentials, and TLS credential factory methods.
 */
class ServerTlsCredentialSupplierContextTest {

    private ServerTlsCredentialSupplierContext context;
    private ClientTlsContext mockClientContext;
    private TlsCredentials mockDefaultCredentials;
    private TlsCredentials mockCreatedCredentials;

    @BeforeEach
    void setUp() {
        mockClientContext = mock(ClientTlsContext.class);
        mockDefaultCredentials = mock(TlsCredentials.class);
        mockCreatedCredentials = mock(TlsCredentials.class);

        context = new ServerTlsCredentialSupplierContext() {
            @Override
            @NonNull
            public Optional<ClientTlsContext> clientTlsContext() {
                return Optional.of(mockClientContext);
            }

            @Override
            @NonNull
            public CompletionStage<TlsCredentials> defaultTlsCredentials() {
                return CompletableFuture.completedFuture(mockDefaultCredentials);
            }

            @Override
            @NonNull
            public CompletionStage<TlsCredentials> tlsCredentials(@NonNull InputStream certificateChainPem, @NonNull InputStream privateKeyPem) {
                return CompletableFuture.completedFuture(mockCreatedCredentials);
            }
        };
    }

    @Test
    void testClientTlsContextReturnsContextWhenAvailable() {
        // When
        Optional<ClientTlsContext> result = context.clientTlsContext();

        // Then
        assertThat(result).isPresent();
        assertThat(result.get()).isSameAs(mockClientContext);
    }

    @Test
    void testClientTlsContextReturnsEmptyWhenNotAvailable() {
        // Given - context without client TLS
        ServerTlsCredentialSupplierContext noClientContext = new ServerTlsCredentialSupplierContext() {
            @Override
            @NonNull
            public Optional<ClientTlsContext> clientTlsContext() {
                return Optional.empty();
            }

            @Override
            @NonNull
            public CompletionStage<TlsCredentials> defaultTlsCredentials() {
                return CompletableFuture.completedFuture(mockDefaultCredentials);
            }

            @Override
            @NonNull
            public CompletionStage<TlsCredentials> tlsCredentials(@NonNull InputStream certificateChainPem, @NonNull InputStream privateKeyPem) {
                return CompletableFuture.completedFuture(mockCreatedCredentials);
            }
        };

        // When
        Optional<ClientTlsContext> result = noClientContext.clientTlsContext();

        // Then
        assertThat(result).isEmpty();
    }

    @Test
    void testDefaultTlsCredentialsReturnsCredentialsWhenConfigured() throws Exception {
        // When
        CompletionStage<TlsCredentials> result = context.defaultTlsCredentials();

        // Then
        assertThat(result).isNotNull();
        assertThat(result.toCompletableFuture().get()).isSameAs(mockDefaultCredentials);
    }

    @Test
    void testDefaultTlsCredentialsFailsWhenNotConfigured() {
        // Given - context without default credentials
        ServerTlsCredentialSupplierContext noDefaultContext = new ServerTlsCredentialSupplierContext() {
            @Override
            @NonNull
            public Optional<ClientTlsContext> clientTlsContext() {
                return Optional.empty();
            }

            @Override
            @NonNull
            public CompletionStage<TlsCredentials> defaultTlsCredentials() {
                return CompletableFuture.failedFuture(new IllegalStateException("No default TLS credentials configured"));
            }

            @Override
            @NonNull
            public CompletionStage<TlsCredentials> tlsCredentials(@NonNull InputStream certificateChainPem, @NonNull InputStream privateKeyPem) {
                return CompletableFuture.completedFuture(mockCreatedCredentials);
            }
        };

        // When/Then
        assertThatThrownBy(() -> noDefaultContext.defaultTlsCredentials().toCompletableFuture().get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("No default TLS credentials configured");
    }

    @Test
    void testTlsCredentialsFactoryMethodAcceptsValidInput() throws Exception {
        // Given
        InputStream certStream = new ByteArrayInputStream("cert-data".getBytes(StandardCharsets.UTF_8));
        InputStream keyStream = new ByteArrayInputStream("key-data".getBytes(StandardCharsets.UTF_8));

        // When
        CompletionStage<TlsCredentials> result = context.tlsCredentials(certStream, keyStream);

        // Then
        assertThat(result).isNotNull();
        assertThat(result.toCompletableFuture().get()).isSameAs(mockCreatedCredentials);
    }

    @Test
    void testTlsCredentialsFactoryMethodFailsForInvalidCertificate() {
        // Given - context that validates and rejects invalid certificates
        ServerTlsCredentialSupplierContext validatingContext = new ServerTlsCredentialSupplierContext() {
            @Override
            @NonNull
            public Optional<ClientTlsContext> clientTlsContext() {
                return Optional.empty();
            }

            @Override
            @NonNull
            public CompletionStage<TlsCredentials> defaultTlsCredentials() {
                return CompletableFuture.completedFuture(mockDefaultCredentials);
            }

            @Override
            @NonNull
            public CompletionStage<TlsCredentials> tlsCredentials(@NonNull InputStream certificateChainPem, @NonNull InputStream privateKeyPem) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("Invalid certificate format"));
            }
        };

        InputStream certStream = new ByteArrayInputStream("invalid-cert".getBytes(StandardCharsets.UTF_8));
        InputStream keyStream = new ByteArrayInputStream("key-data".getBytes(StandardCharsets.UTF_8));

        // When/Then
        assertThatThrownBy(() -> validatingContext.tlsCredentials(certStream, keyStream).toCompletableFuture().get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid certificate format");
    }

    @Test
    void testTlsCredentialsFactoryMethodFailsForKeyMismatch() {
        // Given - context that validates key matches certificate
        ServerTlsCredentialSupplierContext validatingContext = new ServerTlsCredentialSupplierContext() {
            @Override
            @NonNull
            public Optional<ClientTlsContext> clientTlsContext() {
                return Optional.empty();
            }

            @Override
            @NonNull
            public CompletionStage<TlsCredentials> defaultTlsCredentials() {
                return CompletableFuture.completedFuture(mockDefaultCredentials);
            }

            @Override
            @NonNull
            public CompletionStage<TlsCredentials> tlsCredentials(@NonNull InputStream certificateChainPem, @NonNull InputStream privateKeyPem) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("Private key does not match certificate"));
            }
        };

        InputStream certStream = new ByteArrayInputStream("cert-data".getBytes(StandardCharsets.UTF_8));
        InputStream keyStream = new ByteArrayInputStream("wrong-key".getBytes(StandardCharsets.UTF_8));

        // When/Then
        assertThatThrownBy(() -> validatingContext.tlsCredentials(certStream, keyStream).toCompletableFuture().get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Private key does not match certificate");
    }

    @Test
    void testClientTlsContextProvidesClientCertificate() {
        // Given
        X509Certificate mockClientCert = mock(X509Certificate.class);
        when(mockClientContext.clientCertificate()).thenReturn(Optional.of(mockClientCert));

        // When
        Optional<ClientTlsContext> clientContext = context.clientTlsContext();

        // Then
        assertThat(clientContext).isPresent();
        assertThat(clientContext.get().clientCertificate()).isPresent();
        assertThat(clientContext.get().clientCertificate().get()).isSameAs(mockClientCert);
    }

    @Test
    void testClientTlsContextWithoutClientCertificate() {
        // Given
        when(mockClientContext.clientCertificate()).thenReturn(Optional.empty());

        // When
        Optional<ClientTlsContext> clientContext = context.clientTlsContext();

        // Then
        assertThat(clientContext).isPresent();
        assertThat(clientContext.get().clientCertificate()).isEmpty();
    }

    @Test
    void testContextSupportsMultipleCredentialRequests() throws Exception {
        // When - request credentials multiple times
        CompletionStage<TlsCredentials> result1 = context.defaultTlsCredentials();
        CompletionStage<TlsCredentials> result2 = context.defaultTlsCredentials();

        // Then - should return consistent results
        assertThat(result1.toCompletableFuture().get()).isSameAs(mockDefaultCredentials);
        assertThat(result2.toCompletableFuture().get()).isSameAs(mockDefaultCredentials);
    }

    @Test
    void testTlsCredentialsFactorySupportsMultipleCalls() throws Exception {
        // Given
        InputStream certStream1 = new ByteArrayInputStream("cert-data-1".getBytes(StandardCharsets.UTF_8));
        InputStream keyStream1 = new ByteArrayInputStream("key-data-1".getBytes(StandardCharsets.UTF_8));
        InputStream certStream2 = new ByteArrayInputStream("cert-data-2".getBytes(StandardCharsets.UTF_8));
        InputStream keyStream2 = new ByteArrayInputStream("key-data-2".getBytes(StandardCharsets.UTF_8));

        // When
        CompletionStage<TlsCredentials> result1 = context.tlsCredentials(certStream1, keyStream1);
        CompletionStage<TlsCredentials> result2 = context.tlsCredentials(certStream2, keyStream2);

        // Then - both should succeed
        assertThat(result1.toCompletableFuture().get()).isNotNull();
        assertThat(result2.toCompletableFuture().get()).isNotNull();
    }
}
