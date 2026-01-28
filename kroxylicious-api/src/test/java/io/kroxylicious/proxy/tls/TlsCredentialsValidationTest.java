/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for certificate validation in TLS credential factory methods.
 * These tests verify that both ServerTlsCredentialSupplierFactoryContext and
 * ServerTlsCredentialSupplierContext properly validate certificates before
 * creating TlsCredentials instances.
 */
class TlsCredentialsValidationTest {

    @Test
    void testFactoryContextValidatesCertificateFormat() {
        // Given - context that validates certificate format
        ServerTlsCredentialSupplierFactoryContext context = createValidatingFactoryContext(
                "Certificate data is malformed or cannot be parsed");

        InputStream invalidCert = new ByteArrayInputStream("INVALID-CERT-DATA".getBytes(StandardCharsets.UTF_8));
        InputStream validKey = new ByteArrayInputStream("valid-key-data".getBytes(StandardCharsets.UTF_8));

        // When/Then
        assertThatThrownBy(() -> context.tlsCredentials(invalidCert, validKey).toCompletableFuture().get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Certificate data is malformed");
    }

    @Test
    void testFactoryContextValidatesPrivateKeyFormat() {
        // Given - context that validates private key format
        ServerTlsCredentialSupplierFactoryContext context = createValidatingFactoryContext(
                "Private key data is malformed or cannot be parsed");

        InputStream validCert = new ByteArrayInputStream("valid-cert-data".getBytes(StandardCharsets.UTF_8));
        InputStream invalidKey = new ByteArrayInputStream("INVALID-KEY-DATA".getBytes(StandardCharsets.UTF_8));

        // When/Then
        assertThatThrownBy(() -> context.tlsCredentials(validCert, invalidKey).toCompletableFuture().get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Private key data is malformed");
    }

    @Test
    void testFactoryContextValidatesKeyMatchesCertificate() {
        // Given - context that validates key matches certificate
        ServerTlsCredentialSupplierFactoryContext context = createValidatingFactoryContext(
                "Private key does not match the certificate");

        InputStream cert = new ByteArrayInputStream("cert-for-key-A".getBytes(StandardCharsets.UTF_8));
        InputStream key = new ByteArrayInputStream("key-B-data".getBytes(StandardCharsets.UTF_8));

        // When/Then
        assertThatThrownBy(() -> context.tlsCredentials(cert, key).toCompletableFuture().get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Private key does not match the certificate");
    }

    @Test
    void testFactoryContextValidatesCertificateDates() {
        // Given - context that validates certificate dates
        ServerTlsCredentialSupplierFactoryContext context = createValidatingFactoryContext(
                "Certificate has expired");

        InputStream expiredCert = new ByteArrayInputStream("expired-cert-data".getBytes(StandardCharsets.UTF_8));
        InputStream key = new ByteArrayInputStream("key-data".getBytes(StandardCharsets.UTF_8));

        // When/Then
        assertThatThrownBy(() -> context.tlsCredentials(expiredCert, key).toCompletableFuture().get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Certificate has expired");
    }

    @Test
    void testFactoryContextValidatesCertificateChainStructure() {
        // Given - context that validates certificate chain structure
        ServerTlsCredentialSupplierFactoryContext context = createValidatingFactoryContext(
                "Certificate chain is structurally invalid");

        InputStream invalidChain = new ByteArrayInputStream("invalid-chain-structure".getBytes(StandardCharsets.UTF_8));
        InputStream key = new ByteArrayInputStream("key-data".getBytes(StandardCharsets.UTF_8));

        // When/Then
        assertThatThrownBy(() -> context.tlsCredentials(invalidChain, key).toCompletableFuture().get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Certificate chain is structurally invalid");
    }

    @Test
    void testFactoryContextAcceptsValidCertificateAndKey() throws Exception {
        // Given - context that accepts valid input
        TlsCredentials mockCredentials = mock(TlsCredentials.class);
        ServerTlsCredentialSupplierFactoryContext context = createPassingFactoryContext(mockCredentials);

        InputStream validCert = new ByteArrayInputStream("valid-cert-data".getBytes(StandardCharsets.UTF_8));
        InputStream validKey = new ByteArrayInputStream("valid-key-data".getBytes(StandardCharsets.UTF_8));

        // When
        CompletionStage<TlsCredentials> result = context.tlsCredentials(validCert, validKey);

        // Then
        assertThat(result).isNotNull();
        assertThat(result.toCompletableFuture().get()).isSameAs(mockCredentials);
    }

    @Test
    void testSupplierContextValidatesCertificateFormat() {
        // Given - context that validates certificate format
        ServerTlsCredentialSupplierContext context = createValidatingSupplierContext(
                "Certificate data is malformed or cannot be parsed");

        InputStream invalidCert = new ByteArrayInputStream("INVALID-CERT-DATA".getBytes(StandardCharsets.UTF_8));
        InputStream validKey = new ByteArrayInputStream("valid-key-data".getBytes(StandardCharsets.UTF_8));

        // When/Then
        assertThatThrownBy(() -> context.tlsCredentials(invalidCert, validKey).toCompletableFuture().get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Certificate data is malformed");
    }

    @Test
    void testSupplierContextValidatesPrivateKeyFormat() {
        // Given - context that validates private key format
        ServerTlsCredentialSupplierContext context = createValidatingSupplierContext(
                "Private key data is malformed or cannot be parsed");

        InputStream validCert = new ByteArrayInputStream("valid-cert-data".getBytes(StandardCharsets.UTF_8));
        InputStream invalidKey = new ByteArrayInputStream("INVALID-KEY-DATA".getBytes(StandardCharsets.UTF_8));

        // When/Then
        assertThatThrownBy(() -> context.tlsCredentials(validCert, invalidKey).toCompletableFuture().get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Private key data is malformed");
    }

    @Test
    void testSupplierContextValidatesKeyMatchesCertificate() {
        // Given - context that validates key matches certificate
        ServerTlsCredentialSupplierContext context = createValidatingSupplierContext(
                "Private key does not match the certificate");

        InputStream cert = new ByteArrayInputStream("cert-for-key-A".getBytes(StandardCharsets.UTF_8));
        InputStream key = new ByteArrayInputStream("key-B-data".getBytes(StandardCharsets.UTF_8));

        // When/Then
        assertThatThrownBy(() -> context.tlsCredentials(cert, key).toCompletableFuture().get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Private key does not match the certificate");
    }

    @Test
    void testSupplierContextAcceptsValidCertificateAndKey() throws Exception {
        // Given - context that accepts valid input
        TlsCredentials mockCredentials = mock(TlsCredentials.class);
        ServerTlsCredentialSupplierContext context = createPassingSupplierContext(mockCredentials);

        InputStream validCert = new ByteArrayInputStream("valid-cert-data".getBytes(StandardCharsets.UTF_8));
        InputStream validKey = new ByteArrayInputStream("valid-key-data".getBytes(StandardCharsets.UTF_8));

        // When
        CompletionStage<TlsCredentials> result = context.tlsCredentials(validCert, validKey);

        // Then
        assertThat(result).isNotNull();
        assertThat(result.toCompletableFuture().get()).isSameAs(mockCredentials);
    }

    @Test
    void testValidationFailurePreservesExceptionDetails() {
        // Given - context that provides detailed validation errors
        String detailedError = "Certificate validation failed: " +
                "Subject: CN=test.example.com, " +
                "Issuer: CN=Test CA, " +
                "NotBefore: 2023-01-01, NotAfter: 2023-12-31, " +
                "Current time: 2024-01-01 (expired)";

        ServerTlsCredentialSupplierFactoryContext context = createValidatingFactoryContext(detailedError);

        InputStream cert = new ByteArrayInputStream("expired-cert".getBytes(StandardCharsets.UTF_8));
        InputStream key = new ByteArrayInputStream("key".getBytes(StandardCharsets.UTF_8));

        // When/Then
        assertThatThrownBy(() -> context.tlsCredentials(cert, key).toCompletableFuture().get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("CN=test.example.com")
                .hasMessageContaining("expired");
    }

    @Test
    void testMultipleValidationErrorsReported() {
        // Given - context that reports multiple validation errors
        String multipleErrors = "Multiple validation errors: " +
                "[1] Certificate is expired, " +
                "[2] Private key algorithm does not match certificate, " +
                "[3] Certificate chain is incomplete";

        ServerTlsCredentialSupplierContext context = createValidatingSupplierContext(multipleErrors);

        InputStream cert = new ByteArrayInputStream("bad-cert".getBytes(StandardCharsets.UTF_8));
        InputStream key = new ByteArrayInputStream("bad-key".getBytes(StandardCharsets.UTF_8));

        // When/Then
        assertThatThrownBy(() -> context.tlsCredentials(cert, key).toCompletableFuture().get())
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Multiple validation errors")
                .hasMessageContaining("expired")
                .hasMessageContaining("algorithm does not match")
                .hasMessageContaining("incomplete");
    }

    // Helper methods

    private ServerTlsCredentialSupplierFactoryContext createValidatingFactoryContext(String errorMessage) {
        return new TestFactoryContext(errorMessage);
    }

    private ServerTlsCredentialSupplierFactoryContext createPassingFactoryContext(TlsCredentials credentials) {
        return new TestFactoryContext(null, credentials);
    }

    private ServerTlsCredentialSupplierContext createValidatingSupplierContext(String errorMessage) {
        return new TestSupplierContext(errorMessage);
    }

    private ServerTlsCredentialSupplierContext createPassingSupplierContext(TlsCredentials credentials) {
        return new TestSupplierContext(null, credentials);
    }

    // Test implementation classes

    private static class TestFactoryContext implements ServerTlsCredentialSupplierFactoryContext {
        private final String errorMessage;
        private final TlsCredentials credentials;

        TestFactoryContext(String errorMessage) {
            this(errorMessage, null);
        }

        TestFactoryContext(String errorMessage, TlsCredentials credentials) {
            this.errorMessage = errorMessage;
            this.credentials = credentials;
        }

        @Override
        public <P> P pluginInstance(Class<P> pluginClass, String implementationName) {
            return null;
        }

        @Override
        public <P> java.util.Set<String> pluginImplementationNames(Class<P> pluginClass) {
            return java.util.Set.of();
        }

        @Override
        @NonNull
        public io.kroxylicious.proxy.filter.FilterDispatchExecutor filterDispatchExecutor() {
            throw new IllegalStateException("Not available at initialization time");
        }

        @Override
        @NonNull
        public CompletionStage<TlsCredentials> tlsCredentials(@NonNull InputStream certificateChainPem, @NonNull InputStream privateKeyPem) {
            if (errorMessage != null) {
                return CompletableFuture.failedFuture(new IllegalArgumentException(errorMessage));
            }
            return CompletableFuture.completedFuture(credentials);
        }
    }

    private static class TestSupplierContext implements ServerTlsCredentialSupplierContext {
        private final String errorMessage;
        private final TlsCredentials credentials;

        TestSupplierContext(String errorMessage) {
            this(errorMessage, null);
        }

        TestSupplierContext(String errorMessage, TlsCredentials credentials) {
            this.errorMessage = errorMessage;
            this.credentials = credentials;
        }

        @Override
        @NonNull
        public Optional<ClientTlsContext> clientTlsContext() {
            return Optional.empty();
        }

        @Override
        @NonNull
        public CompletionStage<TlsCredentials> defaultTlsCredentials() {
            return CompletableFuture.completedFuture(mock(TlsCredentials.class));
        }

        @Override
        @NonNull
        public CompletionStage<TlsCredentials> tlsCredentials(@NonNull InputStream certificateChainPem, @NonNull InputStream privateKeyPem) {
            if (errorMessage != null) {
                return CompletableFuture.failedFuture(new IllegalArgumentException(errorMessage));
            }
            return CompletableFuture.completedFuture(credentials);
        }
    }
}
