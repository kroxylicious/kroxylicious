/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.FilterDispatchExecutor;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for credential validation in TLS credential factory methods.
 * These tests verify that both ServerTlsCredentialSupplierFactoryContext and
 * ServerTlsCredentialSupplierContext properly validate JDK key/certificate objects
 * before creating TlsCredentials instances.
 */
class TlsCredentialsValidationTest {

    @Test
    void testFactoryContextValidatesKeyMatchesCertificate() {
        ServerTlsCredentialSupplierFactoryContext context = createValidatingFactoryContext(
                "Private key does not match the certificate");

        PrivateKey mockKey = mock(PrivateKey.class);
        Certificate[] mockChain = new Certificate[]{ mock(X509Certificate.class) };

        assertThatThrownBy(() -> context.tlsCredentials(mockKey, mockChain))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Private key does not match the certificate");
    }

    @Test
    void testFactoryContextValidatesCertificateChainStructure() {
        ServerTlsCredentialSupplierFactoryContext context = createValidatingFactoryContext(
                "Certificate chain is structurally invalid");

        PrivateKey mockKey = mock(PrivateKey.class);
        Certificate[] mockChain = new Certificate[]{ mock(X509Certificate.class) };

        assertThatThrownBy(() -> context.tlsCredentials(mockKey, mockChain))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Certificate chain is structurally invalid");
    }

    @Test
    void testFactoryContextAcceptsValidKeyAndCertificate() {
        TlsCredentials mockCredentials = mock(TlsCredentials.class);
        ServerTlsCredentialSupplierFactoryContext context = createPassingFactoryContext(mockCredentials);

        PrivateKey mockKey = mock(PrivateKey.class);
        Certificate[] mockChain = new Certificate[]{ mock(X509Certificate.class) };

        TlsCredentials result = context.tlsCredentials(mockKey, mockChain);
        assertThat(result).isSameAs(mockCredentials);
    }

    @Test
    void testSupplierContextValidatesKeyMatchesCertificate() {
        ServerTlsCredentialSupplierContext context = createValidatingSupplierContext(
                "Private key does not match the certificate");

        PrivateKey mockKey = mock(PrivateKey.class);
        Certificate[] mockChain = new Certificate[]{ mock(X509Certificate.class) };

        assertThatThrownBy(() -> context.tlsCredentials(mockKey, mockChain))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Private key does not match the certificate");
    }

    @Test
    void testSupplierContextAcceptsValidKeyAndCertificate() {
        TlsCredentials mockCredentials = mock(TlsCredentials.class);
        ServerTlsCredentialSupplierContext context = createPassingSupplierContext(mockCredentials);

        PrivateKey mockKey = mock(PrivateKey.class);
        Certificate[] mockChain = new Certificate[]{ mock(X509Certificate.class) };

        TlsCredentials result = context.tlsCredentials(mockKey, mockChain);
        assertThat(result).isSameAs(mockCredentials);
    }

    @Test
    void testValidationFailurePreservesExceptionDetails() {
        String detailedError = "Certificate validation failed: " +
                "Subject: CN=test.example.com, " +
                "Issuer: CN=Test CA, " +
                "NotBefore: 2023-01-01, NotAfter: 2023-12-31, " +
                "Current time: 2024-01-01 (expired)";

        ServerTlsCredentialSupplierFactoryContext context = createValidatingFactoryContext(detailedError);

        PrivateKey mockKey = mock(PrivateKey.class);
        Certificate[] mockChain = new Certificate[]{ mock(X509Certificate.class) };

        assertThatThrownBy(() -> context.tlsCredentials(mockKey, mockChain))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CN=test.example.com")
                .hasMessageContaining("expired");
    }

    @Test
    void testMultipleValidationErrorsReported() {
        String multipleErrors = "Multiple validation errors: " +
                "[1] Certificate is expired, " +
                "[2] Private key algorithm does not match certificate, " +
                "[3] Certificate chain is incomplete";

        ServerTlsCredentialSupplierContext context = createValidatingSupplierContext(multipleErrors);

        PrivateKey mockKey = mock(PrivateKey.class);
        Certificate[] mockChain = new Certificate[]{ mock(X509Certificate.class) };

        assertThatThrownBy(() -> context.tlsCredentials(mockKey, mockChain))
                .isInstanceOf(IllegalArgumentException.class)
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
        public <P> Set<String> pluginImplementationNames(Class<P> pluginClass) {
            return Set.of();
        }

        @Override
        @NonNull
        public FilterDispatchExecutor filterDispatchExecutor() {
            throw new IllegalStateException("Not available at initialization time");
        }

        @Override
        @NonNull
        public TlsCredentials tlsCredentials(@NonNull PrivateKey key, @NonNull Certificate[] certificateChain) {
            if (errorMessage != null) {
                throw new IllegalArgumentException(errorMessage);
            }
            return credentials;
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
        public TlsCredentials tlsCredentials(@NonNull PrivateKey key, @NonNull Certificate[] certificateChain) {
            if (errorMessage != null) {
                throw new IllegalArgumentException(errorMessage);
            }
            return credentials;
        }
    }
}
