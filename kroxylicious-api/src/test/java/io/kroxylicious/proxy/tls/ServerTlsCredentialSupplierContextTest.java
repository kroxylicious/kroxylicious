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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ServerTlsCredentialSupplierContextTest {

    private ServerTlsCredentialSupplierContext context;
    private ClientTlsContext mockClientContext;
    private TlsCredentials mockCreatedCredentials;
    private PrivateKey mockKey;
    private Certificate[] mockChain;

    @BeforeEach
    void setUp() {
        mockClientContext = mock(ClientTlsContext.class);
        mockCreatedCredentials = mock(TlsCredentials.class);
        mockKey = mock(PrivateKey.class);
        mockChain = new Certificate[]{ mock(X509Certificate.class) };

        context = new ServerTlsCredentialSupplierContext() {
            @Override
            @NonNull
            public Optional<ClientTlsContext> clientTlsContext() {
                return Optional.of(mockClientContext);
            }

            @Override
            @NonNull
            public TlsCredentials tlsCredentials(@NonNull PrivateKey key, @NonNull Certificate[] certificateChain) {
                return mockCreatedCredentials;
            }
        };
    }

    @Test
    void testClientTlsContextReturnsContextWhenAvailable() {
        Optional<ClientTlsContext> result = context.clientTlsContext();
        assertThat(result).isPresent();
        assertThat(result.get()).isSameAs(mockClientContext);
    }

    @Test
    void testClientTlsContextReturnsEmptyWhenNotAvailable() {
        ServerTlsCredentialSupplierContext noClientContext = new ServerTlsCredentialSupplierContext() {
            @Override
            @NonNull
            public Optional<ClientTlsContext> clientTlsContext() {
                return Optional.empty();
            }

            @Override
            @NonNull
            public TlsCredentials tlsCredentials(@NonNull PrivateKey key, @NonNull Certificate[] certificateChain) {
                return mockCreatedCredentials;
            }
        };

        assertThat(noClientContext.clientTlsContext()).isEmpty();
    }

    @Test
    void testTlsCredentialsFactoryMethodAcceptsValidInput() {
        TlsCredentials result = context.tlsCredentials(mockKey, mockChain);
        assertThat(result).isSameAs(mockCreatedCredentials);
    }

    @Test
    void testTlsCredentialsFactoryMethodFailsForInvalidCertificate() {
        ServerTlsCredentialSupplierContext validatingContext = new ServerTlsCredentialSupplierContext() {
            @Override
            @NonNull
            public Optional<ClientTlsContext> clientTlsContext() {
                return Optional.empty();
            }

            @Override
            @NonNull
            public TlsCredentials tlsCredentials(@NonNull PrivateKey key, @NonNull Certificate[] certificateChain) {
                throw new IllegalArgumentException("Invalid certificate format");
            }
        };

        assertThatThrownBy(() -> validatingContext.tlsCredentials(mockKey, mockChain))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid certificate format");
    }

    @Test
    void testTlsCredentialsFactoryMethodFailsForKeyMismatch() {
        ServerTlsCredentialSupplierContext validatingContext = new ServerTlsCredentialSupplierContext() {
            @Override
            @NonNull
            public Optional<ClientTlsContext> clientTlsContext() {
                return Optional.empty();
            }

            @Override
            @NonNull
            public TlsCredentials tlsCredentials(@NonNull PrivateKey key, @NonNull Certificate[] certificateChain) {
                throw new IllegalArgumentException("Private key does not match certificate");
            }
        };

        assertThatThrownBy(() -> validatingContext.tlsCredentials(mockKey, mockChain))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Private key does not match certificate");
    }

    @Test
    void testClientTlsContextProvidesClientCertificate() {
        X509Certificate mockClientCert = mock(X509Certificate.class);
        when(mockClientContext.clientCertificate()).thenReturn(Optional.of(mockClientCert));

        Optional<ClientTlsContext> clientContext = context.clientTlsContext();
        assertThat(clientContext).isPresent();
        assertThat(clientContext.get().clientCertificate()).isPresent();
        assertThat(clientContext.get().clientCertificate().get()).isSameAs(mockClientCert);
    }

    @Test
    void testClientTlsContextWithoutClientCertificate() {
        when(mockClientContext.clientCertificate()).thenReturn(Optional.empty());

        Optional<ClientTlsContext> clientContext = context.clientTlsContext();
        assertThat(clientContext).isPresent();
        assertThat(clientContext.get().clientCertificate()).isEmpty();
    }

    @Test
    void testTlsCredentialsFactorySupportsMultipleCalls() {
        TlsCredentials result1 = context.tlsCredentials(mockKey, mockChain);
        TlsCredentials result2 = context.tlsCredentials(mockKey, mockChain);

        assertThat(result1).isNotNull();
        assertThat(result2).isNotNull();
    }
}
