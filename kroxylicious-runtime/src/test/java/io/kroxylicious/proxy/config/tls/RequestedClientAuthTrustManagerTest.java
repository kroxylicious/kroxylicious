/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class RequestedClientAuthTrustManagerTest {

    @Mock
    private X509ExtendedTrustManager delegateTrustManager;

    @Mock
    private X509Certificate mockCertificate;

    private RequestedClientAuthTrustManager trustManager;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        trustManager = new RequestedClientAuthTrustManager(delegateTrustManager);
    }

    @Test
    void getAcceptedIssuers_returnsEmptyArray() {
        // When
        X509Certificate[] acceptedIssuers = trustManager.getAcceptedIssuers();

        // Then - returning empty array indicates client auth is optional
        assertThat(acceptedIssuers)
                .isNotNull()
                .isEmpty();
    }

    @Test
    void checkClientTrusted_withNullChain_allowsConnection() throws CertificateException {
        // When/Then - no certificate presented, connection should be allowed
        assertThatCode(() -> trustManager.checkClientTrusted(null, "RSA"))
                .doesNotThrowAnyException();

        // Verify delegate was never called
        verify(delegateTrustManager, never()).checkClientTrusted(any(), any());
    }

    @Test
    void checkClientTrusted_withEmptyChain_allowsConnection() throws CertificateException {
        // Given
        X509Certificate[] emptyChain = new X509Certificate[0];

        // When/Then - no certificate presented, connection should be allowed
        assertThatCode(() -> trustManager.checkClientTrusted(emptyChain, "RSA"))
                .doesNotThrowAnyException();

        // Verify delegate was never called
        verify(delegateTrustManager, never()).checkClientTrusted(any(), any());
    }

    @Test
    void checkClientTrusted_withValidCertificate_delegatesValidation() throws CertificateException {
        // Given
        X509Certificate[] validChain = new X509Certificate[]{ mockCertificate };

        // When
        trustManager.checkClientTrusted(validChain, "RSA");

        // Then - should delegate to underlying trust manager for validation
        verify(delegateTrustManager).checkClientTrusted(validChain, "RSA");
    }

    @Test
    void checkClientTrusted_withInvalidCertificate_throwsCertificateException() throws CertificateException {
        // Given
        X509Certificate[] invalidChain = new X509Certificate[]{ mockCertificate };
        CertificateException expectedException = new CertificateException("Invalid certificate");

        // Use doThrow().when() pattern for void methods
        Mockito.doThrow(expectedException)
                .when(delegateTrustManager)
                .checkClientTrusted(invalidChain, "RSA");

        // When/Then - should propagate the exception from delegate
        assertThatThrownBy(() -> trustManager.checkClientTrusted(invalidChain, "RSA"))
                .isInstanceOf(CertificateException.class)
                .hasMessage("Invalid certificate");

        verify(delegateTrustManager).checkClientTrusted(invalidChain, "RSA");
    }

    @Test
    void checkClientTrusted_withSocket_withNullChain_allowsConnection() throws CertificateException {
        // When/Then - no certificate presented, connection should be allowed
        assertThatCode(() -> trustManager.checkClientTrusted(null, "RSA", (java.net.Socket) null))
                .doesNotThrowAnyException();

        // Verify delegate was never called - checking the base method since we can't check extended
        verify(delegateTrustManager, never()).checkClientTrusted(any(X509Certificate[].class), any(String.class));
    }

    @Test
    void checkClientTrusted_withSocket_withValidCertificate_delegatesValidation() throws CertificateException {
        // Given
        X509Certificate[] validChain = new X509Certificate[]{ mockCertificate };

        // When
        trustManager.checkClientTrusted(validChain, "RSA", (java.net.Socket) null);

        // Then - should delegate to the extended trust manager method with null Socket
        verify(delegateTrustManager).checkClientTrusted(eq(validChain), eq("RSA"), (Socket) isNull());
    }

    @Test
    void checkClientTrusted_withSSLEngine_withNullChain_allowsConnection() throws CertificateException {
        // When/Then - no certificate presented, connection should be allowed
        assertThatCode(() -> trustManager.checkClientTrusted(null, "RSA", (javax.net.ssl.SSLEngine) null))
                .doesNotThrowAnyException();

        // Verify delegate was never called - checking the base method
        verify(delegateTrustManager, never()).checkClientTrusted(any(X509Certificate[].class), any(String.class));
    }

    @Test
    void checkClientTrusted_withSSLEngine_withValidCertificate_delegatesValidation() throws CertificateException {
        // Given
        X509Certificate[] validChain = new X509Certificate[]{ mockCertificate };

        // When
        trustManager.checkClientTrusted(validChain, "RSA", (javax.net.ssl.SSLEngine) null);

        // Then - should delegate to the extended trust manager method with null SSLEngine
        verify(delegateTrustManager).checkClientTrusted(eq(validChain), eq("RSA"), (SSLEngine) isNull());
    }

    @Test
    void checkServerTrusted_alwaysDelegatesToUnderlying() throws CertificateException {
        // Given
        X509Certificate[] chain = new X509Certificate[]{ mockCertificate };

        // When
        trustManager.checkServerTrusted(chain, "RSA");

        // Then - should always delegate server validation
        verify(delegateTrustManager).checkServerTrusted(chain, "RSA");
    }

    @Test
    void checkServerTrusted_withSocket_alwaysDelegatesToUnderlying() throws CertificateException {
        // Given
        X509Certificate[] chain = new X509Certificate[]{ mockCertificate };

        // When
        trustManager.checkServerTrusted(chain, "RSA", (java.net.Socket) null);

        // Then - should delegate to extended method with null Socket
        verify(delegateTrustManager).checkServerTrusted(eq(chain), eq("RSA"), (Socket) isNull());
    }

    @Test
    void checkServerTrusted_withSSLEngine_alwaysDelegatesToUnderlying() throws CertificateException {
        // Given
        X509Certificate[] chain = new X509Certificate[]{ mockCertificate };

        // When
        trustManager.checkServerTrusted(chain, "RSA", (javax.net.ssl.SSLEngine) null);

        // Then - should delegate to extended method with null SSLEngine
        verify(delegateTrustManager).checkServerTrusted(eq(chain), eq("RSA"), (SSLEngine) isNull());
    }
}
