/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ClientSubjectManagerTest {

    static List<Arguments> clientTlsContext() throws SSLPeerUnverifiedException {

        X509Certificate proxyCertificate = mock(X509Certificate.class);
        X509Certificate proxyIssuer = mock(X509Certificate.class);
        X509Certificate clientCertificate = mock(X509Certificate.class);
        X509Certificate clientIssuer = mock(X509Certificate.class);
        return List.of(
                Arguments.argumentSet("mTLS self-signed",
                        getSslHandler(List.of(proxyCertificate), false, List.of(clientCertificate)),
                        clientCertificate, proxyCertificate),
                Arguments.argumentSet("mTLS chains",
                        getSslHandler(List.of(proxyCertificate, proxyIssuer), false, List.of(clientCertificate, clientIssuer)),
                        clientCertificate, proxyCertificate),
                Arguments.argumentSet("null client cert",
                        getSslHandler(List.of(proxyCertificate), false, null),
                        null, proxyCertificate),
                Arguments.argumentSet("empty client cert",
                        getSslHandler(List.of(proxyCertificate), false, List.of()),
                        null, proxyCertificate),
                Arguments.argumentSet("peer unverified",
                        getSslHandler(List.of(proxyCertificate), true, null),
                        null, proxyCertificate),
                Arguments.argumentSet("No TLS",
                        null, null, null),
                Arguments.argumentSet("No TLS (empty local certs)",
                        getSslHandler(List.of(), true, null),
                        null, null),
                Arguments.argumentSet("No TLS (empty local certs)",
                        getSslHandler(null, true, null),
                        null, null));

    }

    private static SSLSession getSslHandler(@Nullable List<X509Certificate> proxyCertificates,
                                            boolean peerUnverified,
                                            @Nullable List<X509Certificate> clientCertificates)
            throws SSLPeerUnverifiedException {
        var session = mock(SSLSession.class);
        when(session.getLocalCertificates()).thenReturn(proxyCertificates != null ? proxyCertificates.toArray(new Certificate[0]) : null);
        if (peerUnverified) {
            if (clientCertificates != null) {
                throw new IllegalStateException();
            }
            when(session.getPeerCertificates()).thenThrow(new SSLPeerUnverifiedException(""));
        }
        else {
            when(session.getPeerCertificates()).thenReturn(clientCertificates != null ? clientCertificates.toArray(new Certificate[0]) : null);
        }
        return session;
    }

    @ParameterizedTest
    @MethodSource
    void clientTlsContext(@Nullable SSLSession session,
                          @Nullable X509Certificate clientCertificate,
                          @Nullable X509Certificate proxyCertificate) {

        // when
        var localCert = ClientSubjectManager.localTlsCertificate(session);
        var peerCert = ClientSubjectManager.peerTlsCertificate(session);

        // then
        assertThat(localCert).isSameAs(proxyCertificate);
        assertThat(peerCert).isSameAs(clientCertificate);
    }

    @Test
    void initialState() {
        // Given
        ClientSubjectManager impl = new ClientSubjectManager();
        // Then
        assertThat(impl.clientSaslContext()).isEmpty();
    }

    @Test
    void transitionInitialToAuthorized() {
        // Given
        ClientSubjectManager impl = new ClientSubjectManager();
        impl.subjectFromTransport(null, context -> CompletableFuture.completedStage(Subject.anonymous()));
        // When
        impl.clientSaslAuthenticationSuccess("FOO", new Subject(new User("bob")));
        // Then
        assertThat(impl.clientSaslContext()).hasValueSatisfying(csc -> {
            assertThat(csc.mechanismName()).isEqualTo("FOO");
            assertThat(csc.authorizationId()).isEqualTo("bob");
        });
    }

    @Test
    void transitionInitialToFailed() {
        // Given
        ClientSubjectManager impl = new ClientSubjectManager();
        impl.subjectFromTransport(null, context -> CompletableFuture.completedStage(Subject.anonymous()));
        // When
        impl.clientSaslAuthenticationFailure();
        // Then
        assertThat(impl.clientSaslContext()).isEmpty();
    }

    @Test
    void transitionAuthorizedToAuthorized() {
        // Given
        ClientSubjectManager impl = new ClientSubjectManager();
        impl.subjectFromTransport(null, context -> CompletableFuture.completedStage(Subject.anonymous()));
        impl.clientSaslAuthenticationSuccess("FOO", new Subject(new User("bob")));
        // When
        impl.clientSaslAuthenticationSuccess("BAR", new Subject(new User("sue")));
        // Then
        assertThat(impl.clientSaslContext()).hasValueSatisfying(csc -> {
            assertThat(csc.mechanismName()).isEqualTo("BAR");
            assertThat(csc.authorizationId()).isEqualTo("sue");
        });
    }

    @Test
    void transitionAuthorizedToFailed() {
        // Given
        ClientSubjectManager impl = new ClientSubjectManager();
        impl.subjectFromTransport(null, context -> CompletableFuture.completedStage(Subject.anonymous()));
        impl.clientSaslAuthenticationSuccess("FOO", new Subject(new User("bob")));
        // When
        impl.clientSaslAuthenticationFailure();
        // Then
        assertThat(impl.clientSaslContext()).isEmpty();
    }

    @Test
    void transitionFailedToAuthorized() {
        // Given
        ClientSubjectManager impl = new ClientSubjectManager();
        impl.subjectFromTransport(null, context -> CompletableFuture.completedStage(Subject.anonymous()));
        impl.clientSaslAuthenticationFailure();

        // When
        impl.clientSaslAuthenticationSuccess("FOO", new Subject(new User("bob")));
        // Then
        assertThat(impl.clientSaslContext()).hasValueSatisfying(csc -> {
            assertThat(csc.mechanismName()).isEqualTo("FOO");
            assertThat(csc.authorizationId()).isEqualTo("bob");
        });
    }

}
