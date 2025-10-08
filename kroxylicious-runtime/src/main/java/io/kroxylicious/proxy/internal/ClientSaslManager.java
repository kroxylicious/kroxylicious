/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.internal.subject.SubjectBuilder;
import io.kroxylicious.proxy.tag.VisibleForTesting;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.Nullable;

public class ClientSaslManager {

    @VisibleForTesting
    static @Nullable X509Certificate getPeerTlsCertificate(@Nullable SslHandler sslHandler) {
        if (sslHandler != null) {
            SSLSession session = sslHandler.engine().getSession();

            Certificate[] peerCertificates;
            try {
                peerCertificates = session.getPeerCertificates();
            }
            catch (SSLPeerUnverifiedException e) {
                peerCertificates = null;
            }
            if (peerCertificates != null && peerCertificates.length > 0) {
                return Objects.requireNonNull((X509Certificate) peerCertificates[0]);
            }
            else {
                return null;
            }
        }
        else {
            return null;
        }
    }

    @VisibleForTesting
    static @Nullable X509Certificate localTlsCertificate(@Nullable SslHandler sslHandler) {
        if (sslHandler != null) {
            SSLSession session = sslHandler.engine().getSession();
            Certificate[] localCertificates = session.getLocalCertificates();
            if (localCertificates != null && localCertificates.length > 0) {
                return Objects.requireNonNull((X509Certificate) localCertificates[0]);
            }
            else {
                return null;
            }
        }
        else {
            return null;
        }
    }

    private static AttributeKey<ClientSaslManager> ATTR_KEY = AttributeKey.newInstance("ClientSaslManager");

    private record Authorized(
                              String authorizationId,
                              String mechanismName)
            implements ClientSaslContext {}

    private final SubjectBuilder subjectBuilder;
    private final @Nullable ClientTlsContext clientTlsContext;
    private @Nullable Authorized clientAuthorization;
    private CompletionStage<Subject> clientSubject = CompletableFuture.completedFuture(Subject.ANONYMOUS);

    ClientSaslManager(SubjectBuilder subjectBuilder,
                             @Nullable ClientTlsContext clientTlsContext) {

        this.subjectBuilder = subjectBuilder;
        this.clientTlsContext = clientTlsContext;
        this.clientAuthorization = null;
    }

    public static ClientSaslManager bindManager(Channel inboundChannel, SubjectBuilder subjectBuilder) {
        ClientTlsContextImpl tlsContext = Optional.ofNullable(inboundChannel.pipeline().get(SslHandler.class))
                .map(clientFacingSslHandler -> new ClientTlsContextImpl(
                        Objects.requireNonNull(localTlsCertificate(clientFacingSslHandler)), getPeerTlsCertificate(clientFacingSslHandler)))
                .orElse(null);

        ClientSaslManager manager = new ClientSaslManager(subjectBuilder, tlsContext);

        inboundChannel.attr(ATTR_KEY).set(manager);

        return manager;
    }

    public static ClientSaslManager recoverBoundManager(Channel inboundChannel) {
        return inboundChannel.attr(ATTR_KEY).get();
    }

    CompletableFuture<?> clientSaslAuthenticationSuccess(

                                                         String mechanism,
                                                         String clientAuthorizationId) {
        Objects.requireNonNull(mechanism, "mechanism");
        Objects.requireNonNull(clientAuthorizationId, "clientAuthorizationId");
        this.clientAuthorization = new Authorized(clientAuthorizationId, mechanism);
        this.clientSubject = subjectBuilder.buildSubject(new SubjectBuilder.Context() {
            @Override
            public Optional<ClientTlsContext> clientTlsContext() {
                return Optional.ofNullable(clientTlsContext);
            }

            @Override
            public Optional<ClientSaslContext> clientSaslContext() {
                return ClientSaslManager.this.clientSaslContext();
            }
        });
        CompletableFuture<Subject> completableFuture = this.clientSubject.toCompletableFuture();
        return completableFuture;
    }

    public CompletionStage<Subject> authenticatedSubject() {
        return this.clientSubject;
    }

    void clientSaslAuthenticationFailure() {
        this.clientAuthorization = null;
        this.clientSubject = CompletableFuture.completedFuture(Subject.ANONYMOUS);
    }

    public Optional<ClientSaslContext> clientSaslContext() {
        return Optional.ofNullable(clientAuthorization);
    }

    public Optional<ClientTlsContext> clientTlsContext() {
        return Optional.ofNullable(clientTlsContext);
    }
}
