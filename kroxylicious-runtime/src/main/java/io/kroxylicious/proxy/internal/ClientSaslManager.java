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

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import io.netty.channel.Channel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.tag.VisibleForTesting;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.Nullable;

public class ClientSaslManager implements
        ClientSaslContext,
        ClientTlsContext,
        TransportSubjectBuilder.Context {

    @VisibleForTesting
    static @Nullable X509Certificate peerTlsCertificate(@Nullable SslHandler sslHandler) {
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

    static final AttributeKey<ClientSaslManager> ATTR_KEY = AttributeKey.newInstance(ClientSaslManager.class.getName());

    private @Nullable String mechanismName;
    private @Nullable String authorizationId;
    private Subject subject;
    private final @Nullable X509Certificate proxyCertificate;
    private final @Nullable X509Certificate clientCertificate;

    @VisibleForTesting
    ClientSaslManager(
                      @Nullable X509Certificate proxyCertificate,
                      @Nullable X509Certificate clientCertificate,
                      Subject initialSubject) {
        this.proxyCertificate = proxyCertificate;
        this.clientCertificate = clientCertificate;
        this.mechanismName = null;
        this.authorizationId = null;
        this.subject = initialSubject;
    }

    public static ClientSaslManager create(Channel inboundChannel) {
        return Optional.ofNullable(inboundChannel.pipeline().get(SslHandler.class))
                .map(clientFacingSslHandler -> new ClientSaslManager(localTlsCertificate(clientFacingSslHandler),
                        peerTlsCertificate(clientFacingSslHandler),
                        Subject.anonymous()))
                .orElse(new ClientSaslManager(null,
                        null,
                        Subject.anonymous()));
    }

    public ClientSaslManager bindManager(Channel clientChannel, Subject newSubject) {
        this.subject = newSubject;
        clientChannel.attr(ClientSaslManager.ATTR_KEY).set(this);
        return this;
    }

    public static ClientSaslManager recoverBoundManager(Channel inboundChannel) {
        return inboundChannel.attr(ATTR_KEY).get();
    }

    void clientSaslAuthenticationSuccess(
                                         String mechanism,
                                         String clientAuthorizationId,
                                         Subject subject) {
        this.mechanismName = Objects.requireNonNull(mechanism, "mechanism");
        this.authorizationId = Objects.requireNonNull(clientAuthorizationId, "clientAuthorizationId");
        this.subject = Objects.requireNonNull(subject, "subject");
    }

    public Subject authenticatedSubject() {
        return this.subject;
    }

    void clientSaslAuthenticationFailure() {
        this.mechanismName = null;
        this.authorizationId = null;
        this.subject = Subject.anonymous();
    }

    public Optional<ClientTlsContext> clientTlsContext() {
        return proxyCertificate != null ? Optional.of(this) : Optional.empty();
    }

    @Override
    public X509Certificate proxyServerCertificate() {
        // Note this is only reachable when clientTlsContext() returned non-empty => proxyCertificate is not null
        return Objects.requireNonNull(proxyCertificate);
    }

    @Override
    public Optional<X509Certificate> clientCertificate() {
        return Optional.ofNullable(clientCertificate);
    }

    public Optional<ClientSaslContext> clientSaslContext() {
        return mechanismName != null ? Optional.of(this) : Optional.empty();
    }

    @Override
    public String mechanismName() {
        // Note this is only reachable when clientSaslContext() returned non-empty => mechanismName is not null
        return Objects.requireNonNull(mechanismName);
    }

    @Override
    public String authorizationId() {
        // Note this is only reachable when clientSaslContext() returned non-empty => authorizationId is not null
        return Objects.requireNonNull(authorizationId);
    }
}
