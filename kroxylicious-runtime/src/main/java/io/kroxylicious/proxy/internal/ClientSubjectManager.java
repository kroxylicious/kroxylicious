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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.authentication.ClientSaslContext;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.TransportSubjectBuilder;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.tag.VisibleForTesting;
import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.Nullable;

public class ClientSubjectManager implements
        ClientSaslContext,
        ClientTlsContext,
        TransportSubjectBuilder.Context {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientSubjectManager.class);

    @VisibleForTesting
    static @Nullable X509Certificate peerTlsCertificate(@Nullable SSLSession session) {
        if (session != null) {

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
    static @Nullable X509Certificate localTlsCertificate(@Nullable SSLSession session) {
        if (session != null) {
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

    private @Nullable String mechanismName;
    private Subject subject;
    private @Nullable X509Certificate proxyCertificate;
    private @Nullable X509Certificate clientCertificate;

    ClientSubjectManager() {
        this.proxyCertificate = null;
        this.clientCertificate = null;
        this.mechanismName = null;
        this.subject = Subject.anonymous();
    }

    public void subjectFromTransport(@Nullable SSLSession session,
                                     TransportSubjectBuilder transportSubjectBuilder,
                                     Runnable whenDoneCallback) {
        this.clientCertificate = peerTlsCertificate(session);
        this.proxyCertificate = localTlsCertificate(session);
        transportSubjectBuilder.buildTransportSubject(this).whenComplete((newSubject, error) -> {
            if (error == null) {
                this.subject = newSubject;
            }
            else {
                LOGGER.warn("Failed to build subject from transport information; client will be treated as anonymous", error);
                this.subject = Subject.anonymous();
            }
            this.mechanismName = null;
            whenDoneCallback.run();
        });
    }

    void clientSaslAuthenticationSuccess(
                                         String mechanism,
                                         Subject subject) {
        this.mechanismName = Objects.requireNonNull(mechanism, "mechanism");
        this.subject = Objects.requireNonNull(subject, "subject");
    }

    public Subject authenticatedSubject() {
        return this.subject;
    }

    void clientSaslAuthenticationFailure() {
        this.mechanismName = null;
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
        // Note this is only reachable when clientSaslContext() returned non-empty
        // And currently a Subject must have exactly one User principal
        // => this IllegalStateException is never thrown in practice.
        return subject.uniquePrincipalOfType(User.class)
                .map(User::name)
                .orElseThrow(IllegalStateException::new);
    }
}
