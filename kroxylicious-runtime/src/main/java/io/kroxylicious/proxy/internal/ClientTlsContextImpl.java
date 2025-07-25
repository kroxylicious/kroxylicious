/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.security.cert.X509Certificate;
import java.util.Optional;

import io.kroxylicious.proxy.tls.ClientTlsContext;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;

@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
class ClientTlsContextImpl implements ClientTlsContext {
    private final X509Certificate proxyCertificate;
    private final @Nullable X509Certificate clientCertificate;

    ClientTlsContextImpl(X509Certificate proxyCertificate, @Nullable X509Certificate clientCertificate) {
        this.proxyCertificate = proxyCertificate;
        this.clientCertificate = clientCertificate;
    }

    @Override
    public X509Certificate proxyServerCertificate() {
        return proxyCertificate;
    }

    @Override
    public Optional<X509Certificate> clientCertificate() {
        return Optional.ofNullable(clientCertificate);
    }
}
