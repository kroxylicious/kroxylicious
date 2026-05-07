/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Objects;
import java.util.Optional;

import io.kroxylicious.proxy.tls.ClientTlsContext;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplierContext;
import io.kroxylicious.proxy.tls.TlsCredentials;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Runtime implementation of ServerTlsCredentialSupplierContext.
 */
public class ServerTlsCredentialSupplierContextImpl implements ServerTlsCredentialSupplierContext {

    private final Optional<ClientTlsContext> clientTlsContext;

    public ServerTlsCredentialSupplierContextImpl(@Nullable ClientTlsContext clientTlsContext) {
        this.clientTlsContext = Optional.ofNullable(clientTlsContext);
    }

    @NonNull
    @Override
    public Optional<ClientTlsContext> clientTlsContext() {
        return clientTlsContext;
    }

    @NonNull
    @Override
    public TlsCredentials tlsCredentials(@NonNull PrivateKey key, @NonNull X509Certificate[] certificateChain) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(certificateChain, "certificateChain must not be null");
        if (certificateChain.length == 0) {
            throw new IllegalArgumentException("certificateChain must not be empty");
        }

        TlsUtil.validateCertificateChain(key, certificateChain);

        return new TlsCredentialsImpl(key, certificateChain);
    }
}
