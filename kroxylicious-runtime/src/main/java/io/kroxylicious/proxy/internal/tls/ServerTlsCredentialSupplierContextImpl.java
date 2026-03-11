/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Arrays;
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
    public TlsCredentials tlsCredentials(@NonNull PrivateKey key, @NonNull Certificate[] certificateChain) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(certificateChain, "certificateChain must not be null");
        if (certificateChain.length == 0) {
            throw new IllegalArgumentException("certificateChain must not be empty");
        }

        X509Certificate[] x509Chain = Arrays.stream(certificateChain)
                .map(cert -> {
                    if (!(cert instanceof X509Certificate)) {
                        throw new IllegalArgumentException("All certificates in chain must be X509Certificate instances");
                    }
                    return (X509Certificate) cert;
                })
                .toArray(X509Certificate[]::new);

        TlsUtil.validateCertificateChain(key, x509Chain);

        return new TlsCredentialsImpl(key, x509Chain);
    }
}
