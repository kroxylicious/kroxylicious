/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Objects;

import io.kroxylicious.proxy.tls.TlsCredentials;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Runtime implementation of TlsCredentials containing the actual private key and certificate chain.
 */
public class TlsCredentialsImpl implements TlsCredentials {
    private final PrivateKey privateKey;
    private final X509Certificate[] certificateChain;

    public TlsCredentialsImpl(@NonNull PrivateKey privateKey, @NonNull X509Certificate[] certificateChain) {
        this.privateKey = Objects.requireNonNull(privateKey, "privateKey must not be null");
        this.certificateChain = Objects.requireNonNull(certificateChain, "certificateChain must not be null");
        if (certificateChain.length == 0) {
            throw new IllegalArgumentException("certificateChain must not be empty");
        }
    }

    @NonNull
    public PrivateKey getPrivateKey() {
        return privateKey;
    }

    @NonNull
    public X509Certificate[] getCertificateChain() {
        return certificateChain;
    }

    @Override
    public String toString() {
        return "TlsCredentialsImpl{" +
                "certificateChain=" + (certificateChain != null ? certificateChain.length + " certificates" : "null") +
                ", privateKey=" + (privateKey != null ? privateKey.getAlgorithm() : "null") +
                '}';
    }
}
