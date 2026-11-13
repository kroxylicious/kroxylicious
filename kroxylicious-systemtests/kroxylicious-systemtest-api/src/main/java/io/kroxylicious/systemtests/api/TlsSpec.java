/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api;

import java.util.Objects;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Deployment-agnostic description of TLS configuration.
 * The fixture translates this into whatever form the deployment mechanism requires
 * (CRD {@code Tls} object, config file entry, etc.).
 *
 * @param certificateSecretName the name of the Kubernetes Secret (or file path in standalone mode) containing the TLS certificate and key
 * @param trustStoreName        the name of the trust store, or {@code null} if the default trust store is used
 */
public record TlsSpec(@NonNull String certificateSecretName, @Nullable String trustStoreName) {

    public TlsSpec {
        Objects.requireNonNull(certificateSecretName, "certificateSecretName should not be null");
    }

    /**
     * Creates a TLS spec with only a certificate secret and the default trust store.
     *
     * @param certificateSecretName the certificate secret name
     * @return a new TLS spec
     */
    public static TlsSpec of(@NonNull String certificateSecretName) {
        return new TlsSpec(certificateSecretName, null);
    }
}