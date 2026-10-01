/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for authenticating to HashiCorp Vault using the Kubernetes auth method.
 *
 * <p>When Kroxylicious runs as a pod in Kubernetes, the kubelet mounts a projected
 * {@code ServiceAccount} JWT token at a well-known path. This config directs Vault to
 * validate that token via the Kubernetes {@code TokenReview} API and exchange it for a
 * short-lived Vault client token.
 *
 * @param role                  the Vault role bound to the pod's {@code ServiceAccount}; required.
 * @param serviceAccountTokenPath path to the projected ServiceAccount JWT token file;
 *                              defaults to {@code /var/run/secrets/kubernetes.io/serviceaccount/token}.
 * @param authPath              the Vault mount path of the Kubernetes auth engine;
 *                              defaults to {@code kubernetes}.
 */
public record KubernetesCredentialsConfig(
                                          @JsonProperty(value = "role", required = true) String role,
                                          @JsonProperty(value = "serviceAccountTokenPath", required = false) String serviceAccountTokenPath,
                                          @JsonProperty(value = "authPath", required = false) String authPath) {

    /**
     * Default path at which kubelet mounts the projected ServiceAccount JWT token.
     */
    public static final String DEFAULT_SERVICE_ACCOUNT_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token";

    /**
     * Default Vault mount path for the Kubernetes auth engine.
     */
    public static final String DEFAULT_AUTH_PATH = "kubernetes";

    /**
     * Validates and applies defaults.
     */
    public KubernetesCredentialsConfig {
        Objects.requireNonNull(role, "role must not be null");
        if (serviceAccountTokenPath == null) {
            serviceAccountTokenPath = DEFAULT_SERVICE_ACCOUNT_TOKEN_PATH;
        }
        if (authPath == null) {
            authPath = DEFAULT_AUTH_PATH;
        }
    }
}
