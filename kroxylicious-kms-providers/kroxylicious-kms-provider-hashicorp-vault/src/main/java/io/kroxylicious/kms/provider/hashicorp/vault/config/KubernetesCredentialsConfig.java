/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for authenticating to HashiCorp Vault using the Kubernetes auth method.
 *
 * <p>When Kroxylicious runs as a pod in Kubernetes, the kubelet mounts a projected
 * {@code ServiceAccount} JWT token at a well-known path. This config directs Vault to
 * validate that token via the Kubernetes {@code TokenReview} API and exchange it for a
 * short-lived Vault client token.
 *
 * @param vaultRole               the Vault role bound to the pod's {@code ServiceAccount}; required.
 * @param serviceAccountTokenFile path to the projected ServiceAccount JWT token file;
 *                              defaults to {@code /var/run/secrets/kubernetes.io/serviceaccount/token}.
 * @param authPath              the Vault mount path of the Kubernetes auth engine;
 *                              defaults to {@code kubernetes}.
 */
public record KubernetesCredentialsConfig(
                                          @JsonProperty(value = "vaultRole", required = true) String vaultRole,
                                          @JsonProperty(value = "serviceAccountTokenFile", required = false) @Nullable String serviceAccountTokenFile,
                                          @JsonProperty(value = "authPath", required = false) @Nullable String authPath) {

    /**
     * Default path at which kubelet mounts the projected ServiceAccount JWT token.
     */
    public static final String DEFAULT_SERVICE_ACCOUNT_TOKEN_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/token";

    /**
     * Default Vault mount path for the Kubernetes auth engine.
     */
    public static final String DEFAULT_AUTH_PATH = "kubernetes";

    /**
     * Validates required configuration.
     */
    public KubernetesCredentialsConfig {
        Objects.requireNonNull(vaultRole, "vaultRole must not be null");
    }

    /**
     * Gets the effective service account token file path, returning {@link #DEFAULT_SERVICE_ACCOUNT_TOKEN_FILE} if null.
     *
     * @return the effective service account token file path
     */
    public String effectiveServiceAccountTokenFile() {
        return serviceAccountTokenFile != null ? serviceAccountTokenFile : DEFAULT_SERVICE_ACCOUNT_TOKEN_FILE;
    }

    /**
     * Gets the effective auth mount path, returning {@link #DEFAULT_AUTH_PATH} if null.
     *
     * @return the effective auth path
     */
    public String effectiveAuthPath() {
        return authPath != null ? authPath : DEFAULT_AUTH_PATH;
    }
}
