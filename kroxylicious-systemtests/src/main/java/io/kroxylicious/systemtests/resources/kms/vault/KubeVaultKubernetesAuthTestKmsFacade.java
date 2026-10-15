/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.vault;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultEdek;
import io.kroxylicious.kms.provider.hashicorp.vault.WrappingKey;
import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.kms.provider.hashicorp.vault.config.KubernetesCredentialsConfig;
import io.kroxylicious.kms.provider.hashicorp.vault.config.VaultCredentialsConfig;
import io.kroxylicious.systemtests.resources.kms.ServiceAccountAuthTestKmsFacade;

import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * KMS Facade for Vault running inside Kube, where the proxy authenticates with Vault using the
 * Kubernetes auth method (exchanging its pod ServiceAccount token for a Vault token) rather than a
 * static Vault token. The ServiceAccount the proxy runs as must be granted access via
 * {@link #bindToServiceAccount(String, String)} before the proxy is deployed.
 */
public class KubeVaultKubernetesAuthTestKmsFacade extends KubeVaultTestKmsFacade implements ServiceAccountAuthTestKmsFacade<Config, WrappingKey, VaultEdek> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final HttpClient VAULT_CLIENT = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

    /**
     * The Vault role the proxy authenticates as. Bound to a single ServiceAccount by
     * {@link #bindToServiceAccount(String, String)}.
     */
    private static final String ROLE = "kroxylicious-vault-role";

    /**
     * In-cluster address of the Kubernetes API, used by Vault to validate ServiceAccount tokens via the
     * TokenReview API.
     */
    private static final String KUBERNETES_HOST = "https://kubernetes.default.svc:443";

    /**
     * Instantiates a new Kube Vault Kubernetes auth test kms facade.
     */
    public KubeVaultKubernetesAuthTestKmsFacade() {
        // Intentionally empty
    }

    @Override
    public void startVault() {
        super.startVault();
        // Vault may not be ready to serve requests the instant its pod reports healthy, so reuse the
        // superclass's retry approach. All the calls involved are idempotent, so retrying is safe.
        withRetry(this::enableKubernetesAuth, 3);
    }

    private void enableKubernetesAuth() {
        // Enable the Kubernetes auth method, tolerating the case where a previous run already enabled it.
        var enableResponse = send(post("v1/sys/auth/kubernetes", encodeJson(Map.of("type", "kubernetes"))));
        boolean enabled = enableResponse.statusCode() == 204;
        boolean alreadyEnabled = enableResponse.statusCode() == 400 && enableResponse.body().contains("path is already in use");
        if (!enabled && !alreadyEnabled) {
            throw new IllegalStateException(
                    "Unexpected response %d enabling Kubernetes auth: %s".formatted(enableResponse.statusCode(), enableResponse.body()));
        }

        // Point the auth method at the in-cluster Kubernetes API. Vault runs in-cluster and uses its own pod
        // ServiceAccount token for the TokenReview call, so no additional configuration is required here.
        expectSuccess(send(post("v1/auth/kubernetes/config", encodeJson(Map.of("kubernetes_host", KUBERNETES_HOST)))));
    }

    @Override
    public void bindToServiceAccount(String namespace, String serviceAccountName) {
        // Create (or replace) the Vault role, binding it to exactly the given ServiceAccount so that only a
        // pod running as it can obtain a token carrying the encryption filter policy.
        var body = encodeJson(Map.of(
                "bound_service_account_names", List.of(serviceAccountName),
                "bound_service_account_namespaces", List.of(namespace),
                "token_policies", List.of(ENCRYPTION_FILTER_POLICY_NAME),
                "token_ttl", "1h"));
        expectSuccess(send(post("v1/auth/kubernetes/role/" + encode(ROLE, UTF_8), body)));
    }

    @Override
    public Config getKmsServiceConfig() {
        return new Config(getVaultUrl(), new VaultCredentialsConfig(null, new KubernetesCredentialsConfig(ROLE, null, null)), null);
    }

    private HttpRequest post(String relativePath, String jsonBody) {
        return HttpRequest.newBuilder()
                .uri(getVaultUrl().resolve(relativePath))
                .header("X-Vault-Token", VAULT_ROOT_TOKEN)
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
    }

    private static HttpResponse<String> send(HttpRequest request) {
        try {
            return VAULT_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Request to %s failed".formatted(request.uri()), e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during REST API call: %s".formatted(request.uri()), e);
        }
    }

    private static void expectSuccess(HttpResponse<String> response) {
        if (response.statusCode() != 200 && response.statusCode() != 204) {
            throw new IllegalStateException(
                    "Unexpected response %d from %s: %s".formatted(response.statusCode(), response.uri(), response.body()));
        }
    }

    private static String encodeJson(Object obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to encode the request body", e);
        }
    }
}
