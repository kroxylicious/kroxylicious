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
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleRef;
import io.fabric8.kubernetes.api.model.rbac.RoleRefBuilder;
import io.fabric8.kubernetes.api.model.rbac.Subject;
import io.fabric8.kubernetes.api.model.rbac.SubjectBuilder;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.kms.provider.hashicorp.vault.config.KubernetesCredentialsConfig;
import io.kroxylicious.kms.provider.hashicorp.vault.config.VaultCredentialsConfig;
import io.kroxylicious.systemtests.installation.kms.vault.Vault;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static io.kroxylicious.testing.kms.vault.AbstractVaultTestKmsFacade.VAULT_ROOT_TOKEN;

/**
 * KMS Facade for Vault running inside Kubernetes, using the Vault Kubernetes
 * auth method.
 *
 * <p>
 * In addition to deploying Vault (inherited from
 * {@link KubeVaultTestKmsFacade}), this facade:
 * <ol>
 * <li>Creates a {@code ClusterRoleBinding} granting
 * {@code system:auth-delegator} to the
 * Vault ServiceAccount so Vault can validate Kubernetes ServiceAccount tokens
 * via the
 * {@code TokenReview} API.</li>
 * <li>Enables the Kubernetes auth engine at
 * {@code /v1/sys/auth/kubernetes}.</li>
 * <li>Configures the Kubernetes auth engine with the in-cluster API server
 * address.</li>
 * <li>Registers a Vault role ({@code kroxylicious-vault-role}) bound to the
 * {@code default} ServiceAccount in the {@code kroxylicious} namespace with the
 * {@code kroxylicious_encryption_filter_policy} policy.</li>
 * </ol>
 *
 * <p>
 * The KMS config returned by {@link #getKmsServiceConfig()} uses Kubernetes
 * auth
 * ({@code role = "kroxylicious-vault-role"}) rather than a static token.
 */
public class KubeVaultKubernetesAuthTestKmsFacade extends KubeVaultTestKmsFacade {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubeVaultKubernetesAuthTestKmsFacade.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * The Vault role name bound to the Kroxylicious proxy ServiceAccount.
     */
    public static final String VAULT_ROLE_NAME = "kroxylicious-vault-role";

    /**
     * The Kubernetes namespace where the Kroxylicious proxy runs.
     */
    public static final String KROXYLICIOUS_NAMESPACE = "kroxylicious";

    /**
     * The ServiceAccount name used by the Kroxylicious proxy pod.
     */
    public static final String KROXYLICIOUS_SERVICE_ACCOUNT = "default";

    /**
     * The ClusterRoleBinding name granting system:auth-delegator to the Vault
     * ServiceAccount.
     */
    private static final String VAULT_AUTH_DELEGATOR_CRB_NAME = "vault-auth-delegator";

    /**
     * The policy name that grants the Kroxylicious proxy access to the Transit
     * engine.
     */
    private static final String POLICY_NAME = "kroxylicious_encryption_filter_policy";

    /**
     * Constructs a new KubeVaultKubernetesAuthTestKmsFacade.
     */
    public KubeVaultKubernetesAuthTestKmsFacade() {
        super();
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Extends the base deployment by additionally:
     * <ul>
     * <li>Binding the Vault ServiceAccount to {@code system:auth-delegator}</li>
     * <li>Enabling and configuring the Kubernetes auth engine in Vault</li>
     * <li>Registering the Kroxylicious Vault role</li>
     * </ul>
     */
    @Override
    public void startVault() {
        super.startVault();
        createAuthDelegatorClusterRoleBinding();
        enableKubernetesAuth();
        configureKubernetesAuth();
        createKroxyliciousVaultRole();
    }

    /**
     * Creates a {@code ClusterRoleBinding} that grants the
     * {@code system:auth-delegator}
     * ClusterRole to the Vault ServiceAccount, enabling Vault to use the Kubernetes
     * {@code TokenReview} API to validate ServiceAccount JWTs.
     */
    private void createAuthDelegatorClusterRoleBinding() {
        LOGGER.info("Creating ClusterRoleBinding {} for system:auth-delegator", VAULT_AUTH_DELEGATOR_CRB_NAME);

        Subject vaultSubject = new SubjectBuilder()
                .withKind("ServiceAccount")
                .withName("vault")
                .withNamespace(Vault.VAULT_DEFAULT_NAMESPACE)
                .build();

        RoleRef roleRef = new RoleRefBuilder()
                .withApiGroup("rbac.authorization.k8s.io")
                .withKind("ClusterRole")
                .withName("system:auth-delegator")
                .build();

        ClusterRoleBinding crb = new ClusterRoleBindingBuilder()
                .withNewMetadata()
                .withName(VAULT_AUTH_DELEGATOR_CRB_NAME)
                .endMetadata()
                .withSubjects(List.of(vaultSubject))
                .withRoleRef(roleRef)
                .build();

        kubeClient().getClient()
                .rbac()
                .clusterRoleBindings()
                .resource(crb)
                .serverSideApply();

        LOGGER.info("ClusterRoleBinding {} created successfully", VAULT_AUTH_DELEGATOR_CRB_NAME);
    }

    /**
     * Enables the Kubernetes auth engine at the {@code kubernetes} mount path in
     * Vault.
     */
    private void enableKubernetesAuth() {
        LOGGER.info("Enabling Vault Kubernetes auth engine");
        var body = encodeJson(Map.of("type", "kubernetes"));
        var request = createRootVaultPost(getVaultUrl().resolve("v1/sys/auth/kubernetes"), body);
        sendVaultRequest(request, "enable Kubernetes auth engine");
    }

    /**
     * Configures the Kubernetes auth engine with the in-cluster Kubernetes API
     * server address.
     */
    private void configureKubernetesAuth() {
        LOGGER.info("Configuring Vault Kubernetes auth engine with kubernetes_host=https://kubernetes.default.svc:443");
        var body = encodeJson(Map.of("kubernetes_host", "https://kubernetes.default.svc:443"));
        var request = createRootVaultPost(getVaultUrl().resolve("v1/auth/kubernetes/config"), body);
        sendVaultRequest(request, "configure Kubernetes auth engine");
    }

    /**
     * Creates the Vault role that the Kroxylicious proxy pod uses to authenticate.
     *
     * <p>
     * The role is bound to the {@code default} ServiceAccount in the
     * {@code kroxylicious}
     * namespace and grants the {@code kroxylicious_encryption_filter_policy}
     * policy.
     */
    private void createKroxyliciousVaultRole() {
        LOGGER.info("Creating Vault role {} bound to ServiceAccount {}/{}", VAULT_ROLE_NAME, KROXYLICIOUS_NAMESPACE,
                KROXYLICIOUS_SERVICE_ACCOUNT);
        var body = encodeJson(Map.of(
                "bound_service_account_names", List.of(KROXYLICIOUS_SERVICE_ACCOUNT),
                "bound_service_account_namespaces", List.of(KROXYLICIOUS_NAMESPACE),
                "policies", List.of(POLICY_NAME),
                "ttl", "1h"));
        var request = createRootVaultPost(
                getVaultUrl().resolve("v1/auth/kubernetes/role/" + VAULT_ROLE_NAME),
                body);
        sendVaultRequest(request, "create Vault role " + VAULT_ROLE_NAME);
        LOGGER.info("Vault role {} created successfully", VAULT_ROLE_NAME);
    }

    /**
     * Builds a POST {@link HttpRequest} authenticated with the Vault root token.
     *
     * @param uri  the Vault API URI
     * @param body JSON request body
     * @return the built request
     */
    private HttpRequest createRootVaultPost(java.net.URI uri, String body) {
        return HttpRequest.newBuilder()
                .uri(uri)
                .header("X-Vault-Token", VAULT_ROOT_TOKEN)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
    }

    private static final int MAX_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 5000;

    /**
     * Sends the request with retry logic and logs the outcome. Throws on non-2xx
     * responses.
     *
     * <p>
     * Retries are necessary because Vault may not be ready to accept connections
     * immediately
     * after the Helm deploy completes — the pod needs time to start and initialise.
     *
     * @param request     the HTTP request to send
     * @param description human-readable description for logging
     */
    private void sendVaultRequest(HttpRequest request, String description) {
        HttpClient client = HttpClient.newHttpClient();
        int remaining = MAX_ATTEMPTS;
        RuntimeException lastException = null;
        while (remaining > 0) {
            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                int status = response.statusCode();
                if (status >= 200 && status < 300) {
                    LOGGER.debug("Vault request '{}' succeeded with status {}", description, status);
                    return;
                } else {
                    throw new IllegalStateException(
                            "Vault request '%s' failed with status %d: %s".formatted(description, status,
                                    response.body()));
                }
            } catch (IOException e) {
                remaining--;
                lastException = new UncheckedIOException("IO error during Vault request: " + description, e);
                LOGGER.warn("Vault request '{}' failed (remaining {}/{} attempts)", description, remaining,
                        MAX_ATTEMPTS, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted during Vault request: " + description, e);
            }
            if (remaining > 0) {
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted whilst retrying Vault request: " + description, ie);
                }
            }
        }
        throw new IllegalStateException(
                "Vault request '%s' failed after %d attempts".formatted(description, MAX_ATTEMPTS), lastException);
    }

    /**
     * Serialises the given object to a JSON string.
     *
     * @param value the object to serialise
     * @return JSON string representation
     */
    private String encodeJson(Object value) {
        try {
            return OBJECT_MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to serialise request body", e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * Returns a {@link Config} that uses the Kubernetes auth method rather than a
     * static token.
     * The Kroxylicious proxy pod will authenticate to Vault using the projected
     * ServiceAccount JWT
     * mounted at {@code /var/run/secrets/kubernetes.io/serviceaccount/token}.
     */
    @Override
    public Config getKmsServiceConfig() {
        return new Config(
                getVaultTransitEngineUrl(),
                new VaultCredentialsConfig(null, new KubernetesCredentialsConfig(VAULT_ROLE_NAME, null, null)),
                null);
    }
}
