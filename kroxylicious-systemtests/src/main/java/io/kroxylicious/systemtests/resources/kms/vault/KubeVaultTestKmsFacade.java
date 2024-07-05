/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.kms.vault;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.hashicorp.vault.AbstractVaultTestKmsFacade;
import io.kroxylicious.kms.provider.hashicorp.vault.VaultTestKmsFacade;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.installation.kms.vault.Vault;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.utils.VersionComparator;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;
import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * KMS Facade for Vault running inside Kube.
 * Uses command line interaction so to avoid the complication of exposing the Vault endpoint
 * to the test outside the cluster.
 */
public class KubeVaultTestKmsFacade extends AbstractVaultTestKmsFacade {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String VAULT_CMD = "vault";
    private static final String LOGIN = "login";
    private static final String POLICY = "policy";
    private static final String SECRETS = "secrets";
    private static final String READ = "read";
    private static final String WRITE = "write";
    private static final String DELETE = "delete";
    private static final String KEYS_PATH = "transit/keys/%s";
    private final String namespace;
    private final String podName;
    private final Vault vault;

    /**
     * Instantiates a new Kube vault test kms facade.
     *
     */
    public KubeVaultTestKmsFacade() {
        this.namespace = Vault.VAULT_DEFAULT_NAMESPACE;
        this.podName = Vault.VAULT_POD_NAME;
        this.vault = new Vault(VAULT_ROOT_TOKEN);
    }

    @Override
    public void startVault() {
        vault.deploy();
        if (!isCorrectVersionInstalled()) {
            throw new KubeClusterException("Vault version installed " + getVaultVersion() + " does not match with the expected: '"
                    + VaultTestKmsFacade.HASHICORP_VAULT.getVersionPart() + "'");
        }
        runVaultCommand(VAULT_CMD, LOGIN, VAULT_ROOT_TOKEN);
    }

    @Override
    public void stopVault() {
        try {
            vault.delete();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to delete Vault", e);
        }
    }

    @Override
    protected void enableTransit() {
        runVaultCommand(VAULT_CMD, SECRETS, "enable", "transit");
    }

    @Override
    @SuppressWarnings("java:S4087") // explict close is required when using redirecting input
    protected void createPolicy(String policyName, InputStream policyStream) {
        try (var exec = kubeClient().getClient().pods().inNamespace(namespace).withName(podName)
                .redirectingInput()
                .terminateOnError()
                .exec(VAULT_CMD, POLICY, WRITE, policyName, "-")) {

            try (OutputStream input = exec.getInput()) {
                policyStream.transferTo(input);
            }
            exec.close(); // required when using redirecting input

            exec.exitCode().join();
            // https://github.com/kubernetes/kubernetes/issues/89899 exit code unavailable when stdin used, use presence of stderr instead
            if (exec.getError() != null) {
                var stderr = new String(exec.getError().readAllBytes());
                if (!stderr.isEmpty()) {
                    throw new KubeClusterException("Failed to install policy stderr %s".formatted(stderr));
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to install policy", e);
        }
    }

    @Override
    protected String createOrphanToken(String description, boolean noDefaultPolicy, Set<String> policies) {
        Map<String, Object> tokenCreate = runVaultCommand(new TypeReference<>() {
        }, VAULT_CMD, "token", "create", "-display-name", description, "-no-default-policy", "-policy=" + String.join(",", policies),
                "-orphan");
        return Optional.ofNullable(tokenCreate)
                .map(m -> m.get("auth")).map(Map.class::cast)
                .map(m -> m.get("client_token")).map(String.class::cast)
                .orElseThrow(() -> new IllegalArgumentException("unable to find client_token"));
    }

    @NonNull
    @Override
    protected URI getVaultUrl() {
        return URI.create("http://" + vault.getVaultUrl());
    }

    /**
     * Gets vault version.
     *
     * @return the vault version
     */
    public String getVaultVersion() {
        return vault.getVersionInstalled();
    }

    @Override
    public TestKekManager getTestKekManager() {
        return new VaultTestKekManager();
    }

    private boolean isCorrectVersionInstalled() {
        String installedVersion = getVaultVersion();
        String expectedVersion = VaultTestKmsFacade.HASHICORP_VAULT.getVersionPart();

        VersionComparator comparator = new VersionComparator(installedVersion);
        return comparator.compareTo(expectedVersion) == 0;
    }

    private class VaultTestKekManager implements TestKekManager {

        public void generateKek(String alias) {
            Objects.requireNonNull(alias);
            create(alias);
        }

        @Override
        public void deleteKek(String alias) {
            if (exists(alias)) {
                delete(alias);
            }
            else {
                throw new UnknownAliasException(alias);
            }
        }

        public void rotateKek(String alias) {
            Objects.requireNonNull(alias);

            if (exists(alias)) {
                rotate(alias);
            }
            else {
                throw new UnknownAliasException(alias);
            }
        }

        public boolean exists(String alias) {
            try {
                read(alias);
                return true;
            }
            catch (RuntimeException e) {
                if (isNoValueFound(e)) {
                    return false;
                }
                else {
                    throw e;
                }
            }
        }

        private boolean isNoValueFound(Exception e) {
            return e.getMessage().contains("No value found");
        }

        private void create(String keyId) {
            runVaultCommand(new TypeReference<>() {
            }, VAULT_CMD, WRITE, "-f", KEYS_PATH.formatted(keyId));
        }

        private void read(String keyId) {
            runVaultCommand(new TypeReference<>() {
            }, VAULT_CMD, READ, KEYS_PATH.formatted(keyId));
        }

        private void rotate(String keyId) {
            runVaultCommand(new TypeReference<>() {
            }, VAULT_CMD, WRITE, "-f", (KEYS_PATH + "/rotate").formatted(keyId));
        }

        private void delete(String keyId) {
            runVaultCommand(new TypeReference<>() {
            }, VAULT_CMD, WRITE, "-f", (KEYS_PATH + "/config").formatted(keyId), "deletion_allowed=true");

            runVaultCommand(VAULT_CMD, DELETE, KEYS_PATH.formatted(keyId));
        }
    }

    private <T> T runVaultCommand(TypeReference<T> valueTypeRef, String... command) {
        try {
            var execResult = runVaultCommand(command);
            return OBJECT_MAPPER.readValue(execResult.out(), valueTypeRef);
        }
        catch (IOException e) {
            throw new KubeClusterException("Failed to run vault command: %s".formatted(List.of(command)), e);
        }
    }

    private ExecResult runVaultCommand(String... command) {
        var execResult = cmdKubeClient(namespace).execInPod(podName, true, command);
        if (!execResult.isSuccess()) {
            throw new KubeClusterException("Failed to run vault command: %s, exit code: %d, stderr: %s".formatted(List.of(command),
                    execResult.returnCode(), execResult.err()));
        }
        return execResult;
    }
}
