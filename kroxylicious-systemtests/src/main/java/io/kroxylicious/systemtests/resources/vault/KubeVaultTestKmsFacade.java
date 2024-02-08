/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.vault;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Objects;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.systemtests.executor.ExecResult;
import io.kroxylicious.systemtests.installation.vault.Vault;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;
import io.kroxylicious.systemtests.resources.vault.VaultResponse.ReadKeyData;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;

public class KubeVaultTestKmsFacade implements TestKmsFacade<Config, String, VaultEdek> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String VAULT_CMD = "vault";
    private static final String WRITE = "write";
    private final String namespace;
    private final String podName;
    private final Vault vault;
    private String kroxyliciousToken;

    public KubeVaultTestKmsFacade(String namespace, String podName) {
        this.namespace = namespace;
        this.podName = podName;
        this.vault = new Vault(namespace);
    }

    @Override
    public boolean isAvailable() {
        return vault.isAvailable();
    }

    @Override
    public void start() {
        vault.deploy();

        runVaultCommand(VAULT_CMD, "policy", WRITE, "kroxylicious_encryption_filter_policy",
                "/etc/kroxylicious-encryption-filter-policy/kroxylicious_encryption_filter_policy.hcl");

        var tokenCreate = runVaultCommand(new TypeReference<TokenResponse>() {
        }, VAULT_CMD, "token", "create", "-display-name", "kroxylicious_encryption_filter", "-no-default-policy", "-policy=kroxylicious_encryption_filter_policy",
                "-orphan");
        kroxyliciousToken = tokenCreate.auth().clientToken();
    }

    @Override
    public void stop() {
        try {
            vault.delete();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to delete Vault", e);
        }
    }

    @Override
    public TestKekManager getTestKekManager() {
        return new VaultTestKekManager();
    }

    @Override
    public Class<? extends KmsService<Config, String, VaultEdek>> getKmsServiceClass() {
        return null;
    }

    @Override
    public Config getKmsServiceConfig() {
        return new Config(URI.create("http://" + vault.getVaultUrl()).resolve("v1/transit"), kroxyliciousToken, null);
    }

    private <T> T runVaultCommand(TypeReference<T> valueTypeRef, String... command) {
        try {
            var execResult = runVaultCommand(command);
            return OBJECT_MAPPER.readValue(execResult.out(), valueTypeRef);
        }
        catch (IOException e) {
            throw new KubeClusterException("Failed to run vault command: %s".formatted(Arrays.stream(command).toList()), e);
        }
    }

    private ExecResult runVaultCommand(String... command) {
        var execResult = cmdKubeClient(namespace).execInPod(podName, true, command);
        if (!execResult.isSuccess()) {
            throw new KubeClusterException("Failed to run vault command: %s, exit code: %d, stderr: %s".formatted(Arrays.stream(command).toList(),
                    execResult.returnCode(), execResult.err()));
        }
        return execResult;
    }

    private class VaultTestKekManager implements TestKekManager {

        public void generateKek(String alias) {
            Objects.requireNonNull(alias);
            create(alias);
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

        private ReadKeyData create(String keyId) {
            return runVaultCommand(new TypeReference<VaultResponse<ReadKeyData>>() {
            }, VAULT_CMD, "write", "-f", "transit/keys/%s".formatted(keyId)).data();
        }

        private ReadKeyData read(String keyId) {
            return runVaultCommand(new TypeReference<VaultResponse<ReadKeyData>>() {
            }, VAULT_CMD, "read", "transit/keys/%s".formatted(keyId)).data();
        }

        private ReadKeyData rotate(String keyId) {
            return runVaultCommand(new TypeReference<VaultResponse<ReadKeyData>>() {
            }, VAULT_CMD, "write", "-f", "transit/keys/%s/rotate".formatted(keyId)).data();
        }
    }
}
