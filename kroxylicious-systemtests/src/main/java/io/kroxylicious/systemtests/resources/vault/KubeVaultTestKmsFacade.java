/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.vault;

import java.io.IOException;
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

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.cmdKubeClient;

public class KubeVaultTestKmsFacade implements TestKmsFacade<Config, String, VaultEdek> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String VAULT_CMD = "vault";
    private static final String FORMAT_JSON = "-format=json";
    private final String namespace;
    private final String podName;
    private final Vault vault;

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
    }

    @Override
    public void stop() {
        try {
            vault.delete();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
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
        return new Config(URI.create(vault.getBootstrap()), Vault.VAULT_ROOT_TOKEN, null);
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

        private VaultResponse.ReadKeyData create(String keyId) {
            return runVaultCommand(new TypeReference<>() {
            }, VAULT_CMD, "write", "-f", FORMAT_JSON, "transit/keys/%s".formatted(keyId));
        }

        private VaultResponse.ReadKeyData read(String keyId) {
            return runVaultCommand(new TypeReference<>() {
            }, VAULT_CMD, "read", FORMAT_JSON, "transit/keys/%s".formatted(keyId));
        }

        private VaultResponse.ReadKeyData rotate(String keyId) {
            return runVaultCommand(new TypeReference<>() {
            }, VAULT_CMD, "write", "-f", FORMAT_JSON, "transit/keys/%s/rotate".formatted(keyId));
        }

        private <D> D runVaultCommand(TypeReference<VaultResponse<D>> valueTypeRef, String... command) {
            try {
                ExecResult execResult = cmdKubeClient(namespace).execInPod(podName, true, command);
                if (!execResult.isSuccess()) {
                    throw new KubeClusterException("Failed to run vault command: %s, exit code: %d, stderr: %s".formatted(Arrays.stream(command).toList(),
                            execResult.returnCode(), execResult.err()));
                }
                var response = OBJECT_MAPPER.readValue(execResult.out(), valueTypeRef);
                return response.data();
            }
            catch (IOException e) {
                throw new KubeClusterException("Failed to run vault command: %s; error: %s".formatted(Arrays.stream(command).toList(), e.getMessage()));
            }
        }
    }
}
