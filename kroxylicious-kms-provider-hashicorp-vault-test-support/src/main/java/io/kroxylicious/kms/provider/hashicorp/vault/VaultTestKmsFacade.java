/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Objects;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.vault.VaultContainer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsService.Config;
import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse.ReadKeyData;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.TestKekManager;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.UnknownAliasException;

public class VaultTestKmsFacade implements TestKmsFacade<Config, String, VaultEdek> {
    private static final String VAULT_TOKEN = "rootToken";
    private static final String HASHICORP_VAULT = "hashicorp/vault:1.15";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String VAULT_CMD = "vault";

    @SuppressWarnings("rawtypes")
    private VaultContainer vaultContainer;

    @Override
    public boolean isAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    @Override
    @SuppressWarnings("resource")
    public void start() {

        vaultContainer = new VaultContainer<>(HASHICORP_VAULT)
                .withVaultToken(VAULT_TOKEN)
                .withEnv("VAULT_FORMAT", "json")
                .withInitCommand(
                        "secrets enable transit");
        vaultContainer.start();
    }

    @Override
    public void stop() {
        if (vaultContainer != null) {
            vaultContainer.close();
        }
    }

    @Override
    public TestKekManager getTestKekManager() {
        return new VaultTestKekManager();
    }

    @Override
    public Class<VaultKmsService> getKmsServiceClass() {
        return VaultKmsService.class;
    }

    @Override
    public Config getKmsServiceConfig() {
        return new Config(URI.create(vaultContainer.getHttpHostAddress()), VAULT_TOKEN, null);
    }

    private class VaultTestKekManager implements TestKekManager {
        @Override
        public void generateKek(String alias) {
            Objects.requireNonNull(alias);

            if (exists(alias)) {
                throw new AlreadyExistsException(alias);
            }
            else {
                create(alias);
            }
        }

        @Override
        public void rotateKek(String alias) {
            Objects.requireNonNull(alias);

            if (exists(alias)) {
                rotate(alias);
            }
            else {
                throw new UnknownAliasException(alias);
            }
        }

        @Override
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
            return runVaultCommand(new TypeReference<>() {
            }, VAULT_CMD, "write", "-f", "transit/keys/%s".formatted(keyId));
        }

        private ReadKeyData read(String keyId) {
            return runVaultCommand(new TypeReference<>() {
            }, VAULT_CMD, "read", "transit/keys/%s".formatted(keyId));
        }

        private ReadKeyData rotate(String keyId) {
            return runVaultCommand(new TypeReference<>() {
            }, VAULT_CMD, "write", "-f", "transit/keys/%s/rotate".formatted(keyId));
        }

        private <D> D runVaultCommand(TypeReference<VaultResponse<D>> valueTypeRef, String... args) {
            try {
                var execResult = vaultContainer.execInContainer(args);
                int exitCode = execResult.getExitCode();
                if (exitCode != 0) {
                    throw new VaultException(
                            "Failed to run vault command: %s, exit code: %d, stderr: %s".formatted(Arrays.stream(args).toList(), exitCode, execResult.getStderr()));
                }
                var response = OBJECT_MAPPER.readValue(execResult.getStdout(), valueTypeRef);
                return response.data();
            }
            catch (IOException e) {
                throw new VaultException("Failed to run vault command: %s".formatted(Arrays.stream(args).toList()), e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new VaultException(e);
            }
        }

        private static class VaultException extends KmsException {
            VaultException(String message) {
                super(message);
            }

            VaultException(String message, Throwable cause) {
                super(message, cause);
            }

            VaultException(Throwable cause) {
                super(cause);
            }
        }
    }
}
