/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.encryption;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.vault.VaultContainer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsService;
import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse;
import io.kroxylicious.kms.service.KmsService;

import static org.assertj.core.api.Assertions.assertThat;

public class VaultKmsFacade implements TestKmsFacade {
    private static final String VAULT_TOKEN = "rootToken";
    private static final String HASHICORP_VAULT = "hashicorp/vault:1.15";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private VaultContainer vaultContainer;

    @Override
    public boolean isAvailable() {
        return DockerClientFactory.instance().isDockerAvailable();
    }

    @Override
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
        return new TestKekManager() {
            @Override
            public CompletionStage<Void> generateKek(String alias) {
                Objects.requireNonNull(alias);

                try {
                    create(alias);
                    return CompletableFuture.completedStage(null);
                }
                catch (Exception e) {
                    // differentiate exceptions
                    return CompletableFuture.failedStage(e);
                }
            }

            @Override
            public CompletionStage<Void> rotateKek(String alias) {
                Objects.requireNonNull(alias);
                try {
                    rotate(alias);
                    return CompletableFuture.completedStage(null);
                }
                catch (Exception e) {
                    // differentiate exceptions
                    return CompletableFuture.failedStage(e);
                }
            }

            private VaultResponse.ReadKeyData create(String keyId) {
                return runVaultCommand(new TypeReference<>() {
                }, "vault", "write", "-f", "transit/keys/%s".formatted(keyId));
            }

            private VaultResponse.ReadKeyData rotate(String keyId) {
                return runVaultCommand(new TypeReference<>() {
                }, "vault", "write", "-f", "transit/keys/%s/rotate".formatted(keyId));
            }

            private <D> D runVaultCommand(TypeReference<VaultResponse<D>> valueTypeRef, String... args) {
                try {
                    var execResult = vaultContainer.execInContainer(args);
                    int exitCode = execResult.getExitCode();
                    assertThat(exitCode).isZero();
                    var response = OBJECT_MAPPER.readValue(execResult.getStdout(), valueTypeRef);
                    return response.data();
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Failed to run vault command: %s".formatted(Arrays.stream(args).toList()), e);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }

        };
    }

    @Override
    public Class<? extends KmsService<?, ?, ?>> getKmsServiceClass() {
        return VaultKmsService.class;
    }

    @Override
    public Object getKmsServiceConfig() {
        return new VaultKmsService.Config(URI.create(vaultContainer.getHttpHostAddress()), VAULT_TOKEN);
    }
}
