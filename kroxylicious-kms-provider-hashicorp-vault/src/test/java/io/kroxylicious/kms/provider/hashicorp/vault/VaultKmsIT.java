/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.vault.VaultContainer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsService.Config;
import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse.ReadKeyData;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.UnknownKeyException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration tests for HashiCorp Vault.
 * <br/>
 * <h4>TODO</h4>
 * <ul>
 *    <li>test condition such as wrong kek type, expired kek</li>
 * </ul>
 */
class VaultKmsIT {

    private static final String VAULT_TOKEN = "token";

    private static final String HASHICORP_VAULT = "hashicorp/vault:1.15";
    @SuppressWarnings("rawtypes")
    private VaultContainer vaultContainer;
    private VaultKms service;

    @BeforeEach
    @SuppressWarnings("resource")
    void beforeEach() {
        assumeThat(DockerClientFactory.instance().isDockerAvailable()).withFailMessage("docker unavailable").isTrue();

        vaultContainer = new VaultContainer<>(HASHICORP_VAULT)
                .withVaultToken(VAULT_TOKEN)
                .withEnv("VAULT_FORMAT", "json")
                .withInitCommand(
                        "secrets enable transit");
        vaultContainer.start();
        var config = new Config(URI.create(vaultContainer.getHttpHostAddress()), VAULT_TOKEN);

        service = new VaultKmsService().buildKms(config);
    }

    @AfterEach
    void afterEach() {
        if (vaultContainer != null) {
            vaultContainer.close();
        }
    }

    @Test
    void resolveKeyByName() {
        var keyName = "mykey";
        createKek(keyName);
        var resolved = service.resolveAlias(keyName);
        assertThat(resolved)
                .succeedsWithin(Duration.ofSeconds(5))
                .isEqualTo(keyName);
    }

    @Test
    void resolveWithUnknownKey() {
        var keyName = "unknown";
        var resolved = service.resolveAlias(keyName);
        assertThat(resolved)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownKeyException.class);
    }

    @Test
    void generatedEncryptedDekDecryptsBackToPlain() {
        String key = "mykey";
        createKek(key);

        var pairStage = service.generateDekPair(key);
        assertThat(pairStage).succeedsWithin(Duration.ofSeconds(5));
        var pair = pairStage.toCompletableFuture().join();

        var decryptedDekStage = service.decryptEdek(pair.edek());
        assertThat(decryptedDekStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .isEqualTo(pair.dek());
    }

    @Test
    void decryptDekAfterRotate() {
        var key = "mykey";
        var data = createKek(key);
        var originalVersion = data.latestVersion();

        var pairStage = service.generateDekPair(key);
        assertThat(pairStage).succeedsWithin(Duration.ofSeconds(5));
        var pair = pairStage.toCompletableFuture().join();

        var updated = rotateKek(data.name());
        var versionAfterRotate = updated.latestVersion();
        assertThat(versionAfterRotate).isGreaterThan(originalVersion);

        var decryptedDekStage = service.decryptEdek(pair.edek());
        assertThat(decryptedDekStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .isEqualTo(pair.dek());
    }

    @Test
    void generatedDekPairWithUnknownKey() {
        var pairStage = service.generateDekPair("unknown");
        assertThat(pairStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownKeyException.class);
    }

    @Test
    void decryptEdekWithUnknownKey() {
        var secretKeyStage = service.decryptEdek(new VaultEdek("unknown", new byte[]{}));
        assertThat(secretKeyStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownKeyException.class);
    }

    @Test
    void edekSerdeRoundTrip() {
        var key = "mykey";
        createKek(key);

        var pairStage = service.generateDekPair(key);
        assertThat(pairStage).succeedsWithin(Duration.ofSeconds(5));
        var pair = pairStage.toCompletableFuture().join();
        assertThat(pair).extracting(DekPair::edek).isNotNull();

        var edek = pair.edek();
        var serde = service.edekSerde();
        var buf = ByteBuffer.allocate(serde.sizeOf(edek));
        serde.serialize(edek, buf);
        buf.flip();
        var output = serde.deserialize(buf);
        assertThat(output).isEqualTo(edek);
    }

    private ReadKeyData readKek(String keyId) {
        return runVaultCommand(new TypeReference<>() {
        }, "vault", "read", "transit/keys/%s".formatted(keyId));
    }

    private ReadKeyData createKek(String keyId) {
        return runVaultCommand(new TypeReference<>() {
        }, "vault", "write", "-f", "transit/keys/%s".formatted(keyId));
    }

    private ReadKeyData rotateKek(String keyId) {
        return runVaultCommand(new TypeReference<>() {
        }, "vault", "write", "-f", "transit/keys/%s/rotate".formatted(keyId));
    }

    private <D> D runVaultCommand(TypeReference<VaultResponse<D>> valueTypeRef, String... args) {
        try {
            var execResult = vaultContainer.execInContainer(args);
            int exitCode = execResult.getExitCode();
            assertThat(exitCode).isZero();
            var response = new ObjectMapper().readValue(execResult.getStdout(), valueTypeRef);
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

}
