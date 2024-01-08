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
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.vault.VaultContainer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsService.Config;
import io.kroxylicious.kms.provider.hashicorp.vault.VaultResponse.ReadKeyData;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;

import static io.kroxylicious.kms.provider.hashicorp.vault.Metrics.ATTEMPT_NAME;
import static io.kroxylicious.kms.provider.hashicorp.vault.Metrics.CREATE_DATAKEY_OPERATION;
import static io.kroxylicious.kms.provider.hashicorp.vault.Metrics.DECRYPT_EDEK_OPERATION;
import static io.kroxylicious.kms.provider.hashicorp.vault.Metrics.EMPTY_FAILURE;
import static io.kroxylicious.kms.provider.hashicorp.vault.Metrics.NOT_FOUND_FAILURE;
import static io.kroxylicious.kms.provider.hashicorp.vault.Metrics.OUTCOME_FAILURE;
import static io.kroxylicious.kms.provider.hashicorp.vault.Metrics.OUTCOME_NAME;
import static io.kroxylicious.kms.provider.hashicorp.vault.Metrics.OUTCOME_SUCCESS;
import static io.kroxylicious.kms.provider.hashicorp.vault.Metrics.RESOLVE_ALIAS_OPERATION;
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

    private static MeterRegistry meterRegistry;

    @BeforeEach
    @SuppressWarnings("resource")
    void beforeEach() {
        meterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(meterRegistry);
        // we need to recreate the static counters as they are cleared from the global registry afterEach test
        // the old Counter references continue to function but are not connected to the new meterRegistry
        io.kroxylicious.kms.provider.hashicorp.vault.Metrics.initialize();
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
        meterRegistry.clear();
        Metrics.globalRegistry.clear();
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
        assertCounterHasValue(OUTCOME_NAME, List.of(RESOLVE_ALIAS_OPERATION, OUTCOME_SUCCESS, EMPTY_FAILURE), 1);
        assertCounterHasValue(ATTEMPT_NAME, List.of(RESOLVE_ALIAS_OPERATION), 1);
    }

    @Test
    void resolveWithUnknownKey() {
        var keyName = "unknown";
        var resolved = service.resolveAlias(keyName);
        assertThat(resolved)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownAliasException.class);
        assertCounterHasValue(OUTCOME_NAME, List.of(RESOLVE_ALIAS_OPERATION, OUTCOME_FAILURE, NOT_FOUND_FAILURE), 1);
        assertCounterHasValue(ATTEMPT_NAME, List.of(RESOLVE_ALIAS_OPERATION), 1);
    }

    @Test
    void generatedEncryptedDekDecryptsBackToPlain() {
        String key = "mykey";
        createKek(key);

        var pairStage = service.generateDekPair(key);
        assertThat(pairStage).succeedsWithin(Duration.ofSeconds(5));
        var pair = pairStage.toCompletableFuture().join();

        assertCounterHasValue(OUTCOME_NAME, List.of(CREATE_DATAKEY_OPERATION, OUTCOME_SUCCESS, EMPTY_FAILURE), 1);
        assertCounterHasValue(ATTEMPT_NAME, List.of(CREATE_DATAKEY_OPERATION), 1);

        var decryptedDekStage = service.decryptEdek(pair.edek());
        assertThat(decryptedDekStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .isEqualTo(pair.dek());

        assertCounterHasValue(OUTCOME_NAME, List.of(DECRYPT_EDEK_OPERATION, OUTCOME_SUCCESS, EMPTY_FAILURE), 1);
        assertCounterHasValue(ATTEMPT_NAME, List.of(DECRYPT_EDEK_OPERATION), 1);
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

        assertCounterHasValue(OUTCOME_NAME, List.of(CREATE_DATAKEY_OPERATION, OUTCOME_FAILURE, NOT_FOUND_FAILURE), 1);
        assertCounterHasValue(ATTEMPT_NAME, List.of(CREATE_DATAKEY_OPERATION), 1);
    }

    @Test
    void decryptEdekWithUnknownKey() {
        var secretKeyStage = service.decryptEdek(new VaultEdek("unknown", new byte[]{ 1 }));
        assertThat(secretKeyStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownKeyException.class);

        assertCounterHasValue(OUTCOME_NAME, List.of(DECRYPT_EDEK_OPERATION, OUTCOME_FAILURE, NOT_FOUND_FAILURE), 1);
        assertCounterHasValue(ATTEMPT_NAME, List.of(DECRYPT_EDEK_OPERATION), 1);
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

    private void assertCounterHasValue(String name, List<Tag> tags, int expected) {
        var counter = meterRegistry.find(name).tags(tags).counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(expected);
    }

}
