/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.filter.encryption.FixedDekKmsService;
import io.kroxylicious.filter.encryption.config.EncryptionConfigurationException;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryEdek;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.UnitTestingKmsService;
import io.kroxylicious.kms.service.Serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DekManagerTest {

    private UnitTestingKmsService unitTestingKmsService;

    @BeforeEach
    void beforeEach() {
        unitTestingKmsService = UnitTestingKmsService.newInstance();
    }

    @AfterEach
    public void afterEach() {
        Optional.ofNullable(unitTestingKmsService).ifPresent(UnitTestingKmsService::close);
    }

    @Test
    void testResolveAlias() {
        // Given
        var config = new UnitTestingKmsService.Config(12, 96, List.of());
        unitTestingKmsService.initialize(config);
        var kms = unitTestingKmsService.buildKms();
        var kekId = kms.generateKey();
        kms.createAlias(kekId, "foo");
        var dm = new DekManager<>(kms, 1);

        // When
        var resolvedKekId = dm.resolveAlias("foo").toCompletableFuture().join();

        // Then
        assertThat(resolvedKekId).isEqualTo(kekId);
    }

    @ParameterizedTest
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void testLimitsNumbersOfEncryptors(CipherManager cipherManager) {
        // Given
        var config = new UnitTestingKmsService.Config(12, 96, List.of());
        unitTestingKmsService.initialize(config);
        var kms = unitTestingKmsService.buildKms();
        var kekId = kms.generateKey();
        kms.createAlias(kekId, "foo");
        var dm = new DekManager<>(kms, 1);

        // When
        var dek = dm.generateDek(kekId, cipherManager).toCompletableFuture().join();

        // Then
        assertThatThrownBy(() -> dek.encryptor(2)).isExactlyInstanceOf(ExhaustedDekException.class);
    }

    @ParameterizedTest
    @MethodSource("io.kroxylicious.filter.encryption.dek.CipherManagerTest#allCipherManagers")
    void testDecryptedEdekIsGoodForDecryptingData(CipherManager cipherManager) {
        // Given
        var config = new UnitTestingKmsService.Config(12, 96, List.of());
        unitTestingKmsService.initialize(config);
        var kms = unitTestingKmsService.buildKms();
        var kekId = kms.generateKey();
        kms.createAlias(kekId, "foo");
        var dm = new DekManager<>(kms, 1_000);

        // Generate a DEK anduse it to encrypt
        var dek = dm.generateDek(kekId, cipherManager).toCompletableFuture().join();
        ByteBuffer plaintext = ByteBuffer.wrap("hello, world".getBytes(StandardCharsets.UTF_8));
        ByteBuffer aad = null;
        ByteBuffer[] ciphertext = new ByteBuffer[1];
        ByteBuffer[] params = new ByteBuffer[1];
        InMemoryEdek edek;
        try (Dek<InMemoryEdek>.Encryptor encryptor = dek.encryptor(1)) {
            encryptor.generateParameters(size -> params[0] = ByteBuffer.allocate(size));
            encryptor.encrypt(plaintext,
                    aad,
                    size -> ciphertext[0] = ByteBuffer.allocate(size));
            edek = encryptor.edek();
        }
        var edekBuffer = ByteBuffer.allocate(1024);
        Serde<InMemoryEdek> inMemoryEdekSerde = dm.edekSerde();
        inMemoryEdekSerde.serialize(edek, edekBuffer);
        edekBuffer.flip();

        // When
        var decrypted = dm.decryptEdek(inMemoryEdekSerde.deserialize(edekBuffer), cipherManager).toCompletableFuture().join();

        // Then
        assertThatThrownBy(() -> decrypted.encryptor(1)).isExactlyInstanceOf(DestroyedDekException.class);
        var decodedPlaintext = ByteBuffer.allocate(plaintext.capacity());
        decrypted.decryptor().decrypt(ciphertext[0], aad, params[0], decodedPlaintext);
        assertThat(new String(decodedPlaintext.array(), StandardCharsets.UTF_8)).isEqualTo("hello, world");
    }

    @Test
    void aes256KeyMustBe256bits() {
        try (var fixedDekKmsService = new FixedDekKmsService(128)) {
            fixedDekKmsService.initialize(null);
            var kms = fixedDekKmsService.buildKms();
            DekManager<ByteBuffer, ByteBuffer> manager = new DekManager<>(kms, 10000);
            var dekCompletionStage = manager.generateDek(fixedDekKmsService.getKekId(), Aes.AES_256_GCM_128);
            assertThat(dekCompletionStage).failsWithin(10, TimeUnit.SECONDS)
                    .withThrowableOfType(ExecutionException.class)
                    .havingCause()
                    .isExactlyInstanceOf(EncryptionConfigurationException.class)
                    .withMessage("KMS returned 128-bit DEK but AES_256_GCM_128 requires keys of 256 bits");
        }

    }

}
