/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryEdek;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.UnitTestingKmsService;
import io.kroxylicious.kms.service.Serde;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DekManagerTest {

    @Test
    void testResolveAlias() {
        // Given
        UnitTestingKmsService unitTestingKmsService = UnitTestingKmsService.newInstance();
        UnitTestingKmsService.Config options = new UnitTestingKmsService.Config(12, 96);
        var kms = unitTestingKmsService.buildKms(options);
        var kekId = kms.generateKey();
        kms.createAlias(kekId, "foo");
        var dm = new DekManager<>(unitTestingKmsService, options, 1);

        // When
        var resolvedKekId = dm.resolveAlias("foo").toCompletableFuture().join();

        // Then
        assertThat(resolvedKekId).isEqualTo(kekId);
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void testLimitsNumbersOfEncryptors(CipherSpec cipherSpec) {
        // Given
        UnitTestingKmsService unitTestingKmsService = UnitTestingKmsService.newInstance();
        UnitTestingKmsService.Config options = new UnitTestingKmsService.Config(12, 96);
        var kms = unitTestingKmsService.buildKms(options);
        var kekId = kms.generateKey();
        kms.createAlias(kekId, "foo");
        var dm = new DekManager<>(unitTestingKmsService, options, 1);

        // When
        var dek = dm.generateDek(kekId, cipherSpec).toCompletableFuture().join();

        // Then
        assertThatThrownBy(() -> dek.encryptor(2)).isExactlyInstanceOf(ExhaustedDekException.class);
    }

    @ParameterizedTest
    @EnumSource(CipherSpec.class)
    void testDecryptedEdekIsGoodForDecryptingData(CipherSpec cipherSpec) {
        // Given
        UnitTestingKmsService unitTestingKmsService = UnitTestingKmsService.newInstance();
        UnitTestingKmsService.Config options = new UnitTestingKmsService.Config(12, 96);
        var kms = unitTestingKmsService.buildKms(options);
        var kekId = kms.generateKey();
        kms.createAlias(kekId, "foo");
        var dm = new DekManager<>(unitTestingKmsService, options, 1_000);

        // Generate a DEK anduse it to encrypt
        var dek = dm.generateDek(kekId, cipherSpec).toCompletableFuture().join();
        ByteBuffer plaintext = ByteBuffer.wrap("hello, world".getBytes(StandardCharsets.UTF_8));
        ByteBuffer aad = null;
        ByteBuffer[] ciphertext = new ByteBuffer[1];
        ByteBuffer[] params = new ByteBuffer[1];
        InMemoryEdek edek;
        try (Dek<InMemoryEdek>.Encryptor encryptor = dek.encryptor(1)) {
            encryptor.preEncrypt(size -> params[0] = ByteBuffer.allocate(size));
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
        var decrypted = dm.decryptEdek(inMemoryEdekSerde.deserialize(edekBuffer), cipherSpec).toCompletableFuture().join();

        // Then
        assertThatThrownBy(() -> decrypted.encryptor(1)).isExactlyInstanceOf(ExhaustedDekException.class);
        var decodedPlaintext = ByteBuffer.allocate(plaintext.capacity());
        decrypted.decryptor().decrypt(ciphertext[0], aad, params[0], decodedPlaintext);
        assertThat(new String(decodedPlaintext.array(), StandardCharsets.UTF_8)).isEqualTo("hello, world");
    }

}
