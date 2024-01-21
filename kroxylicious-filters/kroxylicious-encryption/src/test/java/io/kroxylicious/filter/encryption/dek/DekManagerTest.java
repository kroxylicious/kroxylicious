/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import io.kroxylicious.filter.encryption.inband.ExhaustedDekException;

import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryEdek;

import io.kroxylicious.kms.service.Serde;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.provider.kroxylicious.inmemory.UnitTestingKmsService;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;
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

    @Test
    void testLimitsNumbersOfEncryptors() {
        // Given
        UnitTestingKmsService unitTestingKmsService = UnitTestingKmsService.newInstance();
        UnitTestingKmsService.Config options = new UnitTestingKmsService.Config(12, 96);
        var kms = unitTestingKmsService.buildKms(options);
        var kekId = kms.generateKey();
        kms.createAlias(kekId, "foo");
        var dm = new DekManager<>(unitTestingKmsService, options, 1);

        // When
        var dek = dm.generateDek(kekId).toCompletableFuture().join();

        // Then
        assertThatThrownBy(() -> dek.encryptor(2)).isExactlyInstanceOf(ExhaustedDekException.class);
    }

    @Test
    void testDecryptedEdekIsGoodForDecryptingData() {
        // Given
        UnitTestingKmsService unitTestingKmsService = UnitTestingKmsService.newInstance();
        UnitTestingKmsService.Config options = new UnitTestingKmsService.Config(12, 96);
        var kms = unitTestingKmsService.buildKms(options);
        var kekId = kms.generateKey();
        kms.createAlias(kekId, "foo");
        var dm = new DekManager<>(unitTestingKmsService, options, 1_000);

        // Generate a DEK anduse it to encrypt
        var dek = dm.generateDek(kekId).toCompletableFuture().join();
        ByteBuffer plaintext = ByteBuffer.wrap("hello, world".getBytes(StandardCharsets.UTF_8));
        ByteBuffer aad = null;
        ByteBuffer[] ciphertext = new ByteBuffer[1];
        ByteBuffer[] params = new ByteBuffer[1];
        InMemoryEdek edek;
        try (DataEncryptionKey<InMemoryEdek>.Encryptor encryptor = dek.encryptor(1)) {
            encryptor.encrypt(plaintext,
                    aad,
                    x -> params[0] = ByteBuffer.allocate(x),
                    (x, y) -> ciphertext[0] = ByteBuffer.allocate(y));
            edek = encryptor.edek();
        }
        var edekBuffer = ByteBuffer.allocate(1024);
        Serde<InMemoryEdek> inMemoryEdekSerde = dm.edekSerde();
        inMemoryEdekSerde.serialize(edek, edekBuffer);
        edekBuffer.flip();

        // When
        var decrypted = dm.decryptEdek(inMemoryEdekSerde.deserialize(edekBuffer)).toCompletableFuture().join();

        // Then
        assertThatThrownBy(() -> decrypted.encryptor(1)).isExactlyInstanceOf(ExhaustedDekException.class);
        var decodedPlaintext = ByteBuffer.allocate(plaintext.capacity());
        decrypted.decryptor().decrypt(ciphertext[0], aad, params[0], decodedPlaintext);
        assertThat(new String(decodedPlaintext.array(), StandardCharsets.UTF_8)).isEqualTo("hello, world");
    }


}
