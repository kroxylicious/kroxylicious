/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UnitTestingKmsServiceTest {

    UnitTestingKmsService service;

    @BeforeEach
    public void before() {
        service = UnitTestingKmsService.newInstance();
    }

    @Test
    void shouldRejectOutOfBoundIvBytes() {
        // given
        assertThrows(IllegalArgumentException.class, () -> new UnitTestingKmsService.Config(0, 128));
    }

    @Test
    void shouldRejectOutOfBoundAuthBits() {
        assertThrows(IllegalArgumentException.class, () -> new UnitTestingKmsService.Config(12, 0));
    }

    @Test
    void shouldSerializeAndDeserialiseKeks() {
        // given
        var kms = service.buildKms(new UnitTestingKmsService.Config());
        var kek = kms.generateKey();
        assertNotNull(kek);

        // when
        Serde<UUID> keyIdSerde = kms.keyIdSerde();
        var buffer = ByteBuffer.allocate(keyIdSerde.sizeOf(kek));
        keyIdSerde.serialize(kek, buffer);
        assertFalse(buffer.hasRemaining());
        buffer.flip();
        var loadedKek = keyIdSerde.deserialize(buffer);

        // then
        assertEquals(kek, loadedKek, "Expect the deserialized kek to be equal to the original kek");
    }

    @Test
    void shouldGenerateDeks() {
        // given
        var kms1 = service.buildKms(new UnitTestingKmsService.Config());
        var kms2 = UnitTestingKmsService.newInstance().buildKms(new UnitTestingKmsService.Config());
        var key1 = kms1.generateKey();
        assertNotNull(key1);
        var key2 = kms2.generateKey();
        assertNotNull(key2);

        // when
        CompletableFuture<InMemoryEdek> gen1 = kms1.generateDek(key1);
        CompletableFuture<InMemoryEdek> gen2 = kms2.generateDek(key2);

        // then
        assertThat(gen1).isCompleted();
        assertThat(gen2).isCompleted();
    }

    @Test
    void shouldRejectsAnotherKmsesKeks() {
        // given
        var kms1 = service.buildKms(new UnitTestingKmsService.Config());
        var kms2 = UnitTestingKmsService.newInstance().buildKms(new UnitTestingKmsService.Config());
        var key1 = kms1.generateKey();
        assertNotNull(key1);
        var key2 = kms2.generateKey();
        assertNotNull(key2);

        // when
        CompletableFuture<InMemoryEdek> gen1 = kms1.generateDek(key2);
        CompletableFuture<InMemoryEdek> gen2 = kms2.generateDek(key1);

        // then
        assertThat(gen1).failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(UnknownKeyException.class)
                .withMessage("io.kroxylicious.kms.service.UnknownKeyException");

        assertThat(gen2).failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(UnknownKeyException.class)
                .withMessage("io.kroxylicious.kms.service.UnknownKeyException");
    }

    @Test
    void shouldDecryptDeks() {
        // given
        var kms = service.buildKms(new UnitTestingKmsService.Config());
        var kek = kms.generateKey();
        assertNotNull(kek);
        var pair = kms.generateDekPair(kek).join();
        assertNotNull(pair);
        assertNotNull(pair.edek());
        assertNotNull(pair.dek());

        // when
        var decryptedDek = kms.decryptEdek(kek, pair.edek()).join();

        // then
        assertEquals(pair.dek(), decryptedDek, "Expect the decrypted DEK to equal the originally generated DEK");
    }

    @Test
    void shouldSerializeAndDeserializeEdeks() {
        var kms = service.buildKms(new UnitTestingKmsService.Config());
        var kek = kms.generateKey();

        var edek = kms.generateDek(kek).join();

        var serde = kms.edekSerde();
        var buffer = ByteBuffer.allocate(serde.sizeOf(edek));
        serde.serialize(edek, buffer);
        assertFalse(buffer.hasRemaining());
        buffer.flip();

        var deserialized = serde.deserialize(buffer);

        assertEquals(edek, deserialized);
    }

    @Test
    void shouldLookupByAlias() {
        var kms = service.buildKms(new UnitTestingKmsService.Config());
        var kek = kms.generateKey();

        var lookup = kms.resolveAlias("bob");
        assertThat(lookup).failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(UnknownAliasException.class)
                .withMessage("io.kroxylicious.kms.service.UnknownAliasException: bob");

        kms.createAlias(kek, "bob");
        var gotFromAlias = kms.resolveAlias("bob").join();
        assertEquals(kek, gotFromAlias);

        kms.deleteAlias("bob");
        lookup = kms.resolveAlias("bob");
        assertThat(lookup).failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(UnknownAliasException.class)
                .withMessage("io.kroxylicious.kms.service.UnknownAliasException: bob");
    }

}
