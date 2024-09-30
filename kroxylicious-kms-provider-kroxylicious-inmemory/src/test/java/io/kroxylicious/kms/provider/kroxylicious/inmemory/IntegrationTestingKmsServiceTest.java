/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.SecretKeyUtils;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IntegrationTestingKmsServiceTest {

    private IntegrationTestingKmsService service;

    @BeforeEach
    public void beforeEach() {
        service = IntegrationTestingKmsService.newInstance();
    }

    @AfterEach
    public void afterEach() {
        Optional.ofNullable(service).ifPresent(IntegrationTestingKmsService::close);
    }

    @Test
    void shouldRejectNullName() {
        // given
        assertThrows(IllegalArgumentException.class, () -> new IntegrationTestingKmsService.Config(null));
    }

    @Test
    void shouldRejectEmptyName() {
        assertThrows(IllegalArgumentException.class, () -> new IntegrationTestingKmsService.Config(""));
    }

    @Test
    void shouldWorkAcrossServiceInstances() {
        // given
        var kmsId = UUID.randomUUID().toString();
        service.initialize(new IntegrationTestingKmsService.Config(kmsId));
        var kms = service.buildKms();
        var kek = kms.generateKey();
        assertNotNull(kek);
        kms.createAlias(kek, "myAlias");

        var service2 = IntegrationTestingKmsService.newInstance();
        service2.initialize(new IntegrationTestingKmsService.Config(kmsId));

        // when
        var theSameKms = service2.buildKms();

        // then
        assertEquals(kek, theSameKms.resolveAlias("myAlias").join());

        IntegrationTestingKmsService.delete(kmsId);
        service2.close();
    }

    @Test
    void shouldGenerateDeks() {
        // given
        var kms1Id = UUID.randomUUID().toString();
        service.initialize(new IntegrationTestingKmsService.Config(kms1Id));
        var kms1 = service.buildKms();

        var kms2Id = UUID.randomUUID().toString();
        var service2 = IntegrationTestingKmsService.newInstance();
        service2.initialize(new IntegrationTestingKmsService.Config(kms2Id));
        var kms2 = service2.buildKms();
        var key1 = kms1.generateKey();
        assertNotNull(key1);
        var key2 = kms2.generateKey();
        assertNotNull(key2);

        // when
        CompletableFuture<DekPair<InMemoryEdek>> gen1 = kms1.generateDekPair(key1);
        CompletableFuture<DekPair<InMemoryEdek>> gen2 = kms2.generateDekPair(key2);

        // then
        assertThat(gen1).isCompleted();
        assertThat(gen2).isCompleted();

        IntegrationTestingKmsService.delete(kms1Id);
        IntegrationTestingKmsService.delete(kms2Id);

        service2.close();
    }

    @Test
    void shouldRejectsAnotherKmsesKeks() {
        // given
        var kms1Id = UUID.randomUUID().toString();
        service.initialize(new IntegrationTestingKmsService.Config(kms1Id));
        var kms1 = service.buildKms();
        var kms2Id = UUID.randomUUID().toString();
        var service2 = IntegrationTestingKmsService.newInstance();
        service2.initialize(new IntegrationTestingKmsService.Config(kms2Id));
        var kms2 = service2.buildKms();
        var key1 = kms1.generateKey();
        assertNotNull(key1);
        var key2 = kms2.generateKey();
        assertNotNull(key2);

        // when
        CompletableFuture<DekPair<InMemoryEdek>> gen1 = kms1.generateDekPair(key2);
        CompletableFuture<DekPair<InMemoryEdek>> gen2 = kms2.generateDekPair(key1);

        // then
        assertThat(gen1).failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(UnknownKeyException.class)
                .withMessage("io.kroxylicious.kms.service.UnknownKeyException");

        assertThat(gen2).failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(UnknownKeyException.class)
                .withMessage("io.kroxylicious.kms.service.UnknownKeyException");

        IntegrationTestingKmsService.delete(kms1Id);
        IntegrationTestingKmsService.delete(kms2Id);
        service2.close();
    }

    @Test
    void shouldDecryptDeks() {
        // given
        var kmsId = UUID.randomUUID().toString();
        service.initialize(new IntegrationTestingKmsService.Config(kmsId));
        var kms = service.buildKms();
        var kek = kms.generateKey();
        assertNotNull(kek);
        var pair = kms.generateDekPair(kek).join();
        assertNotNull(pair);
        assertNotNull(pair.edek());
        assertNotNull(pair.dek());

        // when
        var decryptedDek = kms.decryptEdek(pair.edek()).join();

        // then
        assertTrue(SecretKeyUtils.same((DestroyableRawSecretKey) pair.dek(), (DestroyableRawSecretKey) decryptedDek),
                "Expect the decrypted DEK to equal the originally generated DEK");

        IntegrationTestingKmsService.delete(kmsId);
    }

    @Test
    void shouldSerializeAndDeserializeEdeks() {
        var kmsId = UUID.randomUUID().toString();
        service.initialize(new IntegrationTestingKmsService.Config(kmsId));
        var kms = service.buildKms();
        var kek = kms.generateKey();

        var edek = kms.generateDekPair(kek).join().edek();

        var serde = kms.edekSerde();
        var buffer = ByteBuffer.allocate(serde.sizeOf(edek));
        serde.serialize(edek, buffer);
        assertFalse(buffer.hasRemaining());
        buffer.flip();

        var deserialized = serde.deserialize(buffer);

        assertEquals(edek, deserialized);

        IntegrationTestingKmsService.delete(kmsId);
    }

    @Test
    void shouldLookupByAlias() {
        var kmsId = UUID.randomUUID().toString();
        service.initialize(new IntegrationTestingKmsService.Config(kmsId));
        var kms = service.buildKms();
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

        IntegrationTestingKmsService.delete(kmsId);
    }

    @Test
    void deleteAliasFailsWithUnknownAlias() {
        var kmsId = UUID.randomUUID().toString();
        service.initialize(new IntegrationTestingKmsService.Config(kmsId));
        var kms = service.buildKms();

        assertThatThrownBy(() -> kms.deleteAlias("bob"))
                .isInstanceOf(UnknownAliasException.class);

        IntegrationTestingKmsService.delete(kmsId);
    }

    @Test
    void deleteKey() {
        var kmsId = UUID.randomUUID().toString();
        service.initialize(new IntegrationTestingKmsService.Config(kmsId));
        var kms = service.buildKms();
        var kek = kms.generateKey();

        kms.deleteKey(kek);
        assertThatThrownBy(() -> kms.deleteKey(kek))
                .isInstanceOf(UnknownKeyException.class);

        IntegrationTestingKmsService.delete(kmsId);
    }

    @Test
    void deleteKeyFailsIfAliasPresent() {
        var kmsId = UUID.randomUUID().toString();
        service.initialize(new IntegrationTestingKmsService.Config(kmsId));
        var kms = service.buildKms();
        var kek = kms.generateKey();
        kms.createAlias(kek, "bob");

        assertThatThrownBy(() -> kms.deleteKey(kek))
                .isInstanceOf(KmsException.class)
                .hasMessageContaining("referenced by an alias");

        IntegrationTestingKmsService.delete(kmsId);
    }

}
