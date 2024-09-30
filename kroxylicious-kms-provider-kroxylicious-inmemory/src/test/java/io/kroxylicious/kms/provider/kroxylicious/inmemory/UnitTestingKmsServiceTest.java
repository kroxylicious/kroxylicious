/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.kroxylicious.inmemory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.SecretKeyUtils;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UnitTestingKmsServiceTest {

    private UnitTestingKmsService service;

    @BeforeEach
    public void beforeEach() {
        service = UnitTestingKmsService.newInstance();
        service.initialize(new UnitTestingKmsService.Config());
    }

    @AfterEach
    public void afterEach() {
        Optional.ofNullable(service).ifPresent(UnitTestingKmsService::close);
    }

    @Test
    void shouldRejectOutOfBoundIvBytes() {
        // given
        List<UnitTestingKmsService.Kek> existingKeks = List.of();
        assertThatThrownBy(() -> new UnitTestingKmsService.Config(0, 128, existingKeks))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectOutOfBoundAuthBits() {
        List<UnitTestingKmsService.Kek> existingKeks = List.of();
        assertThatThrownBy(() -> new UnitTestingKmsService.Config(12, 0, existingKeks))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldGenerateDeks() {
        // given
        var kms1 = service.buildKms();

        var key1 = kms1.generateKey();
        assertThat(key1).isNotNull();

        // when
        var gen1 = kms1.generateDekPair(key1);

        // then
        assertThat(gen1).isCompleted();
    }

    @Test
    void shouldRejectsAnotherKmsesKeks() {
        // given
        var kms1 = service.buildKms();
        var service2 = UnitTestingKmsService.newInstance();
        service2.initialize(new UnitTestingKmsService.Config());
        var kms2 = service2.buildKms();

        var key1 = kms1.generateKey();
        assertThat(key1).isNotNull();
        var key2 = kms2.generateKey();
        assertThat(key1).isNotNull();

        // when
        var gen1 = kms1.generateDekPair(key2);
        var gen2 = kms2.generateDekPair(key1);

        // then
        assertThat(gen1).failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(UnknownKeyException.class)
                .withMessage("io.kroxylicious.kms.service.UnknownKeyException");

        assertThat(gen2).failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(UnknownKeyException.class)
                .withMessage("io.kroxylicious.kms.service.UnknownKeyException");
        service2.close();
    }

    @Test
    void shouldDecryptDeks() {
        // given
        var kms = service.buildKms();
        var kek = kms.generateKey();
        assertThat(kek).isNotNull();
        var pair = kms.generateDekPair(kek).join();
        assertThat(pair).isNotNull();
        assertThat(pair.edek()).isNotNull();
        assertThat(pair.dek()).isNotNull();

        // when
        var decryptedDek = kms.decryptEdek(pair.edek()).join();

        // then
        var expected = (DestroyableRawSecretKey) pair.dek();
        assertThat(decryptedDek)
                .asInstanceOf(InstanceOfAssertFactories.type(DestroyableRawSecretKey.class))
                .matches(x -> SecretKeyUtils.same(x, expected));
    }

    @Test
    void shouldSerializeAndDeserializeEdeks() {
        var kms = service.buildKms();
        var kek = kms.generateKey();

        var edek = kms.generateDekPair(kek).join().edek();

        var serde = kms.edekSerde();
        var buffer = ByteBuffer.allocate(serde.sizeOf(edek));
        serde.serialize(edek, buffer);
        assertThat(buffer.hasRemaining()).isFalse();
        buffer.flip();

        var deserialized = serde.deserialize(buffer);

        assertThat(edek).isEqualTo(deserialized);
    }

    @Test
    void shouldLookupByAlias() {
        var kms = service.buildKms();
        var kek = kms.generateKey();

        var lookup = kms.resolveAlias("bob");
        assertThat(lookup).failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(UnknownAliasException.class)
                .withMessage("io.kroxylicious.kms.service.UnknownAliasException: bob");

        kms.createAlias(kek, "bob");
        assertThat(kms.resolveAlias("bob"))
                .succeedsWithin(Duration.ZERO)
                .isEqualTo(kek);

        kms.deleteAlias("bob");
        lookup = kms.resolveAlias("bob");
        assertThat(lookup).failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(UnknownAliasException.class)
                .withMessage("io.kroxylicious.kms.service.UnknownAliasException: bob");
    }

}
