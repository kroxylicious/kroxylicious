/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.kms.service;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.DestroyableRawSecretKey;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.kms.service.UnknownKeyException;
import io.kroxylicious.testing.kms.SecretKeyUtils;
import io.kroxylicious.testing.kms.TestKekManager;
import io.kroxylicious.testing.kms.TestKmsFacade;
import io.kroxylicious.testing.kms.TestKmsFacadeFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KMS IT ensuring that KMS implementations conform to the KMS contact end to end.
 *
 * @param <C> The config type
 * @param <K> The key reference
 * @param <E> The type of encrypted DEK
 */
@ParameterizedClass(autoCloseArguments = true)
@MethodSource("facadesSource")
class KmsIT<C, K, E> {

    static Stream<? extends TestKmsFacade<?, ?, ?>> facadesSource() {
        // We rely on the fact that streams are lazy so the facade isn't built
        // or started until the first test needs it.
        return TestKmsFacadeFactory.getTestKmsFacadeFactories()
                .map(TestKmsFacadeFactory::build)
                .filter(TestKmsFacade::isAvailable)
                .peek(TestKmsFacade::start);
    }

    @Parameter
    TestKmsFacade<?, ?, ?> facade;

    private Kms<K, E> kms;

    private TestKekManager manager;
    private String alias;
    private K resolvedKekId;
    private KmsService<C, K, E> service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void beforeEach() throws Exception {
        service = (KmsService<C, K, E>) facade.getKmsServiceClass().getDeclaredConstructor(new Class[]{}).newInstance();
        C kmsServiceConfig = (C) facade.getKmsServiceConfig();
        service.initialize(kmsServiceConfig);
        kms = service.buildKms();
        manager = facade.getTestKekManager();

        alias = "alias-" + UUID.randomUUID();
        resolvedKekId = createAndResolve(alias);
    }

    @AfterEach
    void afterEach() {
        // Clean up keys to keep the shared KMS environment clean
        idempotentDeleteKek(alias);
        Optional.ofNullable(service).ifPresent(KmsService::close);
    }

    private void idempotentDeleteKek(String kekAlias) {
        if (manager != null && kekAlias != null) {
            try {
                manager.deleteKek(kekAlias);
            }
            catch (Exception e) {
                // Ignore cleanup failures as key might already be deleted by the test or not exist
            }
        }
    }

    @Test
    void resolveKeyByName() {
        String namedAlias = "my-key" + UUID.randomUUID();
        try {
            manager.generateKek(namedAlias);
            var resolved = kms.resolveAlias(alias);
            assertThat(resolved)
                    .succeedsWithin(Duration.ofSeconds(5))
                    .isNotNull();
        }
        finally {
            idempotentDeleteKek(namedAlias);
        }
    }

    @Test
    void resolveWithUnknownAlias() {
        var unknownAlias = "unknown";
        var resolved = kms.resolveAlias(unknownAlias);
        assertThat(resolved)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownAliasException.class);
    }

    @Test
    void generatedEncryptedDekDecryptsBackToPlain() {
        var pairStage = kms.generateDekPair(resolvedKekId);
        var pair = pairStage.toCompletableFuture().join();

        var decryptedDekStage = kms.decryptEdek(pair.edek());
        assertThat(decryptedDekStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .matches(sk -> SecretKeyUtils.same((DestroyableRawSecretKey) sk, (DestroyableRawSecretKey) pair.dek()));
    }

    @Test
    void generatedDekPairWithUnknownKeyId() {
        manager.deleteKek(alias);

        var pairStage = kms.generateDekPair(resolvedKekId);
        assertThat(pairStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownKeyException.class);
    }

    @Test
    void decryptDekAfterRotate() {

        var pairStage = kms.generateDekPair(resolvedKekId);
        assertThat(pairStage).succeedsWithin(Duration.ofSeconds(5));
        var pair = pairStage.toCompletableFuture().join();

        manager.rotateKek(alias);

        var decryptedDekStage = kms.decryptEdek(pair.edek());
        assertThat(decryptedDekStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .matches(sk -> SecretKeyUtils.same((DestroyableRawSecretKey) sk, (DestroyableRawSecretKey) pair.dek()));
    }

    @Test
    void failToDecryptEdekWithUnknownKeyId() {

        var dekPairStage = kms.generateDekPair(resolvedKekId);
        assertThat(dekPairStage).succeedsWithin(Duration.ofSeconds(5));
        var dek = dekPairStage.toCompletableFuture().join();

        manager.deleteKek(alias);

        var secretKeyStage = kms.decryptEdek(dek.edek());

        assertThat(secretKeyStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownKeyException.class);
    }

    @Test
    void edekSerdeRoundTrip() {

        var pairStage = kms.generateDekPair(resolvedKekId);
        assertThat(pairStage).succeedsWithin(Duration.ofSeconds(5));
        var pair = pairStage.toCompletableFuture().join();
        assertThat(pair).extracting(DekPair::edek).isNotNull();

        var edek = pair.edek();
        var serde = kms.edekSerde();
        var buf = ByteBuffer.allocate(serde.sizeOf(edek));
        serde.serialize(edek, buf);
        buf.flip();
        var output = serde.deserialize(buf);
        assertThat(output).isEqualTo(edek);
    }

    private K createAndResolve(String alias) {
        manager.generateKek(alias);

        var resolveStage = kms.resolveAlias(alias);
        assertThat(resolveStage).succeedsWithin(Duration.ofSeconds(5));
        return resolveStage.toCompletableFuture().join();
    }
}
