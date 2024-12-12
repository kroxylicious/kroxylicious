/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.service;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KMS IT ensuring that KMS implementations conform to the KMS contact end to end.
 *
 * @param <C> The config type
 * @param <K> The key reference
 * @param <E> The type of encrypted DEK
 */
@ExtendWith(TestKmsFacadeInvocationContextProvider.class)
class KmsIT<C, K, E> {

    private Kms<K, E> kms;

    private TestKekManager manager;
    private String alias;
    private K resolvedKekId;
    private KmsService<C, K, E> service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void beforeEach(TestKmsFacade<?, ?, ?> facade) throws Exception {
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
        Optional.ofNullable(service).ifPresent(KmsService::close);
    }

    @TestTemplate
    void resolveKeyByName() {
        var alias = "mykey";
        manager.generateKek(alias);
        var resolved = kms.resolveAlias(alias);
        assertThat(resolved)
                .succeedsWithin(Duration.ofSeconds(5))
                .isNotNull();
    }

    @TestTemplate
    void resolveWithUnknownAlias() {
        var alias = "unknown";
        var resolved = kms.resolveAlias(alias);
        assertThat(resolved)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownAliasException.class);
    }

    @TestTemplate
    void generatedEncryptedDekDecryptsBackToPlain() {
        var pairStage = kms.generateDekPair(resolvedKekId);
        var pair = pairStage.toCompletableFuture().join();

        var decryptedDekStage = kms.decryptEdek(pair.edek());
        assertThat(decryptedDekStage)
                .succeedsWithin(Duration.ofSeconds(5))
                .matches(sk -> SecretKeyUtils.same((DestroyableRawSecretKey) sk, (DestroyableRawSecretKey) pair.dek()));
    }

    @TestTemplate
    void generatedDekPairWithUnknownKeyId() {
        manager.deleteKek(alias);

        var pairStage = kms.generateDekPair(resolvedKekId);
        assertThat(pairStage)
                .failsWithin(Duration.ofSeconds(5))
                .withThrowableThat()
                .withCauseInstanceOf(UnknownKeyException.class);
    }

    @TestTemplate
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

    @TestTemplate
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

    @TestTemplate
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
