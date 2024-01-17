/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.dek;

import edu.umd.cs.findbugs.annotations.NonNull;

import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.kms.service.Serde;

import java.util.concurrent.CompletionStage;

public class DekManager<K, E> {

    private final Kms<K, E> kms;

    public <C> DekManager(KmsService<C, K, E> kmsService, C config) {
        this.kms = kmsService.buildKms(config);
    }

    public Serde<E> edekSerde() {
        return kms.edekSerde();
    }

    public CompletionStage<K> resolveAlias(String alias) {
        return kms.resolveAlias(alias);
    }

    public CompletionStage<DataEncryptionKey<E>> generateDek(@NonNull K kekRef) {
        return kms.generateDekPair(kekRef).thenApply(dekPair -> new DataEncryptionKey<>(dekPair.edek(), dekPair.dek(), 1000));
    }

    public CompletionStage<DataEncryptionKey<E>> decryptEdek(@NonNull E edek) {
        return kms.decryptEdek(edek).thenApply(key -> new DataEncryptionKey<>(edek, key, 0));
    }
}
