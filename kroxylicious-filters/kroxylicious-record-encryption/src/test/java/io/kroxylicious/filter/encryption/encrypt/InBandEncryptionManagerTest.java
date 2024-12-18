/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.encrypt;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.encryption.common.FilterThreadExecutor;
import io.kroxylicious.filter.encryption.config.RecordField;
import io.kroxylicious.filter.encryption.crypto.Encryption;
import io.kroxylicious.filter.encryption.dek.DekManager;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryEdek;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryKms;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.UnitTestingKmsService;
import io.kroxylicious.test.record.RecordTestUtils;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

class InBandEncryptionManagerTest {

    // this test covers a bug fix where multiple encrypting threads would invalidate the cache key with
    // undefined results. We only need to invalidate each cached DEK once
    @Test
    void testMultipleThreadsCooperateToMinimiseDekCreations() {
        InMemoryKms kms = getInMemoryKms();
        // we are testing a race condition and want high parallelism to prompt parallel usages of exhausted DEKs
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        final DekManager<UUID, InMemoryEdek> dekManager = new DekManager<>(new AsyncKms<>(kms, executor), 1);
        EncryptionDekCache<UUID, InMemoryEdek> cache = new EncryptionDekCache<>(dekManager, executor, EncryptionDekCache.NO_MAX_CACHE_SIZE, Duration.ofHours(1),
                Duration.ofHours(1));
        var encryptionManager = createEncryptionManager(dekManager, cache, executor);
        var kekId = kms.generateKey();

        var value = new byte[]{ 1, 2, 3 };
        Record record = RecordTestUtils.record(value);

        List<Record> initial = List.of(record);

        int numEncryptionOperations = 50;
        List<CompletableFuture<Void>> encrypts = IntStream.range(0, numEncryptionOperations)
                .mapToObj(x -> doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)), initial,
                        new ArrayList<>()).toCompletableFuture())
                .toList();

        CompletableFuture<?>[] array = encrypts.toArray(CompletableFuture[]::new);
        CompletableFuture<Void> all = CompletableFuture.allOf(array);
        assertThat(all).succeedsWithin(10, TimeUnit.SECONDS);
        assertThat(cache.invalidationCount()).isEqualTo(numEncryptionOperations - 1);
    }

    @NonNull
    private static InMemoryKms getInMemoryKms() {
        var kmsService = UnitTestingKmsService.newInstance();
        kmsService.initialize(new UnitTestingKmsService.Config());
        return kmsService.buildKms();
    }

    @NonNull
    private static CompletionStage<Void> doEncrypt(
                                                   InBandEncryptionManager<UUID, InMemoryEdek> encryptionManager,
                                                   String topic,
                                                   int partition,
                                                   EncryptionScheme<UUID> scheme,
                                                   List<Record> initial,
                                                   List<Record> encrypted) {
        MemoryRecords records = RecordTestUtils.memoryRecords(initial);
        return encryptionManager.encrypt(topic, partition, scheme, records, ByteBufferOutputStream::new)
                .thenApply(memoryRecords -> {
                    memoryRecords.records().forEach(encrypted::add);
                    return null;
                });
    }

    @NonNull
    private static InBandEncryptionManager<UUID, InMemoryEdek> createEncryptionManager(DekManager<UUID, InMemoryEdek> dekManager,
                                                                                       EncryptionDekCache<UUID, InMemoryEdek> cache,
                                                                                       ScheduledExecutorService executor) {

        return new InBandEncryptionManager<>(Encryption.V2,
                dekManager.edekSerde(),
                1024 * 1024,
                8 * 1024 * 1024,
                cache,
                new FilterThreadExecutor(executor));
    }

}
