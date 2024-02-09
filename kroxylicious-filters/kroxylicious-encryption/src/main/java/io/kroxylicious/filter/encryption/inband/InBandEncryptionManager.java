/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.IntFunction;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;

import io.kroxylicious.filter.encryption.EncryptionException;
import io.kroxylicious.filter.encryption.EncryptionManager;
import io.kroxylicious.filter.encryption.EncryptionScheme;
import io.kroxylicious.filter.encryption.EncryptionVersion;
import io.kroxylicious.filter.encryption.FilterThreadExecutor;
import io.kroxylicious.filter.encryption.dek.BufferTooSmallException;
import io.kroxylicious.filter.encryption.dek.CipherSpec;
import io.kroxylicious.filter.encryption.dek.Dek;
import io.kroxylicious.filter.encryption.dek.DekManager;
import io.kroxylicious.filter.encryption.dek.ExhaustedDekException;
import io.kroxylicious.filter.encryption.records.BatchAwareMemoryRecordsBuilder;
import io.kroxylicious.filter.encryption.records.RecordBatchUtils;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public class InBandEncryptionManager<K, E> implements EncryptionManager<K> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InBandEncryptionManager.class);

    private static final int MAX_ATTEMPTS = 3;
    public static final int NO_MAX_CACHE_SIZE = -1;
    @NonNull
    private final FilterThreadExecutor filterThreadExecutor;

    private record CacheKey<K>(K kek, CipherSpec cipherSpec) {}

    private static <K> CacheKey<K> cacheKey(EncryptionScheme<K> encryptionScheme) {
        return new CacheKey<>(encryptionScheme.kekId(), CipherSpec.AES_128_GCM_128);
    }

    @NonNull
    static List<Integer> batchRecordCounts(@NonNull MemoryRecords records) {
        List<Integer> sizes = new ArrayList<>();
        for (MutableRecordBatch batch : records.batches()) {
            sizes.add(InBandEncryptionManager.recordCount(batch));
        }
        return sizes;
    }

    private static int recordCount(@NonNull MutableRecordBatch batch) {
        Integer count = batch.countOrNull();
        if (count == null) {
            // for magic <2 count will be null
            CloseableIterator<Record> iterator = batch.skipKeyValueIterator(BufferSupplier.NO_CACHING);
            int c = 0;
            while (iterator.hasNext()) {
                c++;
                iterator.next();
            }
            count = c;
        }
        return count;
    }

    /**
    * The encryption version used on the produce path.
    * Note that the encryption version used on the fetch path is read from the
    * {@link InBandDecryptionManager#ENCRYPTION_HEADER_NAME} header.
    */
    private final EncryptionVersion encryptionVersion;
    private final DekManager<K, E> dekManager;
    private final AsyncLoadingCache<CacheKey<K>, Dek<E>> dekCache;

    private final int recordBufferInitialBytes;
    private final int recordBufferMaxBytes;

    public InBandEncryptionManager(@NonNull DekManager<K, E> dekManager,
                                   int recordBufferInitialBytes,
                                   int recordBufferMaxBytes,
                                   @Nullable Executor dekCacheExecutor,
                                   @NonNull FilterThreadExecutor filterThreadExecutor,
                                   int dekCacheMaxItems) {
        this.filterThreadExecutor = filterThreadExecutor;
        this.encryptionVersion = EncryptionVersion.V1; // TODO read from config
        this.dekManager = Objects.requireNonNull(dekManager);
        if (recordBufferInitialBytes <= 0) {
            throw new IllegalArgumentException();
        }
        this.recordBufferInitialBytes = recordBufferInitialBytes;
        if (recordBufferMaxBytes <= 0) {
            throw new IllegalArgumentException();
        }
        this.recordBufferMaxBytes = recordBufferMaxBytes;
        Caffeine<Object, Object> cache = Caffeine.newBuilder();
        if (dekCacheMaxItems != NO_MAX_CACHE_SIZE) {
            cache = cache.maximumSize(dekCacheMaxItems);
        }
        if (dekCacheExecutor != null) {
            cache = cache.executor(dekCacheExecutor);
        }
        this.dekCache = cache
                .removalListener(this::afterCacheEviction)
                .buildAsync(this::requestGenerateDek);
    }

    /** Invoked by Caffeine when a DEK needs to be loaded */
    private CompletableFuture<Dek<E>> requestGenerateDek(@NonNull CacheKey<K> cacheKey,
                                                         @NonNull Executor executor) {
        return dekManager.generateDek(cacheKey.kek(), cacheKey.cipherSpec())
                .thenApply(dek -> {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Adding DEK to cache: {}", dek);
                    }
                    dek.destroyForDecrypt();
                    return dek;
                })
                .toCompletableFuture();
    }

    /** Invoked by Caffeine after a DEK is evicted from the cache. */
    private void afterCacheEviction(@Nullable CacheKey<K> cacheKey,
                                    @Nullable Dek<E> dek,
                                    RemovalCause removalCause) {
        if (dek != null) {
            dek.destroyForEncrypt();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Attempted to destroy DEK: {}", dek);
            }
        }
    }

    // @VisibleForTesting
    CompletionStage<Dek<E>> currentDek(@NonNull EncryptionScheme<K> encryptionScheme) {
        // todo should we add some scheduled timeout as well? or should we rely on the KMS to timeout appropriately.
        return filterThreadExecutor.completingOnFilterThread(dekCache.get(cacheKey(encryptionScheme)));
    }

    @Override
    @NonNull
    @SuppressWarnings("java:S2445")
    public CompletionStage<MemoryRecords> encrypt(@NonNull String topicName,
                                                  int partition,
                                                  @NonNull EncryptionScheme<K> encryptionScheme,
                                                  @NonNull MemoryRecords records,
                                                  @NonNull IntFunction<ByteBufferOutputStream> bufferAllocator) {
        if (records.sizeInBytes() == 0) {
            // no records to transform, return input without modification
            return CompletableFuture.completedFuture(records);
        }

        List<Integer> batchRecordCounts = InBandEncryptionManager.batchRecordCounts(records);
        // it is possible to encounter MemoryRecords that have had all their records compacted away, but
        // the recordbatch metadata still exists. https://kafka.apache.org/documentation/#recordbatch
        if (batchRecordCounts.stream().allMatch(size -> size == 0)) {
            return CompletableFuture.completedFuture(records);
        }
        return attemptEncrypt(topicName, partition, encryptionScheme, records, 0, batchRecordCounts, bufferAllocator)
                .thenApply(BatchAwareMemoryRecordsBuilder::build);
    }

    private ByteBufferOutputStream allocateBufferForEncrypt(@NonNull MemoryRecords records,
                                                            @NonNull IntFunction<ByteBufferOutputStream> bufferAllocator) {
        int sizeEstimate = 2 * records.sizeInBytes();
        // Accurate estimation is tricky without knowing the record sizes
        return bufferAllocator.apply(sizeEstimate);
    }

    @SuppressWarnings("java:S2445")
    private CompletionStage<BatchAwareMemoryRecordsBuilder> attemptEncrypt(@NonNull String topicName,
                                                                           int partition,
                                                                           @NonNull EncryptionScheme<K> encryptionScheme,
                                                                           @NonNull MemoryRecords records,
                                                                           int attempt,
                                                                           @NonNull List<Integer> batchRecordCounts,
                                                                           @NonNull IntFunction<ByteBufferOutputStream> bufferAllocator) {
        int allRecordsCount = batchRecordCounts.stream().mapToInt(value -> value).sum();
        if (attempt >= MAX_ATTEMPTS) {
            return CompletableFuture.failedFuture(
                    new RequestNotSatisfiable("failed to reserve an EDEK to encrypt " + allRecordsCount + " records for topic " + topicName + " partition "
                            + partition + " after " + attempt + " attempts"));
        }
        return currentDek(encryptionScheme).thenCompose(dek -> {
            // if it's not alive we know a previous encrypt call has removed this stage from the cache and fall through to retry encrypt
            if (!dek.isDestroyed()) {
                try (Dek<E>.Encryptor encryptor = dek.encryptor(allRecordsCount)) {
                    BatchAwareMemoryRecordsBuilder encrypt = encryptBatches(encryptionScheme, records, encryptor, bufferAllocator);
                    return CompletableFuture.completedFuture(encrypt);
                }
                catch (ExhaustedDekException e) {
                    rotateKeyContext(encryptionScheme, dek);
                    // fall through to recursive call below...
                }
                catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                }
            }
            // recurse, incrementing the attempt number
            return attemptEncrypt(topicName, partition, encryptionScheme, records, attempt + 1, batchRecordCounts, bufferAllocator);
        });
    }

    @NonNull
    private BatchAwareMemoryRecordsBuilder encryptBatches(@NonNull EncryptionScheme<K> encryptionScheme,
                                                          @NonNull MemoryRecords memoryRecords,
                                                          @NonNull Dek<E>.Encryptor encryptor,
                                                          @NonNull IntFunction<ByteBufferOutputStream> bufferAllocator) {
        BatchAwareMemoryRecordsBuilder builder = new BatchAwareMemoryRecordsBuilder(allocateBufferForEncrypt(memoryRecords, bufferAllocator));
        for (MutableRecordBatch batch : memoryRecords.batches()) {
            maybeEncryptBatch(encryptionScheme, encryptor, batch, builder);
        }
        return builder;
    }

    private void maybeEncryptBatch(@NonNull EncryptionScheme<K> encryptionScheme,
                                   @NonNull Dek<E>.Encryptor encryptor,
                                   @NonNull MutableRecordBatch batch,
                                   @NonNull BatchAwareMemoryRecordsBuilder builder) {
        if (batch.isControlBatch()) {
            builder.writeBatch(batch);
        }
        else {
            List<Record> records = StreamSupport.stream(batch.spliterator(), false).toList();
            if (records.isEmpty()) {
                builder.writeBatch(batch);
            }
            else {
                encryptBatch(encryptionScheme, encryptor, batch, builder);
            }
        }
    }

    private void encryptBatch(@NonNull EncryptionScheme<K> encryptionScheme,
                              @NonNull Dek<E>.Encryptor encryptor,
                              @NonNull MutableRecordBatch batch,
                              @NonNull BatchAwareMemoryRecordsBuilder builder) {
        ByteBuffer recordBuffer = ByteBuffer.allocate(recordBufferInitialBytes);
        do {
            try {
                RecordBatchUtils.toMemoryRecords(batch,
                        new RecordEncryptor<>(encryptionVersion, encryptionScheme, encryptor, dekManager.edekSerde(), recordBuffer),
                        builder);
                break;
            }
            catch (BufferTooSmallException e) {
                int newCapacity = 2 * recordBuffer.capacity();
                if (newCapacity > recordBufferMaxBytes) {
                    throw new EncryptionException("Record buffer cannot grow greater than " + recordBufferMaxBytes + " bytes");
                }
                recordBuffer = ByteBuffer.allocate(newCapacity);
            }
        } while (true);
    }

    private void rotateKeyContext(@NonNull EncryptionScheme<K> encryptionScheme,
                                  @NonNull Dek<E> dek) {
        dek.destroyForEncrypt();
        dekCache.synchronous().invalidate(cacheKey(encryptionScheme));
    }
}
