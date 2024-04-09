/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.CloseableIterator;

import io.kroxylicious.filter.encryption.dek.BufferTooSmallException;
import io.kroxylicious.filter.encryption.dek.Dek;
import io.kroxylicious.filter.encryption.dek.ExhaustedDekException;
import io.kroxylicious.filter.encryption.records.RecordStream;
import io.kroxylicious.kms.service.Serde;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

public class InBandEncryptionManager<K, E> implements EncryptionManager<K> {

    private static final int MAX_ATTEMPTS = 3;

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
    * {@link RecordEncryptor#ENCRYPTION_HEADER_NAME} header.
    */
    private final EncryptionVersion encryptionVersion;
    private final Serde<E> edekSerde;
    private final EncryptionDekCache<K, E> dekCache;
    @NonNull
    private final FilterThreadExecutor filterThreadExecutor;
    private final int recordBufferInitialBytes;
    private final int recordBufferMaxBytes;

    public InBandEncryptionManager(@NonNull Serde<E> edekSerde,
                                   int recordBufferInitialBytes,
                                   int recordBufferMaxBytes,
                                   @NonNull EncryptionDekCache<K, E> dekCache,
                                   @NonNull FilterThreadExecutor filterThreadExecutor) {
        this.filterThreadExecutor = filterThreadExecutor;
        this.encryptionVersion = EncryptionVersion.V2; // TODO read from config
        this.edekSerde = Objects.requireNonNull(edekSerde);
        if (recordBufferInitialBytes <= 0) {
            throw new IllegalArgumentException();
        }
        this.recordBufferInitialBytes = recordBufferInitialBytes;
        if (recordBufferMaxBytes <= 0) {
            throw new IllegalArgumentException();
        }
        this.recordBufferMaxBytes = recordBufferMaxBytes;
        this.dekCache = dekCache;

    }

    @VisibleForTesting
    CompletionStage<Dek<E>> currentDek(@NonNull EncryptionScheme<K> encryptionScheme) {
        // todo should we add some scheduled timeout as well? or should we rely on the KMS to timeout appropriately.
        return dekCache.get(encryptionScheme, filterThreadExecutor);
    }

    @Override
    @NonNull
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
        return attemptEncrypt(topicName, partition, encryptionScheme, records, 0, batchRecordCounts, bufferAllocator);
    }

    private ByteBufferOutputStream allocateBufferForEncrypt(@NonNull MemoryRecords records,
                                                            @NonNull IntFunction<ByteBufferOutputStream> bufferAllocator) {
        int sizeEstimate = 2 * records.sizeInBytes();
        // Accurate estimation is tricky without knowing the record sizes
        return bufferAllocator.apply(sizeEstimate);
    }

    private CompletionStage<MemoryRecords> attemptEncrypt(@NonNull String topicName,
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
                    var encryptedMemoryRecords = encryptBatches(
                            topicName,
                            partition,
                            encryptionScheme,
                            records,
                            encryptor,
                            bufferAllocator);
                    return CompletableFuture.completedFuture(encryptedMemoryRecords);
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
            return attemptEncrypt(topicName,
                    partition,
                    encryptionScheme,
                    records,
                    attempt + 1,
                    batchRecordCounts,
                    bufferAllocator);
        });
    }

    @NonNull
    private MemoryRecords encryptBatches(@NonNull String topicName,
                                         int partition,
                                         @NonNull EncryptionScheme<K> encryptionScheme,
                                         @NonNull MemoryRecords memoryRecords,
                                         @NonNull Dek<E>.Encryptor encryptor,
                                         @NonNull IntFunction<ByteBufferOutputStream> bufferAllocator) {
        ByteBuffer recordBuffer = ByteBuffer.allocate(recordBufferInitialBytes);
        do {
            try {
                return RecordStream.ofRecords(memoryRecords)
                        .mapConstant(encryptor)
                        .toMemoryRecords(allocateBufferForEncrypt(memoryRecords, bufferAllocator),
                                new RecordEncryptor<>(topicName,
                                        partition,
                                        encryptionVersion,
                                        encryptionScheme,
                                        edekSerde,
                                        recordBuffer));
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
        dekCache.invalidate(encryptionScheme);
    }
}
