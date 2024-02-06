/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.common.utils.CloseableIterator;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.kroxylicious.filter.encryption.AadSpec;
import io.kroxylicious.filter.encryption.CipherCode;
import io.kroxylicious.filter.encryption.EncryptionException;
import io.kroxylicious.filter.encryption.EncryptionScheme;
import io.kroxylicious.filter.encryption.EncryptionVersion;
import io.kroxylicious.filter.encryption.EnvelopeEncryptionFilter;
import io.kroxylicious.filter.encryption.KeyManager;
import io.kroxylicious.filter.encryption.records.BatchAwareMemoryRecordsBuilder;
import io.kroxylicious.filter.encryption.records.RecordBatchUtils;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.Serde;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An implementation of {@link KeyManager} that uses envelope encryption, AES-GCM and stores the KEK id and encrypted DEK
 * alongside the record ("in-band").
 * @param <K> The type of KEK id.
 * @param <E> The type of the encrypted DEK.
 */
public class InBandKeyManager<K, E> implements KeyManager<K> {

    private static final int MAX_ATTEMPTS = 3;

    /**
     * The encryption header. The value is the encryption version that was used to serialize the parcel and the wrapper.
     */
    static final String ENCRYPTION_HEADER_NAME = "kroxylicious.io/encryption";

    /**
     * The encryption version used on the produce path.
     * Note that the encryption version used on the fetch path is read from the
     * {@link #ENCRYPTION_HEADER_NAME} header.
     */
    private final EncryptionVersion encryptionVersion;

    private final Kms<K, E> kms;
    private final BufferPool bufferPool;
    private final Serde<E> edekSerde;
    // TODO cache expiry, with key descruction
    private final AsyncLoadingCache<K, KeyContext> keyContextCache;
    private final AsyncLoadingCache<E, AesGcmEncryptor> decryptorCache;
    private final long dekTtlNanos;
    private final int maxEncryptionsPerDek;

    public InBandKeyManager(Kms<K, E> kms,
                            BufferPool bufferPool,
                            int maxEncryptionsPerDek) {
        this.kms = kms;
        this.bufferPool = bufferPool;
        this.edekSerde = kms.edekSerde();
        this.dekTtlNanos = 5_000_000_000L;
        this.maxEncryptionsPerDek = maxEncryptionsPerDek;
        // TODO This ^^ must be > the maximum size of a batch to avoid an infinite loop
        this.keyContextCache = Caffeine.newBuilder()
                .buildAsync((key, executor) -> makeKeyContext(key));
        this.decryptorCache = Caffeine.newBuilder()
                .buildAsync((edek, executor) -> makeDecryptor(edek));
        this.encryptionVersion = EncryptionVersion.V1; // TODO read from config
    }

    private CompletionStage<KeyContext> currentDekContext(@NonNull K kekId) {
        // todo should we add some scheduled timeout as well? or should we rely on the KMS to timeout appropriately.
        return keyContextCache.get(kekId);
    }

    private CompletableFuture<KeyContext> makeKeyContext(@NonNull K kekId) {
        return kms.generateDekPair(kekId)
                .thenApply(dekPair -> {
                    E edek = dekPair.edek();
                    short edekSize = (short) edekSerde.sizeOf(edek);
                    ByteBuffer serializedEdek = ByteBuffer.allocate(edekSize);
                    edekSerde.serialize(edek, serializedEdek);
                    serializedEdek.flip();

                    return new KeyContext(serializedEdek,
                            System.nanoTime() + dekTtlNanos,
                            maxEncryptionsPerDek,
                            // Either we have a different Aes encryptor for each thread
                            // or we need mutex
                            // or we externalize the state
                            AesGcmEncryptor.forEncrypt(new AesGcmIvGenerator(new SecureRandom()), dekPair.dek()));
                }).toCompletableFuture();
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

        List<Integer> batchRecordCounts = batchRecordCounts(records);
        // it is possible to encounter MemoryRecords that have had all their records compacted away, but
        // the recordbatch metadata still exists. https://kafka.apache.org/documentation/#recordbatch
        if (batchRecordCounts.stream().allMatch(size -> size == 0)) {
            return CompletableFuture.completedFuture(records);
        }
        return attemptEncrypt(topicName, partition, encryptionScheme, records, 0, batchRecordCounts, bufferAllocator)
                .thenApply(BatchAwareMemoryRecordsBuilder::build);
    }

    @NonNull
    private static List<Integer> batchRecordCounts(@NonNull MemoryRecords records) {
        List<Integer> sizes = new ArrayList<>();
        for (MutableRecordBatch batch : records.batches()) {
            sizes.add(recordCount(batch));
        }
        return sizes;
    }

    private static int recordCount(MutableRecordBatch batch) {
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

    private ByteBufferOutputStream allocateBufferForEncode(MemoryRecords records, IntFunction<ByteBufferOutputStream> bufferAllocator) {
        int sizeEstimate = 2 * records.sizeInBytes();
        // Accurate estimation is tricky without knowing the record sizes
        return bufferAllocator.apply(sizeEstimate);
    }

    @SuppressWarnings("java:S2445")
    private CompletionStage<BatchAwareMemoryRecordsBuilder> attemptEncrypt(String topicName,
                                                                           int partition,
                                                                           @NonNull EncryptionScheme<K> encryptionScheme,
                                                                           @NonNull MemoryRecords records,
                                                                           int attempt,
                                                                           List<Integer> batchRecordCounts,
                                                                           @NonNull IntFunction<ByteBufferOutputStream> bufferAllocator) {
        int allRecordsCount = batchRecordCounts.stream().mapToInt(value -> value).sum();
        if (attempt >= MAX_ATTEMPTS) {
            return CompletableFuture.failedFuture(
                    new RequestNotSatisfiable("failed to reserve an EDEK to encrypt " + allRecordsCount + " records for topic " + topicName + " partition "
                            + partition + " after " + attempt + " attempts"));
        }
        return currentDekContext(encryptionScheme.kekId()).thenCompose(keyContext -> {
            synchronized (keyContext) {
                // if it's not alive we know a previous encrypt call has removed this stage from the cache and fall through to retry encrypt
                if (!keyContext.isDestroyed()) {
                    if (!keyContext.hasAtLeastRemainingEncryptions(allRecordsCount)) {
                        // remove the key context from the cache, then call encrypt again to drive caffeine to recreate it
                        rotateKeyContext(encryptionScheme, keyContext);
                    }
                    else {
                        try {
                            BatchAwareMemoryRecordsBuilder encrypt = encryptBatches(encryptionScheme, records, keyContext, bufferAllocator);
                            return CompletableFuture.completedFuture(encrypt);
                        }
                        catch (Exception e) {
                            return CompletableFuture.failedFuture(e);
                        }
                    }
                }
            }
            return attemptEncrypt(topicName, partition, encryptionScheme, records, attempt + 1, batchRecordCounts, bufferAllocator);
        });
    }

    @NonNull
    private BatchAwareMemoryRecordsBuilder encryptBatches(@NonNull EncryptionScheme<K> encryptionScheme,
                                                          @NonNull MemoryRecords memoryRecords,
                                                          @NonNull KeyContext keyContext,
                                                          @NonNull IntFunction<ByteBufferOutputStream> bufferAllocator) {
        BatchAwareMemoryRecordsBuilder builder = new BatchAwareMemoryRecordsBuilder(allocateBufferForEncode(memoryRecords, bufferAllocator));
        for (MutableRecordBatch batch : memoryRecords.batches()) {
            maybeEncryptBatch(encryptionScheme, keyContext, batch, builder);
        }
        return builder;
    }

    private void maybeEncryptBatch(@NonNull EncryptionScheme<K> encryptionScheme, @NonNull KeyContext keyContext, MutableRecordBatch batch,
                                   BatchAwareMemoryRecordsBuilder builder) {
        if (batch.isControlBatch()) {
            builder.writeBatch(batch);
        }
        else {
            List<Record> records = StreamSupport.stream(batch.spliterator(), false).toList();
            if (records.isEmpty()) {
                builder.writeBatch(batch);
            }
            else {
                var maxParcelSize = records.stream()
                        .mapToInt(kafkaRecord -> Parcel.sizeOfParcel(
                                encryptionVersion.parcelVersion(),
                                encryptionScheme.recordFields(),
                                kafkaRecord))
                        .filter(value -> value > 0)
                        .max()
                        .orElse(0);
                var maxWrapperSize = records.stream()
                        .mapToInt(kafkaRecord -> sizeOfWrapper(keyContext, maxParcelSize))
                        .filter(value -> value > 0)
                        .max()
                        .orElse(0);
                ByteBuffer parcelBuffer = bufferPool.acquire(maxParcelSize);
                ByteBuffer wrapperBuffer = bufferPool.acquire(maxWrapperSize);
                try {
                    RecordBatchUtils.toMemoryRecords(batch,
                            new RecordEncryptor<>(encryptionVersion, encryptionScheme, keyContext, parcelBuffer, wrapperBuffer),
                            builder);
                }
                finally {
                    if (wrapperBuffer != null) {
                        bufferPool.release(wrapperBuffer);
                    }
                    if (parcelBuffer != null) {
                        bufferPool.release(parcelBuffer);
                    }
                }
                keyContext.recordEncryptions(records.size());
            }
        }
    }

    // this must only be called while holding the lock on this keycontext
    private void rotateKeyContext(@NonNull EncryptionScheme<K> encryptionScheme, KeyContext keyContext) {
        keyContext.destroy();
        K kekId = encryptionScheme.kekId();
        keyContextCache.synchronous().invalidate(kekId);
    }

    private int sizeOfWrapper(KeyContext keyContext, int parcelSize) {
        var edek = keyContext.serializedEdek();
        return ByteUtils.sizeOfUnsignedVarint(edek.length)
                + edek.length
                + 1 // aad code
                + 1 // cipher code
                + keyContext.encodedSize(parcelSize);

    }

    /**
     * Reads the {@link #ENCRYPTION_HEADER_NAME} header from the record.
     * @param topicName The topic name.
     * @param partition The partition.
     * @param kafkaRecord The record.
     * @return The encryption header, or null if it's missing (indicating that the record wasn't encrypted).
     */
    static EncryptionVersion decryptionVersion(String topicName, int partition, Record kafkaRecord) {
        for (Header header : kafkaRecord.headers()) {
            if (ENCRYPTION_HEADER_NAME.equals(header.key())) {
                byte[] value = header.value();
                if (value.length != 1) {
                    throw new EncryptionException("Invalid value for header with key '" + ENCRYPTION_HEADER_NAME + "' "
                            + "in record at offset " + kafkaRecord.offset()
                            + " in partition " + partition
                            + " of topic " + topicName);
                }
                return EncryptionVersion.fromCode(value[0]);
            }
        }
        return null;
    }

    private CompletableFuture<AesGcmEncryptor> makeDecryptor(E edek) {
        return kms.decryptEdek(edek)
                .thenApply(AesGcmEncryptor::forDecrypt).toCompletableFuture();
    }

    @NonNull
    @Override
    public CompletionStage<MemoryRecords> decrypt(@NonNull String topicName, int partition, @NonNull MemoryRecords records,
                                                  @NonNull IntFunction<ByteBufferOutputStream> bufferAllocator) {
        if (records.sizeInBytes() == 0) {
            // no records to transform, return input without modification
            return CompletableFuture.completedFuture(records);
        }
        List<Integer> batchRecordCounts = batchRecordCounts(records);
        // it is possible to encounter MemoryRecords that have had all their records compacted away, but
        // the recordbatch metadata still exists. https://kafka.apache.org/documentation/#recordbatch
        if (batchRecordCounts.stream().allMatch(recordCount -> recordCount == 0)) {
            return CompletableFuture.completedFuture(records);
        }
        Set<E> uniqueEdeks = extractEdeks(topicName, partition, records);
        CompletionStage<Map<E, AesGcmEncryptor>> decryptors = resolveAll(uniqueEdeks);
        CompletionStage<BatchAwareMemoryRecordsBuilder> decryptStage = decryptors.thenApply(
                encryptorMap -> decrypt(topicName, partition, records, new BatchAwareMemoryRecordsBuilder(allocateBufferForDecode(records, bufferAllocator)),
                        encryptorMap, batchRecordCounts));
        return decryptStage.thenApply(BatchAwareMemoryRecordsBuilder::build);
    }

    private CompletionStage<Map<E, AesGcmEncryptor>> resolveAll(Set<E> uniqueEdeks) {
        CompletionStage<List<Map.Entry<E, AesGcmEncryptor>>> join = EnvelopeEncryptionFilter.join(
                uniqueEdeks.stream().map(e -> decryptorCache.get(e).thenApply(aesGcmEncryptor -> Map.entry(e, aesGcmEncryptor))).toList());
        return join.thenApply(entries -> entries.stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    private Set<E> extractEdeks(String topicName, int partition, MemoryRecords records) {
        Set<E> edeks = new HashSet<>();
        Serde<E> serde = kms.edekSerde();
        for (Record kafkaRecord : records.records()) {
            var decryptionVersion = decryptionVersion(topicName, partition, kafkaRecord);
            if (decryptionVersion == EncryptionVersion.V1) {
                ByteBuffer wrapper = kafkaRecord.value();
                var edekLength = ByteUtils.readUnsignedVarint(wrapper);
                ByteBuffer slice = wrapper.slice(wrapper.position(), edekLength);
                var edek = serde.deserialize(slice);
                edeks.add(edek);
            }
        }
        return edeks;
    }

    @NonNull
    private BatchAwareMemoryRecordsBuilder decrypt(String topicName,
                                                   int partition,
                                                   @NonNull MemoryRecords records,
                                                   @NonNull BatchAwareMemoryRecordsBuilder builder,
                                                   @NonNull Map<E, AesGcmEncryptor> encryptorMap,
                                                   @NonNull List<Integer> batchRecordCounts) {
        int i = 0;
        for (MutableRecordBatch batch : records.batches()) {
            Integer batchRecordCount = batchRecordCounts.get(i++);
            if (batchRecordCount == 0 || batch.isControlBatch()) {
                builder.writeBatch(batch);
            }
            else {
                decryptBatch(topicName, partition, builder, encryptorMap, batch);
            }
        }
        return builder;
    }

    private void decryptBatch(String topicName, int partition, @NonNull BatchAwareMemoryRecordsBuilder builder, @NonNull Map<E, AesGcmEncryptor> encryptorMap,
                              MutableRecordBatch batch) {
        builder.addBatchLike(batch);
        for (Record kafkaRecord : batch) {
            var decryptionVersion = decryptionVersion(topicName, partition, kafkaRecord);
            if (decryptionVersion == null) {
                builder.append(kafkaRecord);
            }
            else if (decryptionVersion == EncryptionVersion.V1) {
                ByteBuffer wrapper = kafkaRecord.value();
                var edekLength = ByteUtils.readUnsignedVarint(wrapper);
                ByteBuffer slice = wrapper.slice(wrapper.position(), edekLength);
                var edek = edekSerde.deserialize(slice);
                wrapper.position(wrapper.position() + edekLength);
                AesGcmEncryptor aesGcmEncryptor = encryptorMap.get(edek);
                if (aesGcmEncryptor == null) {
                    throw new EncryptionException("no encryptor loaded for edek, " + edek);
                }
                decryptRecord(EncryptionVersion.V1, aesGcmEncryptor, wrapper, kafkaRecord, builder);
            }
        }
    }

    private ByteBufferOutputStream allocateBufferForDecode(MemoryRecords memoryRecords, IntFunction<ByteBufferOutputStream> allocator) {
        int sizeEstimate = memoryRecords.sizeInBytes();
        return allocator.apply(sizeEstimate);
    }

    @SuppressWarnings("java:S2445")
    private void decryptRecord(EncryptionVersion decryptionVersion,
                               AesGcmEncryptor encryptor,
                               ByteBuffer wrapper,
                               Record kafkaRecord,
                               @NonNull BatchAwareMemoryRecordsBuilder builder) {
        var aadSpec = AadSpec.fromCode(wrapper.get());
        ByteBuffer aad = switch (aadSpec) {
            case NONE -> ByteUtils.EMPTY_BUF;
        };

        var cipherCode = CipherCode.fromCode(wrapper.get());

        ByteBuffer plaintextParcel;
        synchronized (encryptor) {
            plaintextParcel = decryptParcel(wrapper.slice(), encryptor);
        }
        Parcel.readParcel(decryptionVersion.parcelVersion(), plaintextParcel, kafkaRecord, (v, h) -> {
            builder.appendWithOffset(kafkaRecord.offset(), kafkaRecord.timestamp(), kafkaRecord.key(), v, h);
        });
    }

    private ByteBuffer decryptParcel(ByteBuffer ciphertextParcel, AesGcmEncryptor encryptor) {
        ByteBuffer plaintext = ciphertextParcel.duplicate();
        encryptor.decrypt(ciphertextParcel, plaintext);
        plaintext.flip();
        return plaintext;
    }

}
