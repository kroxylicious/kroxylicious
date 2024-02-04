/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ByteUtils;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.kroxylicious.filter.encryption.AadSpec;
import io.kroxylicious.filter.encryption.CipherCode;
import io.kroxylicious.filter.encryption.DecryptionManager;
import io.kroxylicious.filter.encryption.EncryptionException;
import io.kroxylicious.filter.encryption.EncryptionManager;
import io.kroxylicious.filter.encryption.EncryptionVersion;
import io.kroxylicious.filter.encryption.EnvelopeEncryptionFilter;
import io.kroxylicious.filter.encryption.records.BatchAwareMemoryRecordsBuilder;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.Serde;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An implementation of {@link EncryptionManager} and {@link DecryptionManager}
 * that uses envelope encryption, AES-GCM and stores the KEK id and encrypted DEK
 * alongside the record ("in-band").
 * @param <K> The type of KEK id.
 * @param <E> The type of the encrypted DEK.
 */
public class InBandDecryptionManager<K, E> implements DecryptionManager {

    /**
     * The encryption header. The value is the encryption version that was used to serialize the parcel and the wrapper.
     */
    static final String ENCRYPTION_HEADER_NAME = "kroxylicious.io/encryption";

    private final AsyncLoadingCache<E, AesGcmEncryptor> decryptorCache;
    private final Kms<K, E> kms;
    private final Serde<E> edekSerde;

    public InBandDecryptionManager(Kms<K, E> kms) {
        this.kms = kms;
        this.edekSerde = kms.edekSerde();
        this.decryptorCache = Caffeine.newBuilder()
                .buildAsync((edek, executor) -> makeDecryptor(edek));
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
        List<Integer> batchRecordCounts = InBandEncryptionManager.batchRecordCounts(records);
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
