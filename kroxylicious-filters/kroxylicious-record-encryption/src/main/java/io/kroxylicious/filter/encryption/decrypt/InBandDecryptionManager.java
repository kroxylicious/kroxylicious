/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.decrypt;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.IntFunction;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import io.kroxylicious.filter.encryption.common.EncryptionException;
import io.kroxylicious.filter.encryption.common.FilterThreadExecutor;
import io.kroxylicious.filter.encryption.common.RecordEncryptionUtil;
import io.kroxylicious.filter.encryption.crypto.Encryption;
import io.kroxylicious.filter.encryption.crypto.EncryptionHeader;
import io.kroxylicious.filter.encryption.crypto.EncryptionResolver;
import io.kroxylicious.filter.encryption.dek.Dek;
import io.kroxylicious.filter.encryption.dek.DekManager;
import io.kroxylicious.kafka.transform.RecordStream;
import io.kroxylicious.kms.service.Serde;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * An implementation of {@link DecryptionManager}
 * that uses envelope encryption, AES-GCM and stores the KEK id and encrypted DEK
 * alongside the record ("in-band").
 * @param <K> The type of KEK id.
 * @param <E> The type of the encrypted DEK.
 */
public class InBandDecryptionManager<K, E> implements DecryptionManager {

    private final DekManager<K, E> dekManager;
    private final FilterThreadExecutor filterThreadExecutor;
    private final DecryptionDekCache<K, E> dekCache;

    private final EncryptionResolver encryptionResolver;

    public InBandDecryptionManager(
            EncryptionResolver encryptionResolver,
            @NonNull
            DekManager<K, E> dekManager,
            @NonNull
            DecryptionDekCache<K, E> dekCache,
            @Nullable
            FilterThreadExecutor filterThreadExecutor
    ) {
        this.encryptionResolver = encryptionResolver;
        this.dekManager = Objects.requireNonNull(dekManager);
        this.dekCache = Objects.requireNonNull(dekCache);
        this.filterThreadExecutor = filterThreadExecutor;
    }

    /**
     * Reads the {@link EncryptionHeader#ENCRYPTION_HEADER_NAME} header from the record.
     * @param topicName The topic name.
     * @param partition The partition.
     * @param kafkaRecord The record.
     * @return The encryption header, or null if it's missing (indicating that the record wasn't encrypted).
     */
    Encryption decryptionVersion(
            @NonNull
            String topicName,
            int partition,
            @NonNull
            Record kafkaRecord
    ) {
        for (Header header : kafkaRecord.headers()) {
            if (EncryptionHeader.ENCRYPTION_HEADER_NAME.equals(header.key())) {
                byte[] value = header.value();
                if (value.length != 1) {
                    throw new EncryptionException(
                            "Invalid value for header with key '"
                                                  + EncryptionHeader.ENCRYPTION_HEADER_NAME
                                                  + "' "
                                                  + "in record at offset "
                                                  + kafkaRecord.offset()
                                                  + " in partition "
                                                  + partition
                                                  + " of topic "
                                                  + topicName
                    );
                }
                return encryptionResolver.fromSerializedId(value[0]);
            }
        }
        return null;
    }

    @NonNull
    @Override
    public CompletionStage<MemoryRecords> decrypt(
            @NonNull
            String topicName,
            int partition,
            @NonNull
            MemoryRecords records,
            @NonNull
            IntFunction<ByteBufferOutputStream> bufferAllocator
    ) {
        if (records.sizeInBytes() == 0) {
            // no records to transform, return input without modification
            return CompletableFuture.completedFuture(records);
        }
        int totalRecords = RecordEncryptionUtil.totalRecordsInBatches(records);
        // it is possible to encounter MemoryRecords that have had all their records compacted away, but
        // the recordbatch metadata still exists. https://kafka.apache.org/documentation/#recordbatch
        if (totalRecords == 0) {
            return CompletableFuture.completedFuture(records);
        }

        CompletionStage<List<DecryptState<E>>> decryptStates = resolveAll(topicName, partition, records);
        return decryptStates.thenApply(
                decryptStateList -> {
                    try {
                        return decrypt(
                                topicName,
                                partition,
                                records,
                                decryptStateList,
                                allocateBufferForDecrypt(records, bufferAllocator)
                        );
                    }
                    finally {
                        for (var ds : decryptStateList) {
                            if (ds != null && ds.decryptor() != null) {
                                ds.decryptor().close();
                            }
                        }
                    }
                }
        );
    }

    /**
     * Gets each record's encryption header (any any) and then resolves those edeks into
     * {@link io.kroxylicious.filter.encryption.dek.Dek.Decryptor}s via the {@link #dekManager}
     * @param topicName The topic name.
     * @param partition The partition.
     * @param records The records to decrypt.
     * @return A stage that completes with a list of the DecryptState
     * for each record in the given {@code records}, in the same order.
     */
    private CompletionStage<List<DecryptState<E>>> resolveAll(
            String topicName,
            int partition,
            MemoryRecords records
    ) {
        Serde<E> serde = dekManager.edekSerde();
        // Use a pair of lists because we end up wanting a `List<DecryptState>`,
        // indexed by the position of the record in the multi-batch MemoryRecords,
        // to pass to `RecordStream`, which avoids needing to use a `Record`
        // itself as a hash key.
        var cacheKeys = new ArrayList<DecryptionDekCache.CacheKey<E>>();
        var states = new ArrayList<DecryptState<E>>();

        // Iterate the records collecting cache keys and decrypt states
        // both cacheKeys and decryptStates use the list index as a way of identifying the corresponding
        // record: The index in the list is the same as their index within the MemoryRecords
        RecordStream.ofRecords(records).forEachRecord((batch, record, ignored) -> {
            var decryptionVersion = decryptionVersion(topicName, partition, record);
            if (decryptionVersion != null) {
                ByteBuffer wrapper = record.value();
                cacheKeys.add(decryptionVersion.wrapper().readSpecAndEdek(wrapper, serde, DecryptionDekCache.CacheKey::new));
                states.add(new DecryptState<>(decryptionVersion));
            } else {
                // It's not encrypted, so use sentinels
                cacheKeys.add(DecryptionDekCache.CacheKey.unencrypted());
                states.add(DecryptState.none());
            }
        });
        // Lookup the decryptors for the cache keys
        return filterThreadExecutor.completingOnFilterThread(dekCache.getAll(cacheKeys, filterThreadExecutor))
                                   .thenApply(cacheKeyDecryptorMap ->
                                   // Once we have the decryptors from the cache...
                                   issueDecryptors(cacheKeyDecryptorMap, cacheKeys, states));
    }

    private @NonNull List<DecryptState<E>> issueDecryptors(
            @NonNull
            Map<DecryptionDekCache.CacheKey<E>, Dek<E>> cacheKeyDecryptorMap,
            @NonNull
            List<DecryptionDekCache.CacheKey<E>> cacheKeys,
            @NonNull
            List<DecryptState<E>> states
    ) {
        Map<DecryptionDekCache.CacheKey<E>, Dek<E>.Decryptor> issuedDecryptors = new HashMap<>();
        try {
            for (int index = 0, cacheKeysSize = cacheKeys.size(); index < cacheKeysSize; index++) {
                DecryptionDekCache.CacheKey<E> cacheKey = cacheKeys.get(index);
                // ...update (in place) the DecryptState with the decryptor
                var decryptor = issuedDecryptors.computeIfAbsent(cacheKey, k -> {
                    Dek<E> dek = cacheKeyDecryptorMap.get(cacheKey);
                    return dek != null ? dek.decryptor() : null;
                });
                states.get(index).withDecryptor(decryptor);
            }
            // return the resolved DecryptStates
            return states;
        }
        catch (RuntimeException e) {
            issuedDecryptors.forEach((cacheKey, decryptor) -> {
                if (decryptor != null) {
                    decryptor.close();
                }
            });
            throw e;
        }
    }

    private static ByteBufferOutputStream allocateBufferForDecrypt(
            MemoryRecords memoryRecords,
            IntFunction<ByteBufferOutputStream> allocator
    ) {
        int sizeEstimate = memoryRecords.sizeInBytes();
        return allocator.apply(sizeEstimate);
    }

    /**
     * Fill the given {@code buffer} with the {@code records},
     * decrypting any which are encrypted using the corresponding decryptor from the given
     * {@code decryptorList}.
     * @param records The records to decrypt.
     * @param decryptorList The decryptors to use.
     * @param buffer The buffer to fill (to encourage buffer reuse).
     * @return The decrypted records.
     */
    @NonNull
    private MemoryRecords decrypt(
            @NonNull
            String topicName,
            int partition,
            @NonNull
            MemoryRecords records,
            @NonNull
            List<DecryptState<E>> decryptorList,
            @NonNull
            ByteBufferOutputStream buffer
    ) {
        return RecordStream.ofRecordsWithIndex(records)
                           .mapPerRecord((batch, record, index) -> decryptorList.get(index))
                           .toMemoryRecords(
                                   buffer,
                                   new RecordDecryptor<>(topicName, partition)
                           );
    }
}
