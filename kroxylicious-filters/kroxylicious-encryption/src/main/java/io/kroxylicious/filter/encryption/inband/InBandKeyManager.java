/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.filter.encryption.EncryptionScheme;
import io.kroxylicious.filter.encryption.KeyManager;
import io.kroxylicious.filter.encryption.Receiver;
import io.kroxylicious.filter.encryption.RecordField;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.Serde;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * An implementation of {@link KeyManager} that uses envelope encryption, AES-GCM and stores the KEK id and encrypted DEK
 * alongside the record ("in-band").
 * @param <K> The type of KEK id.
 * @param <E> The type of the encrypted DEK.
 */
public class InBandKeyManager<K, E> implements KeyManager<K> {

    private static final Logger LOGGER = LoggerFactory.getLogger(InBandKeyManager.class);
    private static final Map<Class<? extends Destroyable>, Boolean> LOGGED_DESTROY_FAILED = new ConcurrentHashMap<>();

    private static final String FIELDS_HEADER_NAME = "kroxylicious.io/encrypted";
    private static final String DEK_HEADER_NAME = "kroxylicious.io/dek";

    private final Kms<K, E> kms;
    private final BufferPool bufferPool;
    private final Serde<K> kekIdSerde;
    private final Serde<E> edekSerde;
    // TODO cache expiry, with key descruction
    private final ConcurrentHashMap<K, CompletionStage<KeyContext>> keyContextCache;
    private final ConcurrentHashMap<RecordHeader, CompletionStage<AesGcmEncryptor>> decryptorCache;
    private final long dekTtlNanos;
    private final int maxEncryptionsPerDek;

    public InBandKeyManager(Kms<K, E> kms,
                            BufferPool bufferPool) {
        this.kms = kms;
        this.bufferPool = bufferPool;
        this.edekSerde = kms.edekSerde();
        this.kekIdSerde = kms.keyIdSerde();
        this.dekTtlNanos = 5_000_000_000L;
        this.maxEncryptionsPerDek = 500_000;
        // TODO This ^^ must be > the maximum size of a batch to avoid an infinite loop
        this.keyContextCache = new ConcurrentHashMap<>();
        this.decryptorCache = new ConcurrentHashMap<>();
    }

    private CompletionStage<KeyContext> getKeyContext(K key,
                                                      Supplier<CompletionStage<KeyContext>> valueSupplier) {
        return keyContextCache.compute(key, (k, v) -> {
            if (v == null) {
                return valueSupplier.get();
                // TODO what happens if the CS complete exceptionally
                // TODO what happens if the CS doesn't complete at all in a reasonably time frame?
            }
            else {
                return v;
            }
        });
    }

    private CompletionStage<KeyContext> currentDekContext(@NonNull K kekId, int numRecords) {
        return getKeyContext(kekId, makeKeyContext(kekId))
                .thenCompose(cachedContext -> {
                    if (!cachedContext.isExpiredForEncryption(System.nanoTime())
                            && cachedContext.hasAtLeastRemainingEncryptions(numRecords)) {
                        // TODO pre-populate decryptorCache?
                        return CompletableFuture.completedFuture(cachedContext);
                    }
                    else {
                        destroy(cachedContext);
                        return currentDekContext(kekId, numRecords);
                    }
                });
    }

    static void destroy(Destroyable destroyable) {
        try {
            destroyable.destroy();
        }
        catch (DestroyFailedException e) {
            var cls = destroyable.getClass();
            LOGGED_DESTROY_FAILED.compute(cls, (k, logged) -> {
                if (logged == null) {
                    LOGGER.warn("Failed to destroy an instance of {}. "
                            + "Note: this message is logged once per class even though there may be many occurrences of this event. "
                            + "This event can happen because the JRE's SecretKeySpec class does not override destroy().",
                            cls, e);
                }
                return Boolean.TRUE;
            });
        }
    }

    private Supplier<CompletionStage<KeyContext>> makeKeyContext(@NonNull K kekId) {
        return () -> kms.generateDekPair(kekId)
                .thenApply(dekPair -> {
                    E edek = dekPair.edek();
                    short kekIdSize = (short) kekIdSerde.sizeOf(kekId);
                    short edekSize = (short) edekSerde.sizeOf(edek);
                    ByteBuffer prefix = bufferPool.acquire(
                            Short.BYTES + // kekId size
                                    kekIdSize + // the kekId
                                    Short.BYTES + // DEK size
                                    edekSize); // the DEK
                    prefix.putShort(kekIdSize);
                    kekIdSerde.serialize(kekId, prefix);
                    prefix.putShort(edekSize);
                    edekSerde.serialize(edek, prefix);
                    prefix.flip();

                    return new KeyContext(prefix,
                            System.nanoTime() + dekTtlNanos,
                            maxEncryptionsPerDek,
                            // Either we have a different Aes encryptor for each thread
                            // or we need mutex
                            // or we externalize the state
                            AesGcmEncryptor.forEncrypt(new AesGcmIvGenerator(new SecureRandom()), dekPair.dek()));
                });
    }

    @NonNull
    @Override
    @SuppressWarnings("java:S2445")
    public CompletionStage<Void> encrypt(@NonNull EncryptionScheme<K> encryptionScheme,
                                         @NonNull List<? extends Record> records,
                                         @NonNull Receiver receiver) {

        var fieldsHeader = createEncryptedFieldsHeader(encryptionScheme.recordFields());
        return currentDekContext(encryptionScheme.kekId(), records.size()).thenAccept(keyContext -> {
            var dekHeader = createEdekHeader(keyContext);
            var maxValuePlaintextSize = encryptionScheme.recordFields().contains(RecordField.RECORD_VALUE)
                    ? records.stream().mapToInt(Record::valueSize).max().orElse(-1)
                    : -1;
            synchronized (keyContext) {
                var valueCiphertext = maxValuePlaintextSize >= 0 ? bufferPool.acquire(keyContext.encodedSize(maxValuePlaintextSize)) : null;
                try {
                    encryptRecords(encryptionScheme, keyContext, records, fieldsHeader, dekHeader, valueCiphertext, receiver);
                }
                finally {
                    if (maxValuePlaintextSize >= 0) {
                        bufferPool.release(valueCiphertext);
                    }
                }
            }
        });
    }

    private void encryptRecords(@NonNull EncryptionScheme<K> encryptionScheme,
                                @NonNull KeyContext keyContext,
                                @NonNull List<? extends Record> records,
                                @NonNull RecordHeader fieldsHeader,
                                @NonNull RecordHeader dekHeader,
                                ByteBuffer valueCiphertext,
                                @NonNull Receiver receiver) {
        records.forEach(kafkaRecord -> {
            ByteBuffer transformedValue;
            Header[] headers = kafkaRecord.headers();
            if (encryptionScheme.recordFields().contains(RecordField.RECORD_VALUE)) {
                transformedValue = encryptRecordValue(keyContext, kafkaRecord, valueCiphertext);
            }
            else {
                transformedValue = kafkaRecord.value();
            }
            if (encryptionScheme.recordFields().contains(RecordField.RECORD_HEADER_VALUES)) {
                for (int i = 0; i < headers.length; i++) {
                    headers[i] = encryptRecordHeaderValue(keyContext, headers[i]);
                }
            }
            Header[] transformedHeaders = prependToHeaders(headers, fieldsHeader, dekHeader);
            receiver.accept(kafkaRecord, transformedValue, transformedHeaders);

            if (valueCiphertext != null) {
                valueCiphertext.rewind();
            }
        });
    }

    private Header encryptRecordHeaderValue(KeyContext keyContext, Header header) {
        byte[] headerValue = header.value();
        ByteBuffer plaintext = ByteBuffer.wrap(headerValue);
        var ciphertext = ByteBuffer.allocate(keyContext.encodedSize(headerValue.length));
        keyContext.encode(plaintext, ciphertext);
        ciphertext.flip();
        return new RecordHeader(header.key(), ciphertext.array());
    }

    @Nullable
    private ByteBuffer encryptRecordValue(KeyContext keyContext,
                                          Record kafkaRecord,
                                          ByteBuffer valueCiphertext) {
        ByteBuffer transformedValue;
        if (!kafkaRecord.hasValue()) {
            transformedValue = null;
        }
        else {
            ByteBuffer plaintext = kafkaRecord.value();
            keyContext.encodedSize(kafkaRecord.valueSize());
            keyContext.encode(plaintext, valueCiphertext);
            valueCiphertext.flip();
            transformedValue = valueCiphertext;
        }
        return transformedValue;
    }

    static @NonNull RecordHeader createEncryptedFieldsHeader(@NonNull Set<RecordField> recordFields) {
        return new RecordHeader(FIELDS_HEADER_NAME, new byte[]{ RecordField.toBits(recordFields) });
    }

    static @NonNull RecordHeader createEdekHeader(@NonNull KeyContext keyContext) {
        return new RecordHeader(DEK_HEADER_NAME, keyContext.prefix());
    }

    @NonNull
    static Header[] prependToHeaders(Header[] oldHeaders, @NonNull RecordHeader... additionalHeaders) {
        if (additionalHeaders.length == 0) {
            return oldHeaders;
        }
        if (oldHeaders == null || oldHeaders.length == 0) {
            return additionalHeaders;
        }
        Header[] newHeaders = new Header[oldHeaders.length + additionalHeaders.length];
        System.arraycopy(additionalHeaders, 0, newHeaders, 0, additionalHeaders.length);
        System.arraycopy(oldHeaders, 0, newHeaders, additionalHeaders.length, oldHeaders.length);
        return newHeaders;
    }

    @NonNull
    static Header[] removeInitialHeaders(@NonNull Header[] oldHeaders, int numToRemove) {
        if (numToRemove < 0) {
            throw new IllegalArgumentException();
        }
        Header[] newHeaders = new Header[oldHeaders.length - numToRemove];
        if (newHeaders.length > 0) {
            System.arraycopy(oldHeaders, numToRemove, newHeaders, 0, newHeaders.length);
        }
        return newHeaders;
    }

    static Set<RecordField> encryptedFields(Record kafkaRecord) {
        for (Header header : kafkaRecord.headers()) {
            if (FIELDS_HEADER_NAME.equals(header.key())) {
                return RecordField.fromBits(header.value()[0]);
            }
        }
        return EnumSet.noneOf(RecordField.class);
    }

    static RecordHeader dek(Record kafkaRecord) {
        for (Header header : kafkaRecord.headers()) {
            if (DEK_HEADER_NAME.equals(header.key())) {
                return (RecordHeader) header;
            }
        }
        throw new IllegalStateException();
    }

    private CompletionStage<AesGcmEncryptor> getOrCacheDecryptor(RecordHeader dekHeader,
                                                                 K kekId,
                                                                 E edek) {
        return decryptorCache.compute(dekHeader, (k, v) -> {
            if (v == null) {
                return kms.decryptEdek(kekId, edek)
                        .thenApply(AesGcmEncryptor::forDecrypt).toCompletableFuture();
                // TODO what happens if the CS complete exceptionally
                // TODO what happens if the CS doesn't complete at all in a reasonably time frame?
            }
            else {
                return v;
            }
        });
    }

    @NonNull
    @Override
    public CompletionStage<Void> decrypt(@NonNull List<? extends Record> records,
                                         @NonNull Receiver receiver) {
        List<CompletionStage<Void>> futures = new ArrayList<>(records.size());
        for (var kafkaRecord : records) {
            var encryptedFields = encryptedFields(kafkaRecord);
            if (encryptedFields.isEmpty()) {
                receiver.accept(kafkaRecord, kafkaRecord.value(), kafkaRecord.headers());
                futures.add(CompletableFuture.completedFuture(null));
            }
            else {
                // right now (because we only support topic name based kek selection) noce we've resolve the first value we
                // can keep the lock and process all the records
                var x = resolveEncryptor(kafkaRecord).thenAccept(encryptor -> {
                    decryptRecord(receiver, kafkaRecord, encryptor, encryptedFields);

                });
                futures.add(x);
            }
        }

        return io.kroxylicious.filter.encryption.EnvelopeEncryptionFilter.join(futures).thenAccept(list -> {
        });
    }

    @SuppressWarnings("java:S2445")
    private void decryptRecord(@NonNull Receiver receiver, Record kafkaRecord, AesGcmEncryptor encryptor, Set<RecordField> encryptedFields) {
        ByteBuffer decryptedValue;
        var headers = removeInitialHeaders(kafkaRecord.headers(), 2);
        synchronized (encryptor) {
            if (encryptedFields.contains(RecordField.RECORD_VALUE)) {
                decryptedValue = decryptRecordValue(kafkaRecord, encryptor);
            }
            else {
                decryptedValue = kafkaRecord.value();
            }
            if (encryptedFields.contains(RecordField.RECORD_HEADER_VALUES)) {
                for (int i = 0; i < headers.length; i++) {
                    headers[i] = decryptRecordHeader(headers[i], encryptor);
                }
            }
        }
        receiver.accept(kafkaRecord, decryptedValue, headers);
    }

    private CompletionStage<AesGcmEncryptor> resolveEncryptor(Record kafkaRecord) {
        var dekHeader = dek(kafkaRecord);
        var buffer = ByteBuffer.wrap(dekHeader.value());
        var kekLength = buffer.getShort();
        int origLimit = buffer.limit();
        buffer.limit(buffer.position() + kekLength);
        var kekId = kekIdSerde.deserialize(buffer);
        buffer.limit(origLimit);
        var edekLength = buffer.getShort();
        buffer.limit(buffer.position() + edekLength);
        var edek = edekSerde.deserialize(buffer);
        buffer.rewind();
        return getOrCacheDecryptor(dekHeader, kekId, edek);
    }

    private Header decryptRecordHeader(Header header, AesGcmEncryptor encryptor) {
        var ciphertext = ByteBuffer.wrap(header.value());
        var plaintext = ciphertext.duplicate();
        encryptor.decrypt(ciphertext, plaintext);
        plaintext.flip();
        byte[] value = new byte[plaintext.limit()];
        plaintext.get(value);
        return new RecordHeader(header.key(), value);
    }

    private ByteBuffer decryptRecordValue(Record kafkaRecord, AesGcmEncryptor encryptor) {
        if (!kafkaRecord.hasValue()) {
            return kafkaRecord.value();
        }
        else {
            ByteBuffer ciphertext = kafkaRecord.value();
            ByteBuffer plaintext = ciphertext.duplicate();
            encryptor.decrypt(ciphertext, plaintext);
            plaintext.flip();
            return plaintext;
        }
    }

}
