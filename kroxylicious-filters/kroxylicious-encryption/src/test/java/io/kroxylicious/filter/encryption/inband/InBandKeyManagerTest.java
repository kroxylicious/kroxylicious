/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.encryption.EncryptionScheme;
import io.kroxylicious.filter.encryption.Receiver;
import io.kroxylicious.filter.encryption.RecordField;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryKms;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.UnitTestingKmsService;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InBandKeyManagerTest {

    @Test
    void shouldBeAbleToDependOnRecordHeaderEquality() {
        // The InBandKeyManager relies internally on RecordHeader implementing equals
        // Since it's Kafka's class let's validate that here
        var rh = new RecordHeader("foo", new byte[]{ 7, 4, 1 });
        var rh2 = new RecordHeader("foo", new byte[]{ 7, 4, 1 });
        var rh3 = new RecordHeader("bar", new byte[]{ 3, 3 });

        assertEquals(rh, rh2);
        assertNotEquals(rh, rh3);
        assertNotEquals(rh2, rh3);
    }

    @Test
    void headers() {
        var myHeader = new RecordHeader("mine", new byte[]{ 1 });
        var nonEmpty = new Header[]{ myHeader };
        var fieldsHeader = InBandKeyManager.createEncryptedFieldsHeader(EnumSet.of(RecordField.RECORD_VALUE));

        var r1 = InBandKeyManager.prependToHeaders(new Header[0], fieldsHeader);
        assertEquals(1, r1.length);
        assertEquals(fieldsHeader, r1[0]);

        var r2 = InBandKeyManager.prependToHeaders(nonEmpty, fieldsHeader);
        assertEquals(2, r2.length);
        assertEquals(fieldsHeader, r2[0]);
        assertEquals(myHeader, r2[1]);

        var x0 = InBandKeyManager.removeInitialHeaders(r1, 1);
        assertEquals(0, x0.length);

        var x1 = InBandKeyManager.removeInitialHeaders(r2, 1);
        assertEquals(1, x1.length);
        assertEquals(myHeader, x1[0]);
    }

    @Test
    void testDestroy() {
        AtomicBoolean called = new AtomicBoolean();
        InBandKeyManager.destroy(new Destroyable() {
            @Override
            public void destroy() throws DestroyFailedException {
                called.set(true);
                throw new DestroyFailedException("Eeek!");
            }
        });

        assertTrue(called.get());
    }

    @Test
    void shouldEncryptRecordValue() {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 500_000);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        TestingRecord record = new TestingRecord(value);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record);
        km.encrypt(new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted));
        record.value().rewind();
        assertEquals(1, encrypted.size());
        assertNotEquals(initial, encrypted);
        // TODO add assertion on headers

        List<TestingRecord> decrypted = new ArrayList<>();
        km.decrypt(encrypted, recordReceivedRecord(decrypted));

        assertEquals(initial, decrypted);
    }

    @NonNull
    private static Receiver recordReceivedRecord(Collection<TestingRecord> list) {
        return (r, v, h) -> {
            list.add(new TestingRecord(copyBytes(v), h));
        };
    }

    @Test
    void shouldEncryptRecordValueForMultipleRecords() throws ExecutionException, InterruptedException, TimeoutException {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 500_000);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        TestingRecord record = new TestingRecord(value);

        var value2 = ByteBuffer.wrap(new byte[]{ 3, 4, 5 });
        TestingRecord record2 = new TestingRecord(value2);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record, record2);
        km.encrypt(new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                (r, v, h) -> {
                    encrypted.add(new TestingRecord(copyBytes(v), h));
                }).toCompletableFuture().get(10, TimeUnit.SECONDS);
        record.value().rewind();
        record2.value().rewind();
        assertEquals(2, encrypted.size());
        assertNotEquals(initial, encrypted);
        // TODO add assertion on headers

        List<TestingRecord> decrypted = new ArrayList<>();
        km.decrypt(encrypted, recordReceivedRecord(decrypted)).toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertEquals(initial, decrypted);
    }

    @Test
    void shouldGenerateNewDekIfOldDekHasNoRemainingEncryptions() throws ExecutionException, InterruptedException, TimeoutException {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 2);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        TestingRecord record = new TestingRecord(value);

        var value2 = ByteBuffer.wrap(new byte[]{ 3, 4, 5 });
        TestingRecord record2 = new TestingRecord(value2);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record, record2);
        km.encrypt(new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted)).toCompletableFuture().get(10, TimeUnit.SECONDS);
        record.value().rewind();
        record2.value().rewind();

        // at this point we have encrypted 2 records with the manager set to maximum 2 encryptions per dek

        km.encrypt(new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted)).toCompletableFuture().get(10, TimeUnit.SECONDS);

        record.value().rewind();
        record2.value().rewind();
        List<byte[]> deks = encrypted.stream()
                .map(testingRecord -> Arrays.stream(testingRecord.headers()).filter(header -> header.key().equals("kroxylicious.io/dek")).findFirst().orElseThrow()
                        .value())
                .toList();
        assertEquals(4, encrypted.size());
        assertEquals(4, deks.size());
        assertArrayEquals(deks.get(0), deks.get(1));
        assertArrayEquals(deks.get(2), deks.get(3));
        assertFalse(Arrays.equals(deks.get(0), deks.get(2)));
        assertFalse(Arrays.equals(deks.get(0), deks.get(3)));
    }

    @Test
    void shouldGenerateNewDekIfOldOneHasSomeRemainingEncryptionsButNotEnoughForWholeBatch() throws ExecutionException, InterruptedException, TimeoutException {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 3);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        TestingRecord record = new TestingRecord(value);

        var value2 = ByteBuffer.wrap(new byte[]{ 3, 4, 5 });
        TestingRecord record2 = new TestingRecord(value2);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record, record2);
        km.encrypt(new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted)).toCompletableFuture().get(10, TimeUnit.SECONDS);
        record.value().rewind();
        record2.value().rewind();

        // at this point we have encrypted 2 records with the manager set to maximum 3 encryptions per dek, so we need a new dek to encrypt 2 more records

        km.encrypt(new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted)).toCompletableFuture().get(10, TimeUnit.SECONDS);

        record.value().rewind();
        record2.value().rewind();
        List<byte[]> deks = encrypted.stream()
                .map(testingRecord -> Arrays.stream(testingRecord.headers()).filter(header -> header.key().equals("kroxylicious.io/dek")).findFirst().orElseThrow()
                        .value())
                .toList();
        assertEquals(4, encrypted.size());
        assertEquals(4, deks.size());
        assertArrayEquals(deks.get(0), deks.get(1));
        assertArrayEquals(deks.get(2), deks.get(3));
        assertFalse(Arrays.equals(deks.get(0), deks.get(2)));
        assertFalse(Arrays.equals(deks.get(0), deks.get(3)));
    }

    @Test
    void shouldUseSameDekForMultipleBatches() throws ExecutionException, InterruptedException, TimeoutException {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 4);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        TestingRecord record = new TestingRecord(value);

        var value2 = ByteBuffer.wrap(new byte[]{ 3, 4, 5 });
        TestingRecord record2 = new TestingRecord(value2);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record, record2);
        km.encrypt(new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted)).toCompletableFuture().get(10, TimeUnit.SECONDS);
        record.value().rewind();
        record2.value().rewind();

        // at this point we have encrypted 2 records with the manager set to maximum 4 encryptions per dek, so we do not need a new dek to encrypt 2 more records

        km.encrypt(new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted)).toCompletableFuture().get(10, TimeUnit.SECONDS);

        record.value().rewind();
        record2.value().rewind();
        List<byte[]> deks = encrypted.stream()
                .map(testingRecord -> Arrays.stream(testingRecord.headers()).filter(header -> header.key().equals("kroxylicious.io/dek")).findFirst().orElseThrow()
                        .value())
                .toList();
        assertEquals(4, encrypted.size());
        assertEquals(4, deks.size());
        assertArrayEquals(deks.get(0), deks.get(1));
        assertArrayEquals(deks.get(0), deks.get(2));
        assertArrayEquals(deks.get(0), deks.get(3));
    }

    @NonNull
    private static ByteBuffer copyBytes(ByteBuffer v) {
        byte[] bytes = new byte[v.remaining()];
        v.get(bytes);
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        return wrap;
    }

    @Test
    void shouldEncryptRecordHeaders() {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 500_000);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var headers = new Header[]{ new RecordHeader("foo", new byte[]{ 4, 5, 6 }) };
        TestingRecord record = new TestingRecord(value, headers);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record);
        km.encrypt(new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_HEADER_VALUES)),
                initial,
                recordReceivedRecord(encrypted));
        value.rewind();

        assertEquals(1, encrypted.size());
        assertNotEquals(initial, encrypted);

        List<TestingRecord> decrypted = new ArrayList<>();
        km.decrypt(encrypted, recordReceivedRecord(decrypted));

        assertEquals(List.of(new TestingRecord(value, new Header[]{ new RecordHeader("foo", new byte[]{ 4, 5, 6 }) })), decrypted);
    }

    @Test
    void shouldEncryptRecordHeadersForMultipleRecords() throws ExecutionException, InterruptedException, TimeoutException {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 500_000);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var headers = new Header[]{ new RecordHeader("foo", new byte[]{ 4, 5, 6 }) };
        TestingRecord record = new TestingRecord(value, headers);
        var value2 = ByteBuffer.wrap(new byte[]{ 7, 8, 9 });
        var headers2 = new Header[]{ new RecordHeader("foo", new byte[]{ 10, 11, 12 }) };
        TestingRecord record2 = new TestingRecord(value2, headers2);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record, record2);
        km.encrypt(new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_HEADER_VALUES)),
                initial,
                recordReceivedRecord(encrypted)).toCompletableFuture().get(10, TimeUnit.SECONDS);
        value.rewind();
        value2.rewind();
        assertEquals(2, encrypted.size());
        assertNotEquals(initial, encrypted);

        List<TestingRecord> decrypted = new ArrayList<>();
        km.decrypt(encrypted, recordReceivedRecord(decrypted));

        assertEquals(List.of(new TestingRecord(value, new Header[]{ new RecordHeader("foo", new byte[]{ 4, 5, 6 }) }),
                new TestingRecord(value2, new Header[]{ new RecordHeader("foo", new byte[]{ 10, 11, 12 }) })), decrypted);
    }

}
