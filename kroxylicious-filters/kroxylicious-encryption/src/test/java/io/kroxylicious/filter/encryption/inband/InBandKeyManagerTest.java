/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.encryption.EncryptionScheme;
import io.kroxylicious.filter.encryption.RecordField;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryKms;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.UnitTestingKmsService;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        var km = new InBandKeyManager<>(kms, BufferPool.allocating());

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        TestingRecord record = new TestingRecord(value);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record);
        km.encrypt(new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                (r, v, h) -> {
                    encrypted.add(new TestingRecord(v, h));
                });
        record.value().rewind();
        assertEquals(1, encrypted.size());
        assertNotEquals(initial, encrypted);
        // TODO add assertion on headers

        List<TestingRecord> decrypted = new ArrayList<>();
        km.decrypt(encrypted, (r, v, h) -> {
            decrypted.add(new TestingRecord(v, h));
        });

        assertEquals(initial, decrypted);
    }

    @Test
    void shouldEncryptRecordHeaders() {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating());

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var headers = new Header[]{ new RecordHeader("foo", new byte[]{ 4, 5, 6 }) };
        TestingRecord record = new TestingRecord(value, headers);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record);
        km.encrypt(new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_HEADER_VALUES)),
                initial,
                (r, v, h) -> {
                    encrypted.add(new TestingRecord(v, h));
                });

        assertEquals(1, encrypted.size());
        assertNotEquals(initial, encrypted);

        List<TestingRecord> decrypted = new ArrayList<>();
        km.decrypt(encrypted, (r, v, h) -> {
            decrypted.add(new TestingRecord(v, h));
        });

        assertEquals(List.of(new TestingRecord(value, new Header[]{ new RecordHeader("foo", new byte[]{ 4, 5, 6 }) })), decrypted);
    }

}
