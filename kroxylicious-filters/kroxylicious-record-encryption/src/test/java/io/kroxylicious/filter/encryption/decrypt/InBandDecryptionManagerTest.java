/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.decrypt;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import javax.crypto.SecretKey;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.ByteUtils;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import io.kroxylicious.filter.encryption.EncryptorCreationException;
import io.kroxylicious.filter.encryption.TestingDek;
import io.kroxylicious.filter.encryption.common.EncryptionException;
import io.kroxylicious.filter.encryption.common.FilterThreadExecutor;
import io.kroxylicious.filter.encryption.config.RecordField;
import io.kroxylicious.filter.encryption.crypto.Encryption;
import io.kroxylicious.filter.encryption.crypto.EncryptionHeader;
import io.kroxylicious.filter.encryption.crypto.EncryptionResolver;
import io.kroxylicious.filter.encryption.dek.CipherSpecResolver;
import io.kroxylicious.filter.encryption.dek.Dek;
import io.kroxylicious.filter.encryption.dek.DekManager;
import io.kroxylicious.filter.encryption.encrypt.EncryptionDekCache;
import io.kroxylicious.filter.encryption.encrypt.EncryptionScheme;
import io.kroxylicious.filter.encryption.encrypt.InBandEncryptionManager;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryEdek;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryKms;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.UnitTestingKmsService;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.test.assertj.MemoryRecordsAssert;
import io.kroxylicious.test.record.RecordTestUtils;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

class InBandDecryptionManagerTest {

    private static final String ARBITRARY_KEY = "key";
    private static final String ARBITRARY_KEY_2 = "key2";
    private static final String ARBITRARY_VALUE = "value";
    private static final String ARBITRARY_VALUE_2 = "value2";
    private static final Header[] ABSENT_HEADERS = {};

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
    void shouldEncryptRecordValue() {
        InMemoryKms kms = getInMemoryKms();
        var encryptionManager = createEncryptionManager(kms, 500_000);
        var decryptionManager = createDecryptionManager(kms);

        var kekId = kms.generateKey();

        var value = new byte[]{ 1, 2, 3 };
        Record record = RecordTestUtils.record(value);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record);
        assertThat(doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)), initial, encrypted))
                .isCompleted();
        assertThat(encrypted.iterator())
                .toIterable()
                .singleElement()
                .extracting(RecordTestUtils::recordValueAsBytes)
                .isNotEqualTo(value);

        List<Record> decrypted = new ArrayList<>();
        String topic = "foo";
        int partition = 1;
        assertThat(doDecrypt(decryptionManager, topic, partition, encrypted, decrypted))
                .isCompleted();

        assertThat(decrypted.iterator())
                .toIterable()
                .singleElement()
                .extracting(RecordTestUtils::recordValueAsBytes)
                .isEqualTo(value);
    }

    @Test
    void shouldPreserveMultipleBatchesOnEncrypt() {
        // given
        InMemoryKms kms = getInMemoryKms();
        EncryptionScheme<UUID> scheme = createScheme(kms);
        var encryptionManager = createEncryptionManager(kms, 500_000);

        MutableRecordBatch firstBatch = RecordTestUtils.singleElementRecordBatch(RecordBatch.CURRENT_MAGIC_VALUE, 1L, Compression.gzip().build(),
                TimestampType.CREATE_TIME, 2L,
                3L,
                (short) 4, 5, false, false, 1, ARBITRARY_KEY.getBytes(
                        StandardCharsets.UTF_8),
                ARBITRARY_VALUE.getBytes(StandardCharsets.UTF_8));

        MutableRecordBatch secondBatch = RecordTestUtils.singleElementRecordBatch(RecordBatch.CURRENT_MAGIC_VALUE, 2L, Compression.NONE,
                TimestampType.LOG_APPEND_TIME, 9L, 10L,
                (short) 11, 12, false, false, 2, ARBITRARY_KEY_2.getBytes(
                        StandardCharsets.UTF_8),
                ARBITRARY_VALUE_2.getBytes(StandardCharsets.UTF_8));
        MemoryRecords records = RecordTestUtils.memoryRecords(firstBatch, secondBatch);

        // when
        MemoryRecords encrypted = assertImmediateSuccessAndGet(encrypt(encryptionManager, scheme, records));

        // then
        MemoryRecordsAssert encryptedAssert = MemoryRecordsAssert.assertThat(encrypted);
        encryptedAssert.hasNumBatches(2);
        encryptedAssert.firstBatch().hasMetadataMatching(firstBatch).hasNumRecords(1).firstRecord().hasValueNotEqualTo(ARBITRARY_VALUE);
        encryptedAssert.lastBatch().hasMetadataMatching(secondBatch).hasNumRecords(1).firstRecord().hasValueNotEqualTo(ARBITRARY_VALUE_2);
    }

    @Test
    void shouldPreserveMultipleBatchesOnDecrypt() {
        // given
        InMemoryKms kms = getInMemoryKms();
        EncryptionScheme<UUID> scheme = createScheme(kms);
        var encryptionManager = createEncryptionManager(kms, 500_000);
        var decryptionManager = createDecryptionManager(kms);

        MutableRecordBatch firstBatch = RecordTestUtils.singleElementRecordBatch(RecordBatch.CURRENT_MAGIC_VALUE, 1L, Compression.gzip().build(),
                TimestampType.CREATE_TIME, 2L,
                3L,
                (short) 4, 5, false, false, 1, ARBITRARY_KEY.getBytes(
                        StandardCharsets.UTF_8),
                ARBITRARY_VALUE.getBytes(StandardCharsets.UTF_8));

        MutableRecordBatch secondBatch = RecordTestUtils.singleElementRecordBatch(RecordBatch.CURRENT_MAGIC_VALUE, 2L, Compression.NONE,
                TimestampType.LOG_APPEND_TIME, 9L, 10L,
                (short) 11, 12, false, false, 2, ARBITRARY_KEY_2.getBytes(
                        StandardCharsets.UTF_8),
                ARBITRARY_VALUE_2.getBytes(StandardCharsets.UTF_8));
        MemoryRecords records = RecordTestUtils.memoryRecords(firstBatch, secondBatch);
        MemoryRecords encrypted = assertImmediateSuccessAndGet(encrypt(encryptionManager, scheme, records));

        // when
        MemoryRecords decrypted = assertImmediateSuccessAndGet(decrypt(decryptionManager, encrypted));

        // then
        MemoryRecordsAssert decryptedAssert = MemoryRecordsAssert.assertThat(decrypted);
        decryptedAssert.hasNumBatches(2);
        decryptedAssert.firstBatch().hasMetadataMatching(firstBatch).hasNumRecords(1).firstRecord().hasValueEqualTo(ARBITRARY_VALUE);
        decryptedAssert.lastBatch().hasMetadataMatching(secondBatch).hasNumRecords(1).firstRecord().hasValueEqualTo(ARBITRARY_VALUE_2);
    }

    @Test
    void shouldPreserveControlBatchOnEncrypt() {
        // given
        InMemoryKms kms = getInMemoryKms();
        EncryptionScheme<UUID> scheme = createScheme(kms);
        var encryptionManager = createEncryptionManager(kms, 500_000);

        MutableRecordBatch firstBatch = RecordTestUtils.singleElementRecordBatch(1L, ARBITRARY_KEY, ARBITRARY_VALUE, ABSENT_HEADERS);
        MutableRecordBatch controlBatch = RecordTestUtils.abortTransactionControlBatch(2);
        Record controlRecord = controlBatch.iterator().next();
        MemoryRecords records = RecordTestUtils.memoryRecords(firstBatch, controlBatch);

        // when
        MemoryRecords encrypted = assertImmediateSuccessAndGet(encrypt(encryptionManager, scheme, records));

        // then
        MemoryRecordsAssert encryptedAssert = MemoryRecordsAssert.assertThat(encrypted);
        encryptedAssert.hasNumBatches(2);
        encryptedAssert.firstBatch().hasMetadataMatching(firstBatch).hasNumRecords(1).firstRecord().hasValueNotEqualTo(ARBITRARY_VALUE);
        encryptedAssert.lastBatch().hasMetadataMatching(controlBatch).hasNumRecords(1).firstRecord().hasValueEqualTo(controlRecord);
    }

    @Test
    void shouldPreserveControlBatchOnDecrypt() {
        // given
        InMemoryKms kms = getInMemoryKms();
        EncryptionScheme<UUID> scheme = createScheme(kms);
        var encryptionManager = createEncryptionManager(kms, 500_000);
        var decryptionManager = createDecryptionManager(kms);

        MutableRecordBatch firstBatch = RecordTestUtils.singleElementRecordBatch(1L, ARBITRARY_KEY, ARBITRARY_VALUE, ABSENT_HEADERS);
        MutableRecordBatch controlBatch = RecordTestUtils.abortTransactionControlBatch(2);
        Record controlRecord = controlBatch.iterator().next();
        MemoryRecords records = RecordTestUtils.memoryRecords(firstBatch, controlBatch);
        MemoryRecords encrypted = assertImmediateSuccessAndGet(encrypt(encryptionManager, scheme, records));

        // when
        MemoryRecords decrypted = assertImmediateSuccessAndGet(decrypt(decryptionManager, encrypted));

        // then
        MemoryRecordsAssert decryptedAssert = MemoryRecordsAssert.assertThat(decrypted);
        decryptedAssert.hasNumBatches(2);
        decryptedAssert.firstBatch().hasMetadataMatching(firstBatch).hasNumRecords(1).firstRecord().hasValueEqualTo(ARBITRARY_VALUE);
        decryptedAssert.lastBatch().hasMetadataMatching(controlBatch).hasNumRecords(1).firstRecord().hasValueEqualTo(controlRecord);
    }

    @Test
    void shouldPreserveEmptyBatchOnEncrypt() {
        // given
        InMemoryKms kms = getInMemoryKms();
        EncryptionScheme<UUID> scheme = createScheme(kms);
        var encryptionManager = createEncryptionManager(kms, 500_000);

        MutableRecordBatch firstBatch = RecordTestUtils.singleElementRecordBatch(1L, ARBITRARY_KEY, ARBITRARY_VALUE, ABSENT_HEADERS);
        MutableRecordBatch emptyBatch = RecordTestUtils.recordBatchWithAllRecordsRemoved(2L);
        MemoryRecords records = RecordTestUtils.memoryRecords(firstBatch, emptyBatch);

        // when
        MemoryRecords encrypted = assertImmediateSuccessAndGet(encrypt(encryptionManager, scheme, records));

        // then
        MemoryRecordsAssert encryptedAssert = MemoryRecordsAssert.assertThat(encrypted);
        encryptedAssert.hasNumBatches(2);
        encryptedAssert.firstBatch().hasMetadataMatching(firstBatch).hasNumRecords(1).firstRecord().hasValueNotEqualTo(ARBITRARY_VALUE);
        encryptedAssert.lastBatch().hasMetadataMatching(emptyBatch).hasNumRecords(0);
    }

    @Test
    void shouldPreserveEmptyBatchOnDecrypt() {
        // given
        InMemoryKms kms = getInMemoryKms();
        EncryptionScheme<UUID> scheme = createScheme(kms);
        var encryptionManager = createEncryptionManager(kms, 500_000);
        var decryptionManager = createDecryptionManager(kms);

        MutableRecordBatch firstBatch = RecordTestUtils.singleElementRecordBatch(1L, ARBITRARY_KEY, ARBITRARY_VALUE, ABSENT_HEADERS);
        MutableRecordBatch emptyBatch = RecordTestUtils.recordBatchWithAllRecordsRemoved(2L);
        MemoryRecords records = RecordTestUtils.memoryRecords(firstBatch, emptyBatch);
        MemoryRecords encrypted = assertImmediateSuccessAndGet(encrypt(encryptionManager, scheme, records));

        // when
        MemoryRecords decrypted = assertImmediateSuccessAndGet(decrypt(decryptionManager, encrypted));

        // then
        MemoryRecordsAssert decryptedAssert = MemoryRecordsAssert.assertThat(decrypted);
        decryptedAssert.hasNumBatches(2);
        decryptedAssert.firstBatch().hasMetadataMatching(firstBatch).hasNumRecords(1).firstRecord().hasValueEqualTo(ARBITRARY_VALUE);
        decryptedAssert.lastBatch().hasMetadataMatching(emptyBatch).hasNumRecords(0);
    }

    @NonNull
    private static CompletionStage<Void> doDecrypt(
                                                   InBandDecryptionManager<UUID, InMemoryEdek> decryptionManager, String topic, int partition, List<Record> encrypted,
                                                   List<Record> decrypted) {
        return decryptionManager.decrypt(topic, partition, RecordTestUtils.memoryRecords(encrypted), ByteBufferOutputStream::new)
                .thenAccept(records -> records.records().forEach(decrypted::add));
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

    @Test
    void shouldTolerateEncryptingAndDecryptingEmptyRecordValue() {
        InMemoryKms kms = getInMemoryKms();
        var encryptionManager = createEncryptionManager(kms, 500_000);
        var decryptionManager = createDecryptionManager(kms);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{});
        Record record = RecordTestUtils.record(value);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record);
        assertThat(doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)), initial, encrypted))
                .isCompleted();
        record.value().rewind();
        assertEquals(1, encrypted.size());
        assertNotEquals(initial, encrypted);

        List<Record> decrypted = new ArrayList<>();
        assertThat(doDecrypt(decryptionManager, "foo", 1, encrypted, decrypted)).isCompleted();

        assertEquals(initial, decrypted);
    }

    @Test
    void decryptSupportsUnencryptedRecordValue() {
        InMemoryKms kms = getInMemoryKms();
        var decryptionManager = createDecryptionManager(kms);

        byte[] recBytes = { 1, 2, 3 };
        Record record = RecordTestUtils.record(recBytes);

        List<Record> received = new ArrayList<>();
        assertThat(doDecrypt(decryptionManager, "foo", 1, List.of(record), received)).isCompleted();

        assertThat(received).hasSize(1);
        assertThat(received.stream()
                .map(RecordTestUtils::recordValueAsBytes))
                .containsExactly(recBytes);
    }

    static List<MemoryRecords> decryptSupportsEmptyRecordBatches() {
        return List.of(MemoryRecords.EMPTY, RecordTestUtils.memoryRecordsWithAllRecordsRemoved());
    }

    @ParameterizedTest
    @MethodSource
    void decryptSupportsEmptyRecordBatches(MemoryRecords records) {
        InMemoryKms kms = getInMemoryKms();
        var decryptionManager = createDecryptionManager(kms);
        assertThat(decryptionManager.decrypt("foo", 1, records, ByteBufferOutputStream::new))
                .succeedsWithin(Duration.ZERO).isSameAs(records);
    }

    // we do not want to break compaction tombstoning by creating a parcel for the null value case
    @Test
    void nullRecordValuesShouldNotBeModifiedAtEncryptTime() {
        InMemoryKms kms = getInMemoryKms();
        var encryptionManager = createEncryptionManager(kms, 500_000);

        var kekId = kms.generateKey();

        Record record = RecordTestUtils.record((ByteBuffer) null);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record);
        assertThat(doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)), initial, encrypted))
                .isCompleted();
        assertEquals(1, encrypted.size());
        assertFalse(encrypted.get(0).hasValue());
    }

    // we do not want to break compaction tombstoning by creating a parcel for the null value case,
    // but currently we do not have a scheme for how to serialize headers when the original record
    // value is null.
    @Test
    void nullRecordValuesAreIncompatibleWithHeaderEncryption() {
        InMemoryKms kms = getInMemoryKms();
        var encryptionManager = createEncryptionManager(kms, 500_000);

        var kekId = kms.generateKey();

        var headers = new Header[]{ new RecordHeader("headerFoo", new byte[]{ 4, 5, 6 }) };
        Record record = RecordTestUtils.record((ByteBuffer) null, headers);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record);
        String expectedMessage = "encrypting headers prohibited when original record value null, we must preserve the null for tombstoning";
        assertThat(doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_HEADER_VALUES)), initial, encrypted))
                .failsWithin(Duration.ofSeconds(5)).withThrowableThat()
                .withMessageContaining(expectedMessage);
    }

    @Test
    void shouldTolerateEncryptingEmptyBatch() {
        InMemoryKms kms = getInMemoryKms();
        var encryptionManager = createEncryptionManager(kms, 500_000);

        var kekId = kms.generateKey();

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of();
        assertThat(doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)), initial, encrypted))
                .isCompleted();

        assertEquals(0, encrypted.size());
    }

    @Test
    void shouldTolerateEncryptingSingleBatchMemoryRecordsWithNoRecords() {
        InMemoryKms kms = getInMemoryKms();
        var encryptionManager = createEncryptionManager(kms, 500_000);
        EncryptionScheme<UUID> scheme = createScheme(kms);
        MemoryRecords records = RecordTestUtils.memoryRecordsWithAllRecordsRemoved();
        assertThat(encrypt(encryptionManager, scheme, records)).succeedsWithin(Duration.ZERO).isSameAs(records);
    }

    @Test
    void encryptionRetry() {
        InMemoryKms kms = getInMemoryKms();
        var kekId = kms.generateKey();
        // configure 1 encryption per dek but then try to encrypt 2 records, will destroy and retry
        var encryptionManager = createEncryptionManager(kms, 1);

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var value2 = ByteBuffer.wrap(new byte[]{ 4, 5, 6 });
        Record record = RecordTestUtils.record(0L, value);
        Record record2 = RecordTestUtils.record(1L, value2);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record, record2);
        CompletionStage<Void> encrypt = doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                encrypted);
        assertThat(encrypt).failsWithin(Duration.ofSeconds(5)).withThrowableThat()
                .withMessageMatching(".*failed to reserve an EDEK to encrypt 2 records for topic topic partition 1 after [0-9]+ attempts");
    }

    @Test
    void dekCreationRetryFailurePropagatedToEncryptCompletionStage() {
        InMemoryKms kms = getInMemoryKms();
        var kekId = kms.generateKey();
        InMemoryKms spyKms = Mockito.spy(kms);
        when(spyKms.generateDekPair(kekId)).thenReturn(CompletableFuture.failedFuture(new EncryptorCreationException("failed to create that DEK")));
        var encryptionManager = createEncryptionManager(spyKms, 500_000);

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var value2 = ByteBuffer.wrap(new byte[]{ 4, 5, 6 });
        Record record = RecordTestUtils.record(0L, value);
        Record record2 = RecordTestUtils.record(1L, value2);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record, record2);
        CompletionStage<Void> encrypt = doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                encrypted);
        assertThat(encrypt).failsWithin(Duration.ofSeconds(5)).withThrowableThat().withMessageContaining("failed to create that DEK");
    }

    @Test
    void edekDecryptionRetryFailurePropagatedToDecryptCompletionStage() {
        InMemoryKms kms = getInMemoryKms();
        var kekId = kms.generateKey();
        InMemoryKms spyKms = Mockito.spy(kms);
        doReturn(CompletableFuture.failedFuture(new KmsException("failed to create that DEK"))).when(spyKms).decryptEdek(any());

        var encryptionManager = createEncryptionManager(spyKms, 500_000);
        var decryptionManager = createDecryptionManager(spyKms);

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var value2 = ByteBuffer.wrap(new byte[]{ 4, 5, 6 });
        Record record = RecordTestUtils.record(0L, value);
        Record record2 = RecordTestUtils.record(1L, value2);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record, record2);
        CompletionStage<Void> encrypt = doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                encrypted);
        assertThat(encrypt).succeedsWithin(Duration.ofSeconds(5));

        List<Record> decrypted = new ArrayList<>();
        CompletionStage<Void> decrypt = doDecrypt(decryptionManager, "topic", 1, encrypted, decrypted);
        assertThat(decrypt).failsWithin(Duration.ofSeconds(5)).withThrowableThat().withMessageContaining("failed to create that DEK");
    }

    @Test
    void afterWeFailToLoadADekTheNextEncryptionAttemptCanSucceed() {
        InMemoryKms kms = getInMemoryKms();
        var kekId = kms.generateKey();
        InMemoryKms spyKms = Mockito.spy(kms);
        when(spyKms.generateDekPair(kekId)).thenReturn(CompletableFuture.failedFuture(new KmsException("failed to create that DEK")));

        var encryptionManager = createEncryptionManager(spyKms, 50_000);

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var value2 = ByteBuffer.wrap(new byte[]{ 4, 5, 6 });
        Record record = RecordTestUtils.record(0L, value);
        Record record2 = RecordTestUtils.record(1L, value2);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record, record2);
        CompletionStage<Void> encrypt = doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                encrypted);
        assertThat(encrypt).failsWithin(Duration.ofSeconds(5)).withThrowableThat().withMessageContaining("failed to create that DEK");

        // given KMS is no longer generating failed futures
        when(spyKms.generateDekPair(kekId)).thenCallRealMethod();

        // when
        CompletionStage<Void> encrypt2 = doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                encrypted);

        // then
        assertThat(encrypt2).succeedsWithin(Duration.ofSeconds(5));
        assertThat(encrypted).hasSize(2);
    }

    @Test
    void shouldEncryptRecordValueForMultipleRecords() throws ExecutionException, InterruptedException, TimeoutException {
        InMemoryKms kms = getInMemoryKms();
        var encryptionManager = createEncryptionManager(kms, 500_000);
        var decryptionManager = createDecryptionManager(kms);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        Record record = RecordTestUtils.record(0L, value);

        var value2 = ByteBuffer.wrap(new byte[]{ 3, 4, 5 });
        Record record2 = RecordTestUtils.record(1L, value2);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record, record2);
        doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                encrypted)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        record.value().rewind();
        record2.value().rewind();
        assertEquals(2, encrypted.size());
        assertNotEquals(initial, encrypted);
        // TODO add assertion on headers

        List<Record> decrypted = new ArrayList<>();
        doDecrypt(decryptionManager, "foo", 1, encrypted, decrypted).toCompletableFuture()
                .get(10, TimeUnit.SECONDS);

        assertEquals(initial, decrypted);
    }

    @Test
    void shouldGenerateNewDekIfOldDekHasNoRemainingEncryptions() throws ExecutionException, InterruptedException, TimeoutException {
        InMemoryKms kms = getInMemoryKms();
        var encryptionManager = createEncryptionManager(kms, 2);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        Record record = RecordTestUtils.record(0L, value);

        var value2 = ByteBuffer.wrap(new byte[]{ 3, 4, 5 });
        Record record2 = RecordTestUtils.record(1L, value2);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record, record2);
        doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                encrypted).toCompletableFuture().get(10, TimeUnit.SECONDS);
        record.value().rewind();
        record2.value().rewind();

        // at this point we have encrypted 2 records with the manager set to maximum 2 encryptions per dek

        doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                encrypted).toCompletableFuture().get(10, TimeUnit.SECONDS);

        record.value().rewind();
        record2.value().rewind();

        assertThat(kms.numDeksGenerated()).isEqualTo(2);
        var edekOne = getSerializedGeneratedEdek(kms, 0);
        var edekTwo = getSerializedGeneratedEdek(kms, 1);
        assertThat(encrypted).hasSize(4);
        List<TestingDek> deks = extractEdeks(encrypted);
        assertThat(deks).containsExactly(edekOne, edekOne, edekTwo, edekTwo);
    }

    @Test
    void shouldGenerateNewDekIfOldOneHasSomeRemainingEncryptionsButNotEnoughForWholeBatch() throws ExecutionException, InterruptedException, TimeoutException {
        InMemoryKms kms = getInMemoryKms();
        var encryptionManager = createEncryptionManager(kms, 3);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        Record record = RecordTestUtils.record(0L, value);

        var value2 = ByteBuffer.wrap(new byte[]{ 3, 4, 5 });
        Record record2 = RecordTestUtils.record(1L, value2);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record, record2);
        doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                encrypted)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        record.value().rewind();
        record2.value().rewind();

        // at this point we have encrypted 2 records with the manager set to maximum 3 encryptions per dek, so we need a new dek to encrypt 2 more records

        doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                encrypted).toCompletableFuture().get(10, TimeUnit.SECONDS);

        record.value().rewind();
        record2.value().rewind();
        assertThat(kms.numDeksGenerated()).isEqualTo(2);
        var edekOne = getSerializedGeneratedEdek(kms, 0);
        var edekTwo = getSerializedGeneratedEdek(kms, 1);
        assertThat(encrypted).hasSize(4);
        List<TestingDek> deks = extractEdeks(encrypted);
        assertThat(deks).containsExactly(edekOne, edekOne, edekTwo, edekTwo);
    }

    @Test
    void shouldUseSameDekForMultipleBatches() throws ExecutionException, InterruptedException, TimeoutException {
        InMemoryKms kms = getInMemoryKms();
        var encryptionManager = createEncryptionManager(kms, 4);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        Record record = RecordTestUtils.record(0L, value);

        var value2 = ByteBuffer.wrap(new byte[]{ 3, 4, 5 });
        Record record2 = RecordTestUtils.record(1L, value2);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record, record2);
        doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                encrypted)
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        record.value().rewind();
        record2.value().rewind();

        // at this point we have encrypted 2 records with the manager set to maximum 4 encryptions per dek, so we do not need a new dek to encrypt 2 more records

        doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                encrypted).toCompletableFuture().get(10, TimeUnit.SECONDS);

        record.value().rewind();
        record2.value().rewind();
        assertThat(kms.numDeksGenerated()).isEqualTo(1);
        var edekOne = getSerializedGeneratedEdek(kms, 0);
        assertThat(encrypted).hasSize(4);
        List<TestingDek> deks = extractEdeks(encrypted);
        assertThat(deks).containsExactly(edekOne, edekOne, edekOne, edekOne);
    }

    @Test
    void shouldEncryptRecordHeaders() {
        InMemoryKms kms = getInMemoryKms();
        var encryptionManager = createEncryptionManager(kms, 500_000);
        var decryptionManager = createDecryptionManager(kms);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var headers = new Header[]{ new RecordHeader("headerFoo", new byte[]{ 4, 5, 6 }) };
        Record record = RecordTestUtils.record(value, headers);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record);
        assertThat(doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE, RecordField.RECORD_HEADER_VALUES)),
                initial,
                encrypted))
                .isCompleted();
        value.rewind();

        assertEquals(1, encrypted.size());
        assertNotEquals(initial, encrypted);

        List<Record> decrypted = new ArrayList<>();
        assertThat(doDecrypt(decryptionManager, "topicFoo", 1, encrypted, decrypted))
                .isCompleted();

        assertEquals(List.of(RecordTestUtils.record(value, new RecordHeader("headerFoo", new byte[]{ 4, 5, 6 }))), decrypted);
    }

    @Test
    void shouldEncryptRecordHeadersForMultipleRecords() throws ExecutionException, InterruptedException, TimeoutException {
        InMemoryKms kms = getInMemoryKms();
        var encryptionManager = createEncryptionManager(kms, 500_000);
        var decryptionManager = createDecryptionManager(kms);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var headers = new Header[]{ new RecordHeader("foo", new byte[]{ 4, 5, 6 }) };
        Record record = RecordTestUtils.record(0L, value, headers);
        var value2 = ByteBuffer.wrap(new byte[]{ 7, 8, 9 });
        var headers2 = new Header[]{ new RecordHeader("foo", new byte[]{ 10, 11, 12 }) };
        Record record2 = RecordTestUtils.record(1L, value2, headers2);

        // checking that non-contiguous offsets are preserved
        var value3 = ByteBuffer.wrap(new byte[]{ 13, 14, 15 });
        var headers3 = new Header[]{ new RecordHeader("foo", new byte[]{ 16, 17, 18 }) };
        Record record3 = RecordTestUtils.record(4L, value3, headers3);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record, record2, record3);
        doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE, RecordField.RECORD_HEADER_VALUES)),
                initial,
                encrypted).toCompletableFuture().get(10, TimeUnit.SECONDS);
        value.rewind();
        value2.rewind();
        value3.rewind();
        assertEquals(3, encrypted.size());
        assertNotEquals(initial, encrypted);

        List<Record> decrypted = new ArrayList<>();
        assertThat(doDecrypt(decryptionManager, "foo", 1, encrypted, decrypted)).isCompleted();

        assertEquals(List.of(RecordTestUtils.record(0L, value, new RecordHeader("foo", new byte[]{ 4, 5, 6 })),
                RecordTestUtils.record(1L, value2, new RecordHeader("foo", new byte[]{ 10, 11, 12 })),
                RecordTestUtils.record(4L, value3, new RecordHeader("foo", new byte[]{ 16, 17, 18 }))), decrypted);
    }

    @Test
    void shouldPropagateHeadersInClearWhenNotEncryptingHeaders() {
        InMemoryKms kms = getInMemoryKms();
        var encryptionManager = createEncryptionManager(kms, 500_000);
        var decryptionManager = createDecryptionManager(kms);

        var kekId = kms.generateKey();

        var value = new byte[]{ 1, 2, 3 };
        var header = new RecordHeader("myHeader", new byte[]{ 4, 5, 6 });
        var record = RecordTestUtils.record(ByteBuffer.wrap(value), header);

        List<Record> encrypted = new ArrayList<>();
        assertThat(doEncrypt(encryptionManager, "topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)), List.of(record), encrypted))
                .isCompleted();
        assertThat(encrypted.iterator())
                .toIterable()
                .singleElement()
                .extracting(Record::headers)
                .asInstanceOf(InstanceOfAssertFactories.array(Header[].class))
                .hasSize(2) /* additional header is the kroxylicious.io/encryption header */
                .contains(header);

        List<Record> decrypted = new ArrayList<>();
        assertThat(doDecrypt(decryptionManager, "foo", 1, encrypted, decrypted))
                .isCompleted();

        assertThat(decrypted.iterator())
                .toIterable()
                .singleElement()
                .extracting(Record::headers)
                .asInstanceOf(InstanceOfAssertFactories.array(Header[].class))
                .hasSize(1)
                .containsExactly(header);
    }

    @ParameterizedTest
    @CsvSource({ "0,1", "0,3" })
    void decryptPreservesOrdering(long offsetA, long offsetB) {
        var topic = "topic";
        var partition = 1;

        InMemoryKms kms = getInMemoryKms();
        var kekId1 = kms.generateKey();
        var kekId2 = kms.generateKey();

        var spyKms = Mockito.spy(kms);

        var encryptionManager = createEncryptionManager(kms, 500_000);
        var decryptionManager = createDecryptionManager(kms);

        byte[] rec1Bytes = { 1, 2, 3 };
        byte[] rec2Bytes = { 4, 5, 6 };
        var rec1 = RecordTestUtils.record(offsetA, ByteBuffer.wrap(rec1Bytes));
        var rec2 = RecordTestUtils.record(offsetB, ByteBuffer.wrap(rec2Bytes));

        List<Record> encrypted = new ArrayList<>();
        var encryptStage = doEncrypt(encryptionManager, topic, partition, new EncryptionScheme<>(kekId1, EnumSet.of(RecordField.RECORD_VALUE)),
                List.of(rec1),
                encrypted)
                .thenApply(u -> doEncrypt(encryptionManager, topic, partition, new EncryptionScheme<>(kekId2, EnumSet.of(RecordField.RECORD_VALUE)),
                        List.of(rec2),
                        encrypted));
        assertThat(encryptStage).isCompleted();
        assertThat(encrypted).hasSize(2);
        assertThat(kms.numDeksGenerated()).isEqualTo(2);

        var lastEdek = kms.getGeneratedEdek(kms.numDeksGenerated() - 1).edek();
        var argument = ArgumentCaptor.forClass(kms.edekClass());

        // intercept the decryptEdek requests and organise for the first n-1 deks to decrypt only after the last
        var trigger = new CompletableFuture<Void>();
        doAnswer((Answer<CompletableFuture<SecretKey>>) invocation -> {
            var edek = argument.getValue();
            var underlying = kms.decryptEdek(edek);
            if (Objects.equals(argument.getValue(), lastEdek)) {
                CompletableFuture.delayedExecutor(25, TimeUnit.MILLISECONDS)
                        .execute(() -> trigger.complete(null));
                return underlying;
            }
            else {
                return underlying.thenCombine(trigger, (sk, other) -> sk);
            }
        }).when(spyKms).decryptEdek(argument.capture());

        List<Record> decrypted = new ArrayList<>();
        var decryptStage = doDecrypt(decryptionManager, topic, partition, encrypted, decrypted);
        assertThat(decryptStage).succeedsWithin(Duration.ofSeconds(1));
        assertThat(decrypted.iterator())
                .toIterable()
                .extracting(RecordTestUtils::recordValueAsBytes)
                .containsExactly(rec1Bytes, rec2Bytes);
        assertThat(decrypted.iterator())
                .toIterable()
                .extracting(Record::offset)
                .containsExactly(offsetA, offsetB);
    }

    @Test
    void decryptPreservesOrdering_RecordSetIncludeUnencrypted() {
        var topic = "topic";
        var partition = 1;

        InMemoryKms kms = getInMemoryKms();
        var kekId = kms.generateKey();

        var encryptionManager = createEncryptionManager(kms, 500_000);
        var decryptionManager = createDecryptionManager(kms);

        byte[] rec1Bytes = { 1, 2, 3 };
        byte[] rec2Bytes = { 4, 5, 6 };
        byte[] rec3Bytes = { 7, 8, 9 };
        var rec1 = RecordTestUtils.record(0L, ByteBuffer.wrap(rec1Bytes));
        var rec2 = RecordTestUtils.record(1L, ByteBuffer.wrap(rec2Bytes));
        var rec3 = RecordTestUtils.record(2L, ByteBuffer.wrap(rec3Bytes));

        // rec1 and rec3 will be encrypted.
        List<Record> encrypted = new ArrayList<>();
        var encryptStage = doEncrypt(encryptionManager, topic, partition, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                List.of(rec1, rec3),
                encrypted);
        assertThat(encryptStage).isCompleted();
        assertThat(encrypted).hasSize(2);

        // rec2 will be unencrypted
        List<Record> decryptInput = new ArrayList<>(encrypted);
        decryptInput.add(1, rec2);

        List<Record> received = new ArrayList<>();
        var decryptStage = doDecrypt(decryptionManager, topic, partition, decryptInput, received);
        assertThat(decryptStage).succeedsWithin(Duration.ofSeconds(1));
        assertThat(received.iterator())
                .toIterable()
                .extracting(RecordTestUtils::recordValueAsBytes)
                .containsExactly(rec1Bytes, rec2Bytes, rec3Bytes);
    }

    @Test
    void shouldDestroyEvictedDeks() {
        // Given
        InMemoryKms kms = getInMemoryKms();
        var kek1 = kms.generateKey();
        var kek2 = kms.generateKey();

        var encryptionManager = createEncryptionManager(kms,
                50_000,
                1024 * 1024,
                8 * 1024 * 1024,
                1);

        var value = new byte[]{ 1, 2, 3 };
        Record record = RecordTestUtils.record(value);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record);
        EncryptionScheme<UUID> scheme1 = new EncryptionScheme<>(kek1, EnumSet.of(RecordField.RECORD_VALUE));
        doEncrypt(encryptionManager, "topic", 1, scheme1, initial, encrypted);

        Dek<InMemoryEdek> dek1 = encryptionManager.currentDek(scheme1).toCompletableFuture().join();
        assertThat(dek1.isDestroyed()).isFalse();

        // When
        // Encrypt with key2, which should evict the DEK for key 1
        EncryptionScheme<UUID> scheme2 = new EncryptionScheme<>(kek2, EnumSet.of(RecordField.RECORD_VALUE));
        doEncrypt(encryptionManager, "topic", 1, scheme2, initial, encrypted);

        // Then
        assertThat(dek1.isDestroyed()).isTrue();
    }

    @Test
    void shouldThrowExecutionExceptionIfRecordTooLarge() {
        // Given
        InMemoryKms kms = getInMemoryKms();
        var kek1 = kms.generateKey();

        var encryptionManager = createEncryptionManager(kms,
                50_000,
                10,
                10,
                1);

        var value = new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        Record record = RecordTestUtils.record(value);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record);
        EncryptionScheme<UUID> scheme1 = new EncryptionScheme<>(kek1, EnumSet.of(RecordField.RECORD_VALUE));

        // When
        assertThat(doEncrypt(encryptionManager, "topic", 1, scheme1, initial, encrypted))
                .failsWithin(1, TimeUnit.NANOSECONDS)
                .withThrowableThat()
                .isExactlyInstanceOf(ExecutionException.class)
                .havingCause()
                .isExactlyInstanceOf(EncryptionException.class)
                .withMessage("Record buffer cannot grow greater than 10 bytes");
    }

    @Test
    void shouldSucceedIfCanReallocateRecordBuffer() {
        // Given
        InMemoryKms kms = getInMemoryKms();
        var kek1 = kms.generateKey();

        var encryptionManager = createEncryptionManager(kms,
                50_000,
                10,
                1000,
                1);

        var value = new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        Record record = RecordTestUtils.record(value);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record);
        EncryptionScheme<UUID> scheme1 = new EncryptionScheme<>(kek1, EnumSet.of(RecordField.RECORD_VALUE));

        // When
        assertThat(doEncrypt(encryptionManager, "topic", 1, scheme1, initial, encrypted))
                .succeedsWithin(1, TimeUnit.NANOSECONDS);
    }
    @Test
    void shouldSucceedIfCanReallocateRecordBufferHappensLater() {
        // Given
        InMemoryKms kms = getInMemoryKms();
        var kek1 = kms.generateKey();

        var encryptionManager = createEncryptionManager(kms,
                50_000,
                100,
                1000,
                1);

        var value = new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        Record record = RecordTestUtils.record(value);

        List<Record> encrypted = new ArrayList<>();
        List<Record> initial = List.of(record);
        EncryptionScheme<UUID> scheme1 = new EncryptionScheme<>(kek1, EnumSet.of(RecordField.RECORD_VALUE));

        // When
        assertThat(doEncrypt(encryptionManager, "topic", 1, scheme1, initial, encrypted))
                .succeedsWithin(1, TimeUnit.NANOSECONDS);
    }


    public TestingDek getSerializedGeneratedEdek(InMemoryKms kms, int i) {
        var generatedEdek = kms.getGeneratedEdek(i);
        var edek = generatedEdek.edek();
        var serde = kms.edekSerde();
        int size = serde.sizeOf(edek);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        serde.serialize(edek, buffer);
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new TestingDek(bytes);
    }

    private <T> T assertImmediateSuccessAndGet(CompletionStage<T> stage) {
        CompletableFuture<T> future = stage.toCompletableFuture();
        assertThat(future).succeedsWithin(Duration.ZERO);
        return future.join();
    }

    @NonNull
    private static List<TestingDek> extractEdeks(List<Record> encrypted) {
        return encrypted.stream()
                .filter(testingRecord -> Stream.of(testingRecord.headers()).anyMatch(header -> header.key().equals(EncryptionHeader.ENCRYPTION_HEADER_NAME)))
                .map(testingRecord -> {
                    ByteBuffer wrapper = testingRecord.value();
                    CipherSpecResolver.ALL.fromSerializedId(wrapper.get());
                    var edekLength = ByteUtils.readUnsignedVarint(wrapper);
                    byte[] edekBytes = new byte[edekLength];
                    wrapper.get(edekBytes);
                    return new TestingDek(edekBytes);
                })
                .toList();
    }

    @NonNull
    private static InBandDecryptionManager<UUID, InMemoryEdek> createDecryptionManager(InMemoryKms kms) {

        DekManager<UUID, InMemoryEdek> dekManager = new DekManager<>(kms, 1);
        var dekCache = new DecryptionDekCache<>(dekManager, directExecutor(), DecryptionDekCache.NO_MAX_CACHE_SIZE);
        return new InBandDecryptionManager<>(EncryptionResolver.ALL,
                dekManager,
                dekCache,
                new FilterThreadExecutor(directExecutor()));
    }

    @NonNull
    private static InBandEncryptionManager<UUID, InMemoryEdek> createEncryptionManager(InMemoryKms kms, int maxEncryptionsPerDek) {
        return createEncryptionManager(kms,
                maxEncryptionsPerDek,
                1024 * 1024,
                8 * 1024 * 1024,
                EncryptionDekCache.NO_MAX_CACHE_SIZE);
    }

    @NonNull
    private static InBandEncryptionManager<UUID, InMemoryEdek> createEncryptionManager(InMemoryKms kms,
                                                                                       int maxEncryptionsPerDek,
                                                                                       int recordBufferInitialBytes,
                                                                                       int recordBufferMaxBytes,
                                                                                       int maxCacheSize) {

        DekManager<UUID, InMemoryEdek> dekManager = new DekManager<>(kms, maxEncryptionsPerDek);
        var cache = new EncryptionDekCache<>(dekManager, directExecutor(), maxCacheSize, Duration.ofHours(1), Duration.ofHours(1));
        return new InBandEncryptionManager<>(Encryption.V2,
                dekManager.edekSerde(),
                recordBufferInitialBytes,
                recordBufferMaxBytes,
                cache,
                new FilterThreadExecutor(directExecutor()));
    }

    @NonNull
    private static Executor directExecutor() {
        // Run cache evictions on the test thread, avoiding the need for tests to sleep to observe cache evictions
        return Runnable::run;
    }

    @NonNull
    private static EncryptionScheme<UUID> createScheme(InMemoryKms kms) {
        var kekId = kms.generateKey();
        return new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE));
    }

    @NonNull
    private static CompletionStage<MemoryRecords> decrypt(InBandDecryptionManager<UUID, InMemoryEdek> km, MemoryRecords encrypted) {
        return km.decrypt("topic", 1, encrypted, ByteBufferOutputStream::new);
    }

    @NonNull
    private static InMemoryKms getInMemoryKms() {
        var kmsService = UnitTestingKmsService.newInstance();
        kmsService.initialize(new UnitTestingKmsService.Config());
        return kmsService.buildKms();
    }

    @NonNull
    private static CompletionStage<MemoryRecords> encrypt(InBandEncryptionManager<UUID, InMemoryEdek> km, EncryptionScheme<UUID> scheme, MemoryRecords records) {
        return km.encrypt("topic", 1, scheme, records, ByteBufferOutputStream::new);
    }

}
