/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.inband;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.utils.ByteUtils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.filter.encryption.BackoffStrategy;
import io.kroxylicious.filter.encryption.EncryptionScheme;
import io.kroxylicious.filter.encryption.Receiver;
import io.kroxylicious.filter.encryption.RecordField;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryKms;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.UnitTestingKmsService;
import io.kroxylicious.kms.service.DekPair;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.Serde;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

class InBandKeyManagerTest {

    public static final ScheduledExecutorService EXECUTOR_SERVICE = Executors.newScheduledThreadPool(1);
    public static final BackoffStrategy BACKOFF_STRATEGY = failures -> Duration.ZERO;
    public static final RecordHeader ENCRYPTION_V1 = new RecordHeader(InBandKeyManager.ENCRYPTION_HEADER_NAME, new byte[]{ 1 });

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
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 500_000, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        TestingRecord record = new TestingRecord(value);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record);
        assertThat(km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)), initial, recordReceivedRecord(encrypted)))
                .isCompleted();
        record.value().rewind();
        assertEquals(1, encrypted.size());
        assertNotEquals(initial, encrypted);
        // TODO add assertion on headers

        List<TestingRecord> decrypted = new ArrayList<>();
        assertThat(km.decrypt("foo", 1, encrypted, recordReceivedRecord(decrypted)))
                .isCompleted();

        assertEquals(initial, decrypted);
    }

    @Test
    void shouldTolerateEncryptingAndDecryptingEmptyRecordValue() {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 500_000, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{});
        TestingRecord record = new TestingRecord(value);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record);
        assertThat(km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)), initial, recordReceivedRecord(encrypted)))
                .isCompleted();
        record.value().rewind();
        assertEquals(1, encrypted.size());
        assertNotEquals(initial, encrypted);

        List<TestingRecord> decrypted = new ArrayList<>();
        assertThat(km.decrypt("foo", 1, encrypted, recordReceivedRecord(decrypted)))
                .isCompleted();

        assertEquals(initial, decrypted);
    }

    // we do not want to break compaction tombstoning by creating a parcel for the null value case
    @Test
    void nullRecordValuesShouldNotBeModifiedAtEncryptTime() {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 500_000, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var kekId = kms.generateKey();

        TestingRecord record = new TestingRecord(null);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record);
        assertThat(km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)), initial, recordReceivedRecord(encrypted)))
                .isCompleted();
        assertEquals(1, encrypted.size());
        assertFalse(encrypted.get(0).hasValue());
    }

    // we do not want to break compaction tombstoning by creating a parcel for the null value case,
    // but currently we do not have a scheme for how to serialize headers when the original record
    // value is null.
    @Test
    void nullRecordValuesAreIncompatibleWithHeaderEncryption() {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 500_000, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var kekId = kms.generateKey();

        var headers = new Header[]{ new RecordHeader("headerFoo", new byte[]{ 4, 5, 6 }) };
        TestingRecord record = new TestingRecord(null, headers);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record);
        String expectedMessage = "encrypting headers prohibited when original record value null, we must preserve the null for tombstoning";
        assertThat(km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_HEADER_VALUES)), initial, recordReceivedRecord(encrypted)))
                .failsWithin(Duration.ofSeconds(5)).withThrowableThat()
                .withMessageContaining(expectedMessage);
    }

    @Test
    void shouldTolerateEncryptingEmptyBatch() {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 500_000, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var kekId = kms.generateKey();

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of();
        assertThat(km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)), initial, recordReceivedRecord(encrypted)))
                .isCompleted();

        assertEquals(0, encrypted.size());
    }

    @Test
    void encryptionRetry() {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var kekId = kms.generateKey();
        // configure 1 encryption per dek but then try to encrypt 2 records, will destroy and retry
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 1, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var value2 = ByteBuffer.wrap(new byte[]{ 4, 5, 6 });
        TestingRecord record = new TestingRecord(value);
        TestingRecord record2 = new TestingRecord(value2);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record, record2);
        CompletionStage encrypt = km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted));
        assertThat(encrypt).failsWithin(Duration.ofSeconds(5)).withThrowableThat()
                .withMessageMatching(".*failed to reserve an EDEK to encrypt 2 records for topic topic partition 1 after [0-9]+ attempts");
    }

    @Test
    void dekCreationRetryFailurePropagatedToEncryptCompletionStage() {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var kekId = kms.generateKey();
        InMemoryKms spyKms = Mockito.spy(kms);
        when(spyKms.generateDekPair(kekId)).thenReturn(CompletableFuture.failedFuture(new EncryptorCreationException("failed to create that DEK")));
        var km = new InBandKeyManager<>(spyKms, BufferPool.allocating(), 500000, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var value2 = ByteBuffer.wrap(new byte[]{ 4, 5, 6 });
        TestingRecord record = new TestingRecord(value);
        TestingRecord record2 = new TestingRecord(value2);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record, record2);
        CompletionStage encrypt = km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted));
        assertThat(encrypt).failsWithin(Duration.ofSeconds(5)).withThrowableThat().withMessageMatching(".*generateDekPair failed after [0-9]+ attempts");
    }

    @Test
    void dekCreationRetrySuccess() {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var kekId = kms.generateKey();
        InMemoryKms spyKms = Mockito.spy(kms);
        when(spyKms.generateDekPair(kekId)).thenReturn(CompletableFuture.failedFuture(new EncryptorCreationException("failed to create that DEK")),
                CompletableFuture.failedFuture(new EncryptorCreationException("failed to create that DEK"))).thenCallRealMethod();
        var km = new InBandKeyManager<>(spyKms, BufferPool.allocating(), 50000, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var value2 = ByteBuffer.wrap(new byte[]{ 4, 5, 6 });
        TestingRecord record = new TestingRecord(value);
        TestingRecord record2 = new TestingRecord(value2);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record, record2);
        CompletionStage encrypt = km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted));
        assertThat(encrypt).succeedsWithin(Duration.ofSeconds(5));
        assertThat(encrypted).hasSize(2);
    }

    @Test
    void edekDecryptionRetryFailurePropagatedToDecryptCompletionStage() {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var kekId = kms.generateKey();
        Kms spyKms = Mockito.spy(kms);
        doReturn(CompletableFuture.failedFuture(new KmsException("failed to create that DEK"))).when(spyKms).decryptEdek(any());

        var km = new InBandKeyManager<>(spyKms, BufferPool.allocating(), 50000, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var value2 = ByteBuffer.wrap(new byte[]{ 4, 5, 6 });
        TestingRecord record = new TestingRecord(value);
        TestingRecord record2 = new TestingRecord(value2);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record, record2);
        CompletionStage encrypt = km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted));
        assertThat(encrypt).succeedsWithin(Duration.ofSeconds(5));

        List<TestingRecord> decrypted = new ArrayList<>();
        CompletionStage decrypt = km.decrypt("topic", 1, encrypted, recordReceivedRecord(decrypted));
        assertThat(decrypt).failsWithin(Duration.ofSeconds(5)).withThrowableThat().withMessageMatching(".*decryptEdek failed after [0-9]+ attempts");
    }

    @Test
    void afterWeFailToLoadADekTheNextEncryptionAttemptCanSucceed() {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var kekId = kms.generateKey();
        InMemoryKms spyKms = Mockito.spy(kms);
        when(spyKms.generateDekPair(kekId)).thenReturn(CompletableFuture.failedFuture(new KmsException("failed to create that DEK")));

        var km = new InBandKeyManager<>(spyKms, BufferPool.allocating(), 50000, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var value2 = ByteBuffer.wrap(new byte[]{ 4, 5, 6 });
        TestingRecord record = new TestingRecord(value);
        TestingRecord record2 = new TestingRecord(value2);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record, record2);
        CompletionStage encrypt = km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted));
        assertThat(encrypt).failsWithin(Duration.ofSeconds(5)).withThrowableThat().withMessageMatching(".*generateDekPair failed after [0-9]+ attempts");

        // given KMS is no longer generating failed futures
        when(spyKms.generateDekPair(kekId)).thenCallRealMethod();

        // when
        CompletionStage encrypt2 = km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted));

        // then
        assertThat(encrypt2).succeedsWithin(Duration.ofSeconds(5));
        assertThat(encrypted).hasSize(2);
    }

    @Test
    void decryptionFailure() {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var kekId = kms.generateKey();
        Kms spyKms = Mockito.spy(kms);
        doReturn(CompletableFuture.failedFuture(new KmsException("failed to create that DEK"))).when(spyKms).decryptEdek(any());

        var km = new InBandKeyManager<>(spyKms, BufferPool.allocating(), 50000, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var value2 = ByteBuffer.wrap(new byte[]{ 4, 5, 6 });
        TestingRecord record = new TestingRecord(value);
        TestingRecord record2 = new TestingRecord(value2);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record, record2);
        CompletionStage encrypt = km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted));
        assertThat(encrypt).succeedsWithin(Duration.ofSeconds(5));

        List<TestingRecord> decrypted = new ArrayList<>();
        CompletionStage decrypt = km.decrypt("topic", 1, encrypted, recordReceivedRecord(decrypted));
        assertThat(decrypt).failsWithin(Duration.ofSeconds(5)).withThrowableThat().withMessageMatching(".*decryptEdek failed after [0-9]+ attempts");
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
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 500_000, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        TestingRecord record = new TestingRecord(value);

        var value2 = ByteBuffer.wrap(new byte[]{ 3, 4, 5 });
        TestingRecord record2 = new TestingRecord(value2);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record, record2);
        km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                (r, v, h) -> {
                    encrypted.add(new TestingRecord(copyBytes(v), h));
                })
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        record.value().rewind();
        record2.value().rewind();
        assertEquals(2, encrypted.size());
        assertNotEquals(initial, encrypted);
        // TODO add assertion on headers

        List<TestingRecord> decrypted = new ArrayList<>();
        km.decrypt("foo", 1, encrypted, recordReceivedRecord(decrypted))
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);

        assertEquals(initial, decrypted);
    }

    @Test
    void shouldGenerateNewDekIfOldDekHasNoRemainingEncryptions() throws ExecutionException, InterruptedException, TimeoutException {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 2, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        TestingRecord record = new TestingRecord(value);

        var value2 = ByteBuffer.wrap(new byte[]{ 3, 4, 5 });
        TestingRecord record2 = new TestingRecord(value2);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record, record2);
        km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted)).toCompletableFuture().get(10, TimeUnit.SECONDS);
        record.value().rewind();
        record2.value().rewind();

        // at this point we have encrypted 2 records with the manager set to maximum 2 encryptions per dek

        km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted)).toCompletableFuture().get(10, TimeUnit.SECONDS);

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
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 3, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        TestingRecord record = new TestingRecord(value);

        var value2 = ByteBuffer.wrap(new byte[]{ 3, 4, 5 });
        TestingRecord record2 = new TestingRecord(value2);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record, record2);
        km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted))
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        record.value().rewind();
        record2.value().rewind();

        // at this point we have encrypted 2 records with the manager set to maximum 3 encryptions per dek, so we need a new dek to encrypt 2 more records

        km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted)).toCompletableFuture().get(10, TimeUnit.SECONDS);

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
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 4, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        TestingRecord record = new TestingRecord(value);

        var value2 = ByteBuffer.wrap(new byte[]{ 3, 4, 5 });
        TestingRecord record2 = new TestingRecord(value2);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record, record2);
        km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted))
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        record.value().rewind();
        record2.value().rewind();

        // at this point we have encrypted 2 records with the manager set to maximum 4 encryptions per dek, so we do not need a new dek to encrypt 2 more records

        km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE)),
                initial,
                recordReceivedRecord(encrypted)).toCompletableFuture().get(10, TimeUnit.SECONDS);

        record.value().rewind();
        record2.value().rewind();
        assertThat(kms.numDeksGenerated()).isEqualTo(1);
        var edekOne = getSerializedGeneratedEdek(kms, 0);
        assertThat(encrypted).hasSize(4);
        List<TestingDek> deks = extractEdeks(encrypted);
        assertThat(deks).containsExactly(edekOne, edekOne, edekOne, edekOne);
    }

    @NonNull
    private static ByteBuffer copyBytes(ByteBuffer v) {
        if (v == null) {
            return null;
        }
        byte[] bytes = new byte[v.remaining()];
        v.get(bytes);
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        return wrap;
    }

    @Test
    void shouldEncryptRecordHeaders() {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 500_000, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var headers = new Header[]{ new RecordHeader("headerFoo", new byte[]{ 4, 5, 6 }) };
        TestingRecord record = new TestingRecord(value, headers);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record);
        assertThat(km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE, RecordField.RECORD_HEADER_VALUES)),
                initial,
                recordReceivedRecord(encrypted)))
                .isCompleted();
        value.rewind();

        assertEquals(1, encrypted.size());
        assertNotEquals(initial, encrypted);

        List<TestingRecord> decrypted = new ArrayList<>();
        assertThat(km.decrypt("topciFoo", 1, encrypted, recordReceivedRecord(decrypted)))
                .isCompleted();

        assertEquals(List.of(new TestingRecord(value, new Header[]{ new RecordHeader("headerFoo", new byte[]{ 4, 5, 6 }) })), decrypted);
    }

    @Test
    void shouldEncryptRecordHeadersForMultipleRecords() throws ExecutionException, InterruptedException, TimeoutException {
        var kmsService = UnitTestingKmsService.newInstance();
        InMemoryKms kms = kmsService.buildKms(new UnitTestingKmsService.Config());
        var km = new InBandKeyManager<>(kms, BufferPool.allocating(), 500_000, EXECUTOR_SERVICE,
                BACKOFF_STRATEGY);

        var kekId = kms.generateKey();

        var value = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
        var headers = new Header[]{ new RecordHeader("foo", new byte[]{ 4, 5, 6 }) };
        TestingRecord record = new TestingRecord(value, headers);
        var value2 = ByteBuffer.wrap(new byte[]{ 7, 8, 9 });
        var headers2 = new Header[]{ new RecordHeader("foo", new byte[]{ 10, 11, 12 }) };
        TestingRecord record2 = new TestingRecord(value2, headers2);

        List<TestingRecord> encrypted = new ArrayList<>();
        List<TestingRecord> initial = List.of(record, record2);
        km.encrypt("topic", 1, new EncryptionScheme<>(kekId, EnumSet.of(RecordField.RECORD_VALUE, RecordField.RECORD_HEADER_VALUES)),
                initial,
                recordReceivedRecord(encrypted)).toCompletableFuture().get(10, TimeUnit.SECONDS);
        value.rewind();
        value2.rewind();
        assertEquals(2, encrypted.size());
        assertNotEquals(initial, encrypted);

        List<TestingRecord> decrypted = new ArrayList<>();
        assertThat(km.decrypt("foo", 1, encrypted, recordReceivedRecord(decrypted)))
                .isCompleted();

        assertEquals(List.of(new TestingRecord(value, new Header[]{ new RecordHeader("foo", new byte[]{ 4, 5, 6 }) }),
                new TestingRecord(value2, new Header[]{ new RecordHeader("foo", new byte[]{ 10, 11, 12 }) })), decrypted);
    }

    public TestingDek getSerializedGeneratedEdek(InMemoryKms kms, int i) {
        DekPair generatedEdek = kms.getGeneratedEdek(i);
        var edek = generatedEdek.edek();
        Serde serde = kms.edekSerde();
        int size = serde.sizeOf(edek);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        serde.serialize(edek, buffer);
        buffer.flip();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new TestingDek(bytes);
    }

    @NonNull
    private static List<TestingDek> extractEdeks(List<TestingRecord> encrypted) {
        List<TestingDek> deks = encrypted.stream()
                .filter(testingRecord -> Stream.of(testingRecord.headers()).anyMatch(header -> header.key().equals(InBandKeyManager.ENCRYPTION_HEADER_NAME)))
                .map(testingRecord -> {
                    ByteBuffer wrapper = testingRecord.value();
                    var edekLength = ByteUtils.readUnsignedVarint(wrapper);
                    byte[] edekBytes = new byte[edekLength];
                    wrapper.get(edekBytes);
                    return new TestingDek(edekBytes);
                })
                .toList();
        return deks;
    }

}
