/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.matcher.AssertionMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.hamcrest.MockitoHamcrest;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.test.record.RecordTestUtils;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.condition.kafka.FetchResponseDataCondition.fetchResponseMatching;
import static io.kroxylicious.test.condition.kafka.ProduceRequestDataCondition.produceRequestMatching;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EnvelopeEncryptionFilterTest {

    private static final String UNENCRYPTED_TOPIC = "unencrypted";
    private static final String ENCRYPTED_TOPIC = "encrypt_me";
    private static final String KEK_ID_1 = "KEK_ID_1";
    private static final byte[] HELLO_PLAIN_WORLD = "Hello World".getBytes(UTF_8);
    private static final byte[] HELLO_CIPHER_WORLD = "Hello Ciphertext World!".getBytes(UTF_8);

    private static final byte[] ENCRYPTED_MESSAGE_BYTES = "xslkajfd;ljsaefjjKLDJlkDSJFLJK';,kSDKF'".getBytes(UTF_8);

    @Mock(strictness = LENIENT)
    KeyManager<String> keyManager;

    @Mock(strictness = LENIENT)
    TopicNameBasedKekSelector<String> kekSelector;

    @Mock(strictness = LENIENT)
    private FilterContext context;

    @Captor
    private ArgumentCaptor<ApiMessage> apiMessageCaptor;

    private EnvelopeEncryptionFilter<String> encryptionFilter;

    @BeforeEach
    void setUp() {
        when(context.forwardRequest(any(RequestHeaderData.class), apiMessageCaptor.capture())).then(invocationOnMock -> {
            final RequestFilterResult filterResult = mock(RequestFilterResult.class);
            return CompletableFuture.completedFuture(filterResult);
        });

        when(context.forwardResponse(any(ResponseHeaderData.class), apiMessageCaptor.capture())).then(invocationOnMock -> {
            final ResponseFilterResult filterResult = mock(ResponseFilterResult.class);
            return CompletableFuture.completedFuture(filterResult);
        });

        when(context.createByteBufferOutputStream(anyInt())).thenAnswer(invocationOnMock -> {
            final int capacity = invocationOnMock.getArgument(0);
            return new ByteBufferOutputStream(capacity);
        });

        final Map<String, String> topicNameToKekId = new HashMap<>();
        topicNameToKekId.put(UNENCRYPTED_TOPIC, null);
        topicNameToKekId.put(ENCRYPTED_TOPIC, KEK_ID_1);

        when(kekSelector.selectKek(anySet())).thenAnswer(invocationOnMock -> {
            Set<String> wanted = invocationOnMock.getArgument(0);
            var copy = new HashMap<>(topicNameToKekId);
            copy.keySet().retainAll(wanted);
            return CompletableFuture.completedFuture(copy);
        });

        when(keyManager.encrypt(any(), anyInt(), any(), anyList(), any(Receiver.class))).thenAnswer(invocationOnMock -> {
            final List<? extends Record> actualRecords = invocationOnMock.getArgument(3);
            final Receiver receiver = invocationOnMock.getArgument(4);
            for (Record actualRecord : actualRecords) {
                receiver.accept(actualRecord, ByteBuffer.allocate(actualRecord.sizeInBytes()), new Header[0]);
            }
            return CompletableFuture.completedFuture(null);
        });

        when(keyManager.decrypt(any(), anyInt(), anyList(), any(Receiver.class))).thenAnswer(invocationOnMock -> {
            final List<? extends Record> actualRecords = invocationOnMock.getArgument(2);
            final Receiver receiver = invocationOnMock.getArgument(3);
            for (Record actualRecord : actualRecords) {
                receiver.accept(actualRecord, ByteBuffer.allocate(actualRecord.sizeInBytes()), new Header[0]);
            }
            return CompletableFuture.completedFuture(null);
        });

        encryptionFilter = new EnvelopeEncryptionFilter<>(keyManager, kekSelector);
    }

    @Test
    void shouldNotEncryptTopicWithoutKeyId() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setName(UNENCRYPTED_TOPIC)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), produceRequestData, context);

        // Then
        verify(keyManager, never()).encrypt(any(), anyInt(), any(), anyList(), any(Receiver.class));
    }

    @Test
    void shouldEncryptTopicWithKeyId() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setName(ENCRYPTED_TOPIC)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), produceRequestData, context);

        // Then
        verify(keyManager).encrypt(any(), anyInt(), any(), anyList(), any(Receiver.class));
    }

    @Test
    void shouldOnlyEncryptTopicWithKeyId() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setName(ENCRYPTED_TOPIC)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))),
                new TopicProduceData()
                        .setName(UNENCRYPTED_TOPIC)
                        .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), produceRequestData, context);

        // Then
        verify(keyManager).encrypt(any(), anyInt(), any(),
                argThat(records -> assertThat(records)
                        .hasSize(1)
                        .allSatisfy(record -> assertThat(record.value()).isEqualTo(ByteBuffer.wrap(HELLO_CIPHER_WORLD)))),
                any());
    }

    @Test
    void produceShouldMaintainClientOffsets() {
        // Given
        var offsets = List.of(0L, 2L, 3L);
        var recsWithNonConsecutiveOffsets = makeRecords(offsets.stream().mapToLong(Long::longValue),
                (u) -> RecordTestUtils.record(ByteBuffer.wrap(HELLO_PLAIN_WORLD)));
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setName(ENCRYPTED_TOPIC)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(recsWithNonConsecutiveOffsets))));

        when(keyManager.encrypt(any(), anyInt(), any(), assertArg(records -> assertThat(records).hasSize(3)), any(Receiver.class))).thenAnswer(invocationOnMock -> {
            final List<Record> records = invocationOnMock.getArgument(3);
            final Receiver receiver = invocationOnMock.getArgument(4);

            records.forEach(rec -> receiver.accept(rec, ByteBuffer.wrap(HELLO_CIPHER_WORLD), Record.EMPTY_HEADERS));
            return CompletableFuture.completedFuture(null);
        });

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION,
                new RequestHeaderData(), produceRequestData, context);

        // Then
        verify(context, times(1)).forwardRequest(any(RequestHeaderData.class), assertArg(actualFetchResponse -> assertThat(actualFetchResponse)
                .has(produceRequestMatching(prd -> {
                    var actuals = produceRequestToRecordStream(prd).map(Record::offset).toList();
                    return Objects.equals(actuals, offsets);
                }))));
    }

    @NonNull
    private static Stream<Record> produceRequestToRecordStream(ProduceRequestData fetchResponseData) {
        return fetchResponseData.topicData().stream()
                .flatMap(pd -> pd.partitionData().stream())
                .map(PartitionProduceData::records)
                .map(Records.class::cast)
                .flatMap(r -> StreamSupport.stream(r.records().spliterator(), false));
    }

    @Test
    void shouldPassThroughUnencryptedRecords() {
        // Given
        var fetchResponseData = buildFetchResponseData(new FetchableTopicResponse()
                .setTopic(UNENCRYPTED_TOPIC)
                .setPartitions(List.of(new PartitionData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));

        // When
        encryptionFilter.onFetchResponse(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
                new ResponseHeaderData(), fetchResponseData, context);

        // Then
        verify(context).forwardResponse(any(ResponseHeaderData.class), assertArg(actualFetchResponse -> assertThat(actualFetchResponse)
                .isInstanceOf(FetchResponseData.class).isEqualTo(fetchResponseData)));
    }

    @Test
    void shouldDecryptEncryptedRecords() {
        // Given
        var encryptedFetchResponse = buildFetchResponseData(new FetchableTopicResponse()
                .setTopic(ENCRYPTED_TOPIC)
                .setPartitions(List.of(new PartitionData().setRecords(makeRecord(ENCRYPTED_MESSAGE_BYTES)))));

        when(keyManager.decrypt(any(), anyInt(), assertArg(records -> assertThat(records).hasSize(1)), any(Receiver.class))).thenAnswer(invocationOnMock -> {
            final List<Record> records = invocationOnMock.getArgument(2);
            final Receiver receiver = invocationOnMock.getArgument(3);
            receiver.accept(records.get(0), ByteBuffer.wrap(HELLO_PLAIN_WORLD), Record.EMPTY_HEADERS);

            return CompletableFuture.completedFuture(null);
        });

        // When
        encryptionFilter.onFetchResponse(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
                new ResponseHeaderData(), encryptedFetchResponse, context);

        // Then
        verify(context).forwardResponse(any(ResponseHeaderData.class), assertArg(actualFetchResponse -> assertThat(actualFetchResponse)
                .isInstanceOf(FetchResponseData.class)
                .asInstanceOf(InstanceOfAssertFactories.type(FetchResponseData.class))
        // .has(new FetchResponseDataCondition(fetchResponseData -> true)) //This is where the new conditions from https://github.com/kroxylicious/kroxylicious/pull/756 come in
        ));
    }

    @Test
    void fetchShouldMaintainBrokerOffsets() {
        // Given
        var offsets = List.of(0L, 2L, 3L);
        var recsWithNonConsecutiveOffsets = makeRecords(offsets.stream().mapToLong(Long::longValue), (u) -> RecordTestUtils.record(ByteBuffer.wrap(HELLO_CIPHER_WORLD)));
        var encryptedFetchResponse = buildFetchResponseData(new FetchableTopicResponse()
                .setTopic(ENCRYPTED_TOPIC)
                .setPartitions(List.of(new PartitionData().setRecords(recsWithNonConsecutiveOffsets))));

        when(keyManager.decrypt(any(), anyInt(), assertArg(records -> assertThat(records).hasSize(3)), any(Receiver.class))).thenAnswer(invocationOnMock -> {
            final List<Record> records = invocationOnMock.getArgument(2);
            final Receiver receiver = invocationOnMock.getArgument(3);

            records.forEach(rec -> receiver.accept(rec, ByteBuffer.wrap(HELLO_PLAIN_WORLD), Record.EMPTY_HEADERS));
            return CompletableFuture.completedFuture(null);
        });

        // When
        encryptionFilter.onFetchResponse(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
                new ResponseHeaderData(), encryptedFetchResponse, context);

        // Then
        verify(context, times(1)).forwardResponse(any(ResponseHeaderData.class), assertArg(actualFetchResponse -> assertThat(actualFetchResponse)
                .has(fetchResponseMatching(fetchResponseData -> {
                    var actuals = fetchResponseToRecordStream(fetchResponseData).map(Record::offset).toList();
                    return Objects.equals(actuals, offsets);
                }))));
    }

    @NonNull
    private static Stream<Record> fetchResponseToRecordStream(FetchResponseData fetchResponseData) {
        return fetchResponseData.responses().stream()
                .flatMap(pd -> pd.partitions().stream())
                .map(PartitionData::records)
                .map(Records.class::cast)
                .flatMap(r -> StreamSupport.stream(r.records().spliterator(), false));
    }

    @Test
    void shouldEncryptTopic() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setName(ENCRYPTED_TOPIC)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))));

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), produceRequestData, context);

        // Then
        verify(context).forwardRequest(any(), argThat(request -> assertThat(request)
                .has(produceRequestMatching(pr -> pr.topicData().stream().anyMatch(td -> ENCRYPTED_TOPIC.equals(td.name()))))));
    }

    private static FetchResponseData buildFetchResponseData(FetchableTopicResponse... topicResponses) {
        var data = new FetchResponseData();
        data.responses().addAll(Arrays.asList(topicResponses));
        return data;
    }

    private static ProduceRequestData buildProduceRequestData(TopicProduceData... produceData) {
        var data = new ProduceRequestData();
        data.topicData().addAll(Arrays.asList(produceData));
        return data;

    }

    private static MemoryRecords makeRecord(byte[] payload) {
        return makeRecords(LongStream.of(0), u -> RecordTestUtils.record(ByteBuffer.wrap(payload), new RecordHeader("myKey", "myValue".getBytes())));
    }

    private static MemoryRecords makeRecords(LongStream offsets, Function<Long, Record> messageFunc) {
        var stream = new ByteBufferOutputStream(ByteBuffer.allocate(1000));

        var recordsBuilder = memoryRecordsBuilderForStream(stream);
        offsets.forEach(offset -> {
            recordsBuilder.appendWithOffset(offset, messageFunc.apply(offset));
        });

        return recordsBuilder.build();
    }

    @NonNull
    private static MemoryRecordsBuilder memoryRecordsBuilderForStream(ByteBufferOutputStream stream) {
        return new MemoryRecordsBuilder(stream, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, TimestampType.CREATE_TIME, 0,
                RecordBatch.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, stream.remaining());
    }

    public static <T> T argThat(Consumer<T> assertions) {
        return MockitoHamcrest.argThat(new AssertionMatcher<>() {

            String underlyingDescription;

            @Override
            public void assertion(T actual) throws AssertionError {
                AbstractAssert.setDescriptionConsumer(description -> underlyingDescription = description.value());
                assertions.accept(actual);
            }

            @Override
            public void describeTo(org.hamcrest.Description description) {
                super.describeTo(description);
                description.appendValue(Objects.requireNonNullElse(underlyingDescription, "custom argument matcher"));
            }
        });
    }
}
