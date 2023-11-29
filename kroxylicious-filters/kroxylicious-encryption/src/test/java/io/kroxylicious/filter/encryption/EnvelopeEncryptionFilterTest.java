/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
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

import edu.umd.cs.findbugs.annotations.NonNull;

import io.kroxylicious.filter.encryption.inband.TestingRecord;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;

import static io.kroxylicious.filter.encryption.ProduceRequestDataCondition.hasRecordsForTopic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EnvelopeEncryptionFilterTest {

    public static final String UNENCRYPTED_TOPIC = "unencrypted";
    public static final String ENCRYPTED_TOPIC = "encrypt_me";
    public static final String KEK_ID_1 = "KEK_ID_1";
    public static final String ENCRYPTED_MESSAGE = "xslkajfd;ljsaefjjKLDJlkDSJFLJK';,kSDKF'";

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

        when(context.createByteBufferOutputStream(anyInt())).thenAnswer(invocationOnMock -> {
            final int capacity = invocationOnMock.getArgument(0);
            return new ByteBufferOutputStream(capacity);
        });

        final Map<String, String> topicNameToKekId = new HashMap<>();
        topicNameToKekId.put(UNENCRYPTED_TOPIC, null);
        topicNameToKekId.put(ENCRYPTED_TOPIC, KEK_ID_1);
        when(kekSelector.selectKek(anySet())).thenReturn(CompletableFuture.completedFuture(topicNameToKekId));

        when(keyManager.encrypt(any(), anyList(), any(Receiver.class))).thenAnswer(invocationOnMock -> {
            final List<? extends Record> actualRecords = invocationOnMock.getArgument(1);
            final Receiver receiver = invocationOnMock.getArgument(2);
            for (Record actualRecord : actualRecords) {
                receiver.accept(actualRecord, ByteBuffer.allocate(actualRecord.sizeInBytes()), new Header[0]);
            }
            return CompletableFuture.completedFuture(null);
        });

        when(keyManager.decrypt(anyList(), any(Receiver.class))).thenAnswer(invocationOnMock -> {
            final List<? extends Record> actualRecords = invocationOnMock.getArgument(0);
            final Receiver receiver = invocationOnMock.getArgument(1);
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
        final ProduceRequestData produceRequestData = buildProduceRequestData(new Payload(UNENCRYPTED_TOPIC, "Hello World!" ));

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), produceRequestData, context);

        // Then
        verify(keyManager, never()).encrypt(any(), anyList(), any(Receiver.class));
    }

    @Test
    void shouldEncryptTopicWithKeyId() {
        // Given
        final ProduceRequestData produceRequestData = buildProduceRequestData(new Payload(ENCRYPTED_TOPIC, "Hello World!" ));

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), produceRequestData, context);

        // Then
        verify(keyManager).encrypt(any(), anyList(), any(Receiver.class));
    }

    @Test
    void shouldOnlyEncryptTopicWithKeyId() {
        // Given
        final Payload encryptedPayload = new Payload(ENCRYPTED_TOPIC, "Hello Ciphertext World!" );
        final ProduceRequestData produceRequestData = buildProduceRequestData(encryptedPayload,
                new Payload(UNENCRYPTED_TOPIC, "Hello Plaintext World!" ));

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), produceRequestData, context);

        // Then
        verify(keyManager).encrypt(any(),
                argThat(records -> assertThat(records)
                        .hasSize(1)
                        .allSatisfy(record -> assertThat(record.value()).isEqualByComparingTo(encryptedPayload.messageBytes()))),
                any());
    }

    @Test
    void shouldPassThroughUnEncryptedRecords() {
        // Given
        final FetchResponseData fetchResponseData = buildFetchResponseData(new Payload(UNENCRYPTED_TOPIC, "Hello Plaintext World!" ));

        // When
        encryptionFilter.onFetchResponse(ProduceResponseData.HIGHEST_SUPPORTED_VERSION,
                new ResponseHeaderData(), fetchResponseData, context);

        // Then
        verify(context).forwardResponse(any(ResponseHeaderData.class), assertArg(actualFetchResponse -> assertThat(actualFetchResponse)
                .isInstanceOf(FetchResponseData.class).
                isEqualTo(fetchResponseData)));
    }

    @Test
    void shouldDecryptEncryptedRecords() {
        // Given
        final FetchResponseData encryptedFetchResponse = buildFetchResponseData(new Payload(ENCRYPTED_TOPIC, ENCRYPTED_MESSAGE));
        final Payload plainTextPayload = new Payload(ENCRYPTED_TOPIC, "Hello Plaintext World!" );

        when(keyManager.decrypt(assertArg(records -> assertThat(records).hasSize(1)), any(Receiver.class))).thenAnswer(invocationOnMock -> {
            final Receiver receiver = invocationOnMock.getArgument(1);
            receiver.accept(new TestingRecord(plainTextPayload.messageBytes()), plainTextPayload.messageBytes(), Record.EMPTY_HEADERS);

            return CompletableFuture.completedFuture(null);
        });

        // When
        encryptionFilter.onFetchResponse(ProduceResponseData.HIGHEST_SUPPORTED_VERSION,
                new ResponseHeaderData(), encryptedFetchResponse, context);

        // Then
        verify(context).forwardResponse(any(ResponseHeaderData.class), assertArg(actualFetchResponse -> assertThat(actualFetchResponse)
                .isInstanceOf(FetchResponseData.class)
                .asInstanceOf(InstanceOfAssertFactories.type(FetchResponseData.class))
//                .has(new FetchResponseDataCondition(fetchResponseData -> true)) //This is where the new conditions from https://github.com/kroxylicious/kroxylicious/pull/756 come in
        ));
    }

    @Test
    void shouldEncryptTopic() {
        // Given
        final Payload encryptedPayload = new Payload(ENCRYPTED_TOPIC, "Hello Ciphertext World!" );
        final ProduceRequestData produceRequestData = buildProduceRequestData(encryptedPayload,
                new Payload(UNENCRYPTED_TOPIC, "Hello Plaintext World!" ));

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), produceRequestData, context);

        // Then
        verify(context).forwardRequest(any(), argThat(request ->
                assertThat(request).isInstanceOf(ProduceRequestData.class)
                        .asInstanceOf(InstanceOfAssertFactories.type(ProduceRequestData.class))
                        .is(hasRecordsForTopic(ENCRYPTED_TOPIC))
        ));
    }

    private static FetchResponseData buildFetchResponseData(Payload... payloads) {
        final FetchResponseData fetchResponseData = new FetchResponseData();
        var stream = new ByteBufferOutputStream(ByteBuffer.allocate(1000));
        for (Payload payload : payloads) {
            var recordsBuilder = memoryRecordsBuilderForStream(stream);
            final FetchResponseData.FetchableTopicResponse fetchableTopicResponse = new FetchResponseData.FetchableTopicResponse();
            fetchableTopicResponse.setTopic(payload.topicName());
            final FetchResponseData.PartitionData partitionData = new FetchResponseData.PartitionData();
            recordsBuilder.append(new SimpleRecord(System.currentTimeMillis(), null, payload.message.getBytes(StandardCharsets.UTF_8)));
            partitionData.setRecords(recordsBuilder.build());
            fetchableTopicResponse.partitions().add(partitionData);

            fetchResponseData.responses().add(fetchableTopicResponse);
        }
        return fetchResponseData;
    }

    private static ProduceRequestData buildProduceRequestData(Payload... payloads) {
        var requestData = new ProduceRequestData();

        // Build records from stream
        var stream = new ByteBufferOutputStream(ByteBuffer.allocate(1000));
        var topics = new ProduceRequestData.TopicProduceDataCollection();

        for (Payload payload : payloads) {
            var recordsBuilder = memoryRecordsBuilderForStream(stream);

            // Create record Headers
            Header header = new RecordHeader("myKey", "myValue".getBytes());

            // Add transformValue as buffer to records
            recordsBuilder.append(RecordBatch.NO_TIMESTAMP, null, payload.messageBytes(), new Header[]{ header });
            var records = recordsBuilder.build();
            // Build partitions from built records
            var partitions = new ArrayList<ProduceRequestData.PartitionProduceData>();
            var partitionData = new ProduceRequestData.PartitionProduceData();
            partitionData.setRecords(records);
            partitions.add(partitionData);
            // Build topics from built partitions

            var topicData = new ProduceRequestData.TopicProduceData();
            topicData.setPartitionData(partitions);
            topicData.setName(payload.topicName);
            topics.add(topicData);
        }

        // Add built topics to ProduceRequestData object so that we can return it
        requestData.setTopicData(topics);
        return requestData;
    }

    @NonNull
    private static MemoryRecordsBuilder memoryRecordsBuilderForStream(ByteBufferOutputStream stream) {
        return new MemoryRecordsBuilder(stream, RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, TimestampType.CREATE_TIME, 0,
                RecordBatch.NO_TIMESTAMP, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH, stream.remaining());
    }

    private record Payload(String topicName, String message) {
        ByteBuffer messageBytes() {
            return ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8)).position(0);
        }
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
                description.appendValue(Objects.requireNonNullElse(underlyingDescription, "custom argument matcher" ));
            }
        });
    }
}