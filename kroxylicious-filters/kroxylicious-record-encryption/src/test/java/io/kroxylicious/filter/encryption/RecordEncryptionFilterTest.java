/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.LongStream;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.NetworkException;
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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.matcher.AssertionMatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.hamcrest.MockitoHamcrest;
import org.mockito.junit.jupiter.MockitoExtension;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import io.kroxylicious.filter.encryption.common.FilterThreadExecutor;
import io.kroxylicious.filter.encryption.config.TopicNameBasedKekSelector;
import io.kroxylicious.filter.encryption.config.TopicNameKekSelection;
import io.kroxylicious.filter.encryption.config.UnresolvedKeyPolicy;
import io.kroxylicious.filter.encryption.decrypt.DecryptionManager;
import io.kroxylicious.filter.encryption.encrypt.EncryptionManager;
import io.kroxylicious.filter.encryption.encrypt.RequestNotSatisfiable;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;
import io.kroxylicious.test.record.RecordTestUtils;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.condition.kafka.ProduceRequestDataCondition.produceRequestMatching;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RecordEncryptionFilterTest {

    private static final String UNRESOLVED_TOPIC = "unresolved";
    private static final String ENCRYPTED_TOPIC = "encrypt_me";
    private static final String KEK_ID_1 = "KEK_ID_1";
    private static final byte[] HELLO_PLAIN_WORLD = "Hello World".getBytes(UTF_8);
    private static final byte[] HELLO_CIPHER_WORLD = "Hello Ciphertext World!".getBytes(UTF_8);

    private static final byte[] ENCRYPTED_MESSAGE_BYTES = "xslkajfd;ljsaefjjKLDJlkDSJFLJK';,kSDKF'".getBytes(UTF_8);

    @Mock(strictness = LENIENT)
    EncryptionManager<String> encryptionManager;

    @Mock(strictness = LENIENT)
    DecryptionManager decryptionManager;

    @Mock(strictness = LENIENT)
    TopicNameBasedKekSelector<String> kekSelector;

    @Mock(strictness = LENIENT)
    private FilterContext context;

    @Mock(strictness = LENIENT)
    RequestFilterResultBuilder requestFilterResultBuilder;

    @Mock(strictness = LENIENT)
    private CloseOrTerminalStage<RequestFilterResult> closeOrTerminalStage;

    @Captor
    private ArgumentCaptor<ApiMessage> apiMessageCaptor;

    private RecordEncryptionFilter<String> encryptionFilter;
    private SimpleMeterRegistry simpleMeterRegistry;

    @BeforeEach
    void setUp() {
        when(context.getVirtualClusterName()).thenReturn("test");

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
        topicNameToKekId.put(ENCRYPTED_TOPIC, KEK_ID_1);

        when(kekSelector.selectKek(anySet())).thenAnswer(invocationOnMock -> {
            Set<String> wanted = invocationOnMock.getArgument(0);
            var copy = new HashMap<>(topicNameToKekId);
            copy.keySet().retainAll(wanted);
            return CompletableFuture.completedFuture(new TopicNameKekSelection<>(topicNameToKekId, Set.of(UNRESOLVED_TOPIC)));
        });

        when(encryptionManager.encrypt(any(), anyInt(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(RecordTestUtils.singleElementMemoryRecords("key", "value")));

        when(decryptionManager.decrypt(any(), anyInt(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(RecordTestUtils.singleElementMemoryRecords("decrypt", "decrypt")));

        encryptionFilter = new RecordEncryptionFilter<>(encryptionManager, decryptionManager, kekSelector, new FilterThreadExecutor(Runnable::run),
                UnresolvedKeyPolicy.PASSTHROUGH_UNENCRYPTED);

        simpleMeterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry.add(simpleMeterRegistry);
    }

    @AfterEach
    void tearDown() {
        if (simpleMeterRegistry != null) {
            simpleMeterRegistry.getMeters().forEach(Metrics.globalRegistry::remove);
            Metrics.globalRegistry.remove(simpleMeterRegistry);
        }
    }

    @Test
    void shouldNotEncryptTopicWithoutKeyId() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setName(UNRESOLVED_TOPIC)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), produceRequestData, context);

        // Then
        verify(encryptionManager, never()).encrypt(any(), anyInt(), any(), any(), any());
    }

    @Test
    void shouldCountPlainAndEncryptedRecords() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setName(ENCRYPTED_TOPIC)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))),
                new TopicProduceData()
                        .setName(UNRESOLVED_TOPIC)
                        .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), produceRequestData, context);

        // Then
        assertThat(Metrics.globalRegistry.get("kroxylicious_filter_record_encryption_plain_records").counter())
                .isNotNull()
                .satisfies(counter -> assertThat(counter.getId()).isNotNull())
                .satisfies(counter -> assertThat(counter.count())
                        .isGreaterThan(0));

        assertThat(Metrics.globalRegistry.get("kroxylicious_filter_record_encryption_encrypted_records").counter())
                .isNotNull()
                .satisfies(counter -> assertThat(counter.getId()).isNotNull())
                .satisfies(counter -> assertThat(counter.count())
                        .isGreaterThan(0));
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
        verify(encryptionManager).encrypt(any(), anyInt(), any(), any(), any());
    }

    @Test
    void shouldOnlyEncryptTopicWithKeyId() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setName(ENCRYPTED_TOPIC)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))),
                new TopicProduceData()
                        .setName(UNRESOLVED_TOPIC)
                        .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, new RequestHeaderData(), produceRequestData, context);

        // Then
        verify(encryptionManager).encrypt(any(), anyInt(), any(),
                argThat(records -> assertThat(records.records())
                        .hasSize(1)
                        .allSatisfy(record -> assertThat(record.value()).isEqualTo(ByteBuffer.wrap(HELLO_CIPHER_WORLD)))),
                any());
    }

    @Test
    void shouldRejectWholeMessageIfPolicyIsToRejectUnresolvedKeys() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setName(ENCRYPTED_TOPIC)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))),
                new TopicProduceData()
                        .setName(UNRESOLVED_TOPIC)
                        .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));

        when(context.requestFilterResultBuilder()).thenReturn(requestFilterResultBuilder);
        when(requestFilterResultBuilder.errorResponse(any(), any(), any())).thenReturn(closeOrTerminalStage);
        RuntimeException exception = new RuntimeException("exception");
        CompletableFuture<RequestFilterResult> completed = CompletableFuture.failedFuture(exception);
        when(closeOrTerminalStage.completed()).thenReturn(completed);

        // When
        var rejectUnresolvedFilter = new RecordEncryptionFilter<>(encryptionManager, decryptionManager, kekSelector, new FilterThreadExecutor(Runnable::run),
                UnresolvedKeyPolicy.REJECT);
        CompletionStage<RequestFilterResult> stage = rejectUnresolvedFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION,
                new RequestHeaderData(), produceRequestData, context);

        // Then
        verify(encryptionManager, never()).encrypt(any(), anyInt(), any(), any(), any());
        verify(requestFilterResultBuilder).errorResponse(any(), any(),
                Mockito.argThat(e -> e instanceof InvalidRecordException && e.getMessage().equals("failed to resolve key for: [unresolved]")));

        assertThat(stage).failsWithin(0, TimeUnit.SECONDS).withThrowableThat().withCause(exception);

    }

    @Test
    void shouldPassThroughUnencryptedRecords() {
        // Given
        var fetchResponseData = buildFetchResponseData(new FetchableTopicResponse()
                .setTopic(UNRESOLVED_TOPIC)
                .setPartitions(List.of(new PartitionData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));

        // When
        encryptionFilter.onFetchResponse(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
                new ResponseHeaderData(), fetchResponseData, context);

        // Then
        verify(context).forwardResponse(any(ResponseHeaderData.class), assertArg(actualFetchResponse -> assertThat(actualFetchResponse)
                .isInstanceOf(FetchResponseData.class).isEqualTo(fetchResponseData)));
    }

    @Test
    void shouldPropagateRequestExceptions() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setName(ENCRYPTED_TOPIC)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))));
        RuntimeException exception = new RuntimeException("boom");
        when(kekSelector.selectKek(anySet())).thenReturn(CompletableFuture.failedFuture(exception));

        // When
        CompletionStage<RequestFilterResult> stage = encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION,
                new RequestHeaderData(), produceRequestData, context);

        // Then
        assertThat(stage).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause().isSameAs(exception);
    }

    @Test
    void shouldRespondWithErrorOnRequestNotSatisfiableExceptions() {
        // Given
        final RequestFilterResultBuilder resultBuilder = stubErrorResponse();

        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setName(ENCRYPTED_TOPIC)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))));
        final RequestNotSatisfiable failure = new RequestNotSatisfiable("could not acquire a DEK", new NetworkException("could not acquire a DEK"));

        when(kekSelector.selectKek(anySet())).thenReturn(CompletableFuture.failedFuture(failure));

        // When
        CompletionStage<RequestFilterResult> stage = encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION,
                new RequestHeaderData(), produceRequestData, context);

        // Then
        assertThat(stage)
                .succeedsWithin(Duration.ofSeconds(10));
        verify(context).requestFilterResultBuilder();
        verify(resultBuilder).errorResponse(any(), any(), argThat(throwable -> assertThat(throwable.getClass()).isAssignableTo(ApiException.class)));
    }

    @Test
    void shouldPropagateResponseExceptions() {
        // Given
        var fetchResponseData = buildFetchResponseData(new FetchableTopicResponse()
                .setTopic(UNRESOLVED_TOPIC)
                .setPartitions(List.of(new PartitionData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));
        RuntimeException exception = new RuntimeException("boom");
        when(decryptionManager.decrypt(any(), anyInt(), any(), any())).thenReturn(CompletableFuture.failedFuture(exception));

        // When
        CompletionStage<ResponseFilterResult> stage = encryptionFilter.onFetchResponse(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
                new ResponseHeaderData(), fetchResponseData, context);

        // Then
        assertThat(stage).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause().isSameAs(exception);
    }

    @Test
    void shouldDecryptEncryptedRecords() {
        // Given
        var encryptedFetchResponse = buildFetchResponseData(new FetchableTopicResponse()
                .setTopic(ENCRYPTED_TOPIC)
                .setPartitions(List.of(new PartitionData().setRecords(makeRecord(ENCRYPTED_MESSAGE_BYTES)))));

        MemoryRecords decryptedRecords = RecordTestUtils.singleElementMemoryRecords("key", "value");
        when(decryptionManager.decrypt(any(), anyInt(), any(), any())).thenReturn(CompletableFuture.completedFuture(decryptedRecords));

        // When
        encryptionFilter.onFetchResponse(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
                new ResponseHeaderData(), encryptedFetchResponse, context);

        // Then
        verify(context).forwardResponse(any(ResponseHeaderData.class), assertArg(actualFetchResponse -> {
            assertThat(actualFetchResponse)
                    .isInstanceOf(FetchResponseData.class)
                    .asInstanceOf(InstanceOfAssertFactories.type(FetchResponseData.class));
            FetchResponseData fetchResponse = (FetchResponseData) actualFetchResponse;
            assertThat(fetchResponse.responses()).hasSize(1);
            FetchableTopicResponse onlyTopicResponse = fetchResponse.responses().iterator().next();
            assertThat(onlyTopicResponse.topic()).isEqualTo(ENCRYPTED_TOPIC);
            assertThat(onlyTopicResponse.partitions()).hasSize(1);
            PartitionData onlyPartition = onlyTopicResponse.partitions().iterator().next();
            assertThat(onlyPartition.records()).isSameAs(decryptedRecords);
        }));
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

    @NonNull
    @SuppressWarnings("unchecked")
    private RequestFilterResultBuilder stubErrorResponse() {
        final RequestFilterResultBuilder resultBuilder = mock(RequestFilterResultBuilder.class);
        final CloseOrTerminalStage<RequestFilterResult> errorResponseBuilder = mock(CloseOrTerminalStage.class);
        final RequestFilterResult errorResult = mock(RequestFilterResult.class);
        when(context.requestFilterResultBuilder()).thenReturn(resultBuilder);
        when(resultBuilder.errorResponse(any(), any(), any(ApiException.class))).thenReturn(errorResponseBuilder);
        when(errorResponseBuilder.completed()).thenReturn(CompletableFuture.completedStage(errorResult));
        return resultBuilder;
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
        return new MemoryRecordsBuilder(stream, RecordBatch.CURRENT_MAGIC_VALUE, Compression.NONE, TimestampType.CREATE_TIME, 0,
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
