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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData.ShareFetchableTopicResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.matcher.AssertionMatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
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
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.UnknownKeyException;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.metadata.TopicNameMappingException;
import io.kroxylicious.test.assertj.MockFilterContextAssert;
import io.kroxylicious.test.context.MockFilterContext;
import io.kroxylicious.test.record.RecordTestUtils;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.condition.kafka.ProduceRequestDataCondition.produceRequestMatching;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.atIndex;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RecordEncryptionFilterTest {

    private static final String UNRESOLVED_TOPIC = "unresolved";
    private static final Uuid UNRESOLVED_TOPIC_ID = Uuid.randomUuid();
    private static final String ENCRYPTED_TOPIC = "encrypt_me";
    private static final Uuid ENCRYPTED_TOPIC_ID = Uuid.randomUuid();
    private static final String KEK_ID_1 = "KEK-ID-1";
    private static final byte[] HELLO_PLAIN_WORLD = "Hello World".getBytes(UTF_8);
    private static final byte[] HELLO_CIPHER_WORLD = "Hello Ciphertext World!".getBytes(UTF_8);

    private static final byte[] ENCRYPTED_MESSAGE_BYTES = "xslkajfd;ljsaefjjKLDJlkDSJFLJK';,kSDKF'".getBytes(UTF_8);

    @Mock(strictness = LENIENT)
    EncryptionManager<String> encryptionManager;

    @Mock(strictness = LENIENT)
    DecryptionManager decryptionManager;

    @Mock(strictness = LENIENT)
    TopicNameBasedKekSelector<String> kekSelector;

    private RecordEncryptionFilter<String> encryptionFilter;
    private SimpleMeterRegistry simpleMeterRegistry;

    @BeforeEach
    void setUp() {

        final Map<String, String> topicNameToKekId = new HashMap<>();
        topicNameToKekId.put(ENCRYPTED_TOPIC, KEK_ID_1);

        when(kekSelector.selectKek(anySet()))
                .thenAnswer(invocationOnMock -> CompletableFuture.completedFuture(new TopicNameKekSelection<>(topicNameToKekId, Set.of(UNRESOLVED_TOPIC))));

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

        RequestHeaderData header = new RequestHeaderData();
        MockFilterContext context = MockFilterContext.builder(header, produceRequestData).build();

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, header, produceRequestData, context);

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

        RequestHeaderData header = new RequestHeaderData();
        MockFilterContext context = MockFilterContext.builder(header, produceRequestData).build();

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, header, produceRequestData, context);

        // Then
        assertThat(Metrics.globalRegistry.get("kroxylicious_filter_record_encryption_plain_records").counter())
                .isNotNull()
                .satisfies(counter -> {
                    assertThat(counter.getId()).isNotNull();
                    assertThat(counter.count()).isEqualTo(1);
                });

        assertThat(Metrics.globalRegistry.get("kroxylicious_filter_record_encryption_encrypted_records").counter())
                .isNotNull()
                .satisfies(counter -> {
                    assertThat(counter.getId()).isNotNull();
                    assertThat(counter.count()).isEqualTo(1);
                });
    }

    @Test
    void shouldCountPlainAndEncryptedRecordsWhenTopicIdsUsed() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setTopicId(ENCRYPTED_TOPIC_ID)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))),
                new TopicProduceData()
                        .setTopicId(UNRESOLVED_TOPIC_ID)
                        .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));

        RequestHeaderData header = new RequestHeaderData();
        MockFilterContext context = MockFilterContext.builder(header, produceRequestData).withTopicName(ENCRYPTED_TOPIC_ID, ENCRYPTED_TOPIC)
                .withTopicName(UNRESOLVED_TOPIC_ID, UNRESOLVED_TOPIC).build();

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, header, produceRequestData, context);

        // Then
        assertThat(Metrics.globalRegistry.get("kroxylicious_filter_record_encryption_plain_records").counter())
                .isNotNull()
                .satisfies(counter -> {
                    assertThat(counter.getId()).isNotNull();
                    assertThat(counter.count()).isEqualTo(1);
                });

        assertThat(Metrics.globalRegistry.get("kroxylicious_filter_record_encryption_encrypted_records").counter())
                .isNotNull()
                .satisfies(counter -> {
                    assertThat(counter.getId()).isNotNull();
                    assertThat(counter.count()).isEqualTo(1);
                });
    }

    @Test
    void shouldEncryptTopicWithKeyId() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setName(ENCRYPTED_TOPIC)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));
        RequestHeaderData header = new RequestHeaderData();
        MockFilterContext context = MockFilterContext.builder(header, produceRequestData).build();

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, header, produceRequestData, context);

        // Then
        verify(encryptionManager).encrypt(any(), anyInt(), any(), any(), any());
    }

    @Test
    void shouldEncryptTopicWithTopicId() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setTopicId(ENCRYPTED_TOPIC_ID)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));
        RequestHeaderData header = new RequestHeaderData();
        MockFilterContext context = MockFilterContext.builder(header, produceRequestData).withTopicName(ENCRYPTED_TOPIC_ID, ENCRYPTED_TOPIC).build();

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, header, produceRequestData, context);

        // Then
        verify(encryptionManager).encrypt(eq(ENCRYPTED_TOPIC), anyInt(), any(), any(), any());
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
        RequestHeaderData header = new RequestHeaderData();
        MockFilterContext context = MockFilterContext.builder(header, produceRequestData).build();

        // When
        encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION, header, produceRequestData, context);

        // Then
        verify(encryptionManager).encrypt(any(), anyInt(), any(),
                argThat(records -> assertThat(records.records())
                        .hasSize(1)
                        .allSatisfy(kafkaRecord -> assertThat(kafkaRecord.value()).isEqualTo(ByteBuffer.wrap(HELLO_CIPHER_WORLD)))),
                any());
    }

    @Test
    void shouldRejectProduceIfTopicNameCannotBeMapped() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setTopicId(ENCRYPTED_TOPIC_ID)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))));
        produceRequestData.setAcks((short) 1);

        RequestHeaderData header = new RequestHeaderData();
        MockFilterContext context = MockFilterContext.builder(header, produceRequestData).build();

        // When
        CompletionStage<RequestFilterResult> stage = encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION,
                header, produceRequestData, context);

        // Then
        verify(encryptionManager, never()).encrypt(any(), anyInt(), any(), any(), any());
        assertThat(stage).succeedsWithin(0, TimeUnit.SECONDS).satisfies(requestFilterResult -> {
            MockFilterContextAssert.assertThat(requestFilterResult).isShortCircuitResponse()
                    .isErrorResponse().errorResponse()
                    .isInstanceOf(UnknownTopicIdException.class);
        });

    }

    @Test
    void shouldDropZeroAckProduceIfTopicNameCannotBeMapped() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setTopicId(ENCRYPTED_TOPIC_ID)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))));
        produceRequestData.setAcks((short) 0);

        RequestHeaderData header = new RequestHeaderData();
        MockFilterContext context = MockFilterContext.builder(header, produceRequestData).build();

        // When
        CompletionStage<RequestFilterResult> stage = encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION,
                header, produceRequestData, context);

        // Then
        verify(encryptionManager, never()).encrypt(any(), anyInt(), any(), any(), any());
        assertThat(stage).succeedsWithin(0, TimeUnit.SECONDS).satisfies(requestFilterResult -> {
            MockFilterContextAssert.assertThat(requestFilterResult)
                    .isNotForwardRequest()
                    .isDropRequest();
        });
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

        RequestHeaderData header = new RequestHeaderData();
        MockFilterContext context = MockFilterContext.builder(header, produceRequestData).build();

        // When
        var rejectUnresolvedFilter = new RecordEncryptionFilter<>(encryptionManager, decryptionManager, kekSelector, new FilterThreadExecutor(Runnable::run),
                UnresolvedKeyPolicy.REJECT);
        CompletionStage<RequestFilterResult> stage = rejectUnresolvedFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION,
                header, produceRequestData, context);

        // Then
        verify(encryptionManager, never()).encrypt(any(), anyInt(), any(), any(), any());
        assertThat(stage).succeedsWithin(0, TimeUnit.SECONDS).satisfies(requestFilterResult -> {
            MockFilterContextAssert.assertThat(requestFilterResult).isShortCircuitResponse()
                    .isErrorResponse().errorResponse()
                    .isInstanceOf(InvalidRecordException.class)
                    .hasMessage("failed to resolve key for: [" + UNRESOLVED_TOPIC + "]");
        });

    }

    @Test
    void shouldPassThroughUnencryptedRecords() {
        // Given
        var fetchResponseData = buildFetchResponseData(new FetchableTopicResponse()
                .setTopic(UNRESOLVED_TOPIC)
                .setPartitions(List.of(new PartitionData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));
        ResponseHeaderData headerData = new ResponseHeaderData();
        MockFilterContext context = MockFilterContext.builder(headerData, fetchResponseData).build();

        // When
        CompletionStage<ResponseFilterResult> response = encryptionFilter.onFetchResponse(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
                headerData, fetchResponseData, context);

        // Then
        assertThat(response).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult -> {
            MockFilterContextAssert.assertThat(responseFilterResult).isForwardResponse()
                    .hasMessageEqualTo(fetchResponseData);
        });
    }

    @Test
    void shouldPassThroughUnencryptedRecordsForRequestUsingTopicId() {
        // Given
        var fetchResponseData = buildFetchResponseData(new FetchableTopicResponse()
                .setTopicId(UNRESOLVED_TOPIC_ID)
                .setPartitions(List.of(new PartitionData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));
        ResponseHeaderData headerData = new ResponseHeaderData();
        MockFilterContext context = MockFilterContext.builder(headerData, fetchResponseData)
                .withTopicName(UNRESOLVED_TOPIC_ID, UNRESOLVED_TOPIC).build();

        // When
        CompletionStage<ResponseFilterResult> response = encryptionFilter.onFetchResponse(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
                headerData, fetchResponseData, context);

        // Then
        assertThat(response).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult -> {
            MockFilterContextAssert.assertThat(responseFilterResult).isForwardResponse()
                    .hasMessageEqualTo(fetchResponseData);
        });
    }

    @Test
    void shouldPassThroughUnencryptedShareRecords() {
        // Given
        var fetchResponseData = buildShareFetchResponseData(new ShareFetchableTopicResponse()
                .setTopicId(UNRESOLVED_TOPIC_ID)
                .setPartitions(List.of(new ShareFetchResponseData.PartitionData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));
        ResponseHeaderData headerData = new ResponseHeaderData();
        MockFilterContext context = MockFilterContext.builder(headerData, fetchResponseData).withTopicName(UNRESOLVED_TOPIC_ID, UNRESOLVED_TOPIC).build();

        // When
        CompletionStage<ResponseFilterResult> response = encryptionFilter.onShareFetchResponse(ShareFetchResponseData.HIGHEST_SUPPORTED_VERSION,
                headerData, fetchResponseData, context);

        // Then
        assertThat(response).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult -> {
            MockFilterContextAssert.assertThat(responseFilterResult).isForwardResponse().hasMessageEqualTo(fetchResponseData);
        });
    }

    @Test
    void shouldPropagateRequestExceptions() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setName(ENCRYPTED_TOPIC)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))));
        RuntimeException exception = new RuntimeException("boom");
        when(kekSelector.selectKek(anySet())).thenReturn(CompletableFuture.failedFuture(exception));
        RequestHeaderData header = new RequestHeaderData();
        MockFilterContext context = MockFilterContext.builder(header, produceRequestData).build();

        // When
        CompletionStage<RequestFilterResult> stage = encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION,
                header, produceRequestData, context);

        // Then
        assertThat(stage).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause().isSameAs(exception);
    }

    @Test
    void shouldRespondWithErrorOnRequestNotSatisfiableExceptions() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setName(ENCRYPTED_TOPIC)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))));
        final RequestNotSatisfiable failure = new RequestNotSatisfiable("could not acquire a DEK", new NetworkException("could not acquire a DEK"));

        when(kekSelector.selectKek(anySet())).thenReturn(CompletableFuture.failedFuture(failure));
        RequestHeaderData header = new RequestHeaderData();
        MockFilterContext context = MockFilterContext.builder(header, produceRequestData).build();

        // When
        CompletionStage<RequestFilterResult> stage = encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION,
                header, produceRequestData, context);

        // Then
        assertThat(stage)
                .succeedsWithin(Duration.ofSeconds(10)).satisfies(requestFilterResult -> {
                    MockFilterContextAssert.assertThat(requestFilterResult)
                            .isNotForwardRequest()
                            .isErrorResponse()
                            .isShortCircuitResponse()
                            .errorResponse()
                            .isInstanceOf(NetworkException.class)
                            .hasMessage("could not acquire a DEK");
                });
    }

    @Test
    void shouldPropagateResponseExceptions() {
        // Given
        var fetchResponseData = buildFetchResponseData(new FetchableTopicResponse()
                .setTopic(UNRESOLVED_TOPIC)
                .setPartitions(List.of(new PartitionData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));
        RuntimeException exception = new RuntimeException("boom");
        when(decryptionManager.decrypt(any(), anyInt(), any(), any())).thenReturn(CompletableFuture.failedFuture(exception));
        ResponseHeaderData headerData = new ResponseHeaderData();
        MockFilterContext context = MockFilterContext.builder(headerData, fetchResponseData).build();

        // When
        CompletionStage<ResponseFilterResult> stage = encryptionFilter.onFetchResponse(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
                headerData, fetchResponseData, context);

        // Then
        assertThat(stage).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause().isSameAs(exception);
    }

    @Test
    void shouldPropagateShareResponseExceptions() {
        // Given
        var fetchResponseData = buildShareFetchResponseData(new ShareFetchableTopicResponse()
                .setTopicId(ENCRYPTED_TOPIC_ID)
                .setPartitions(List.of(new ShareFetchResponseData.PartitionData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))));
        RuntimeException exception = new RuntimeException("boom");
        when(decryptionManager.decrypt(any(), anyInt(), any(), any())).thenReturn(CompletableFuture.failedFuture(exception));
        ResponseHeaderData headerData = new ResponseHeaderData();
        MockFilterContext context = MockFilterContext.builder(headerData, fetchResponseData).withTopicName(ENCRYPTED_TOPIC_ID, ENCRYPTED_TOPIC).build();

        // When
        CompletionStage<ResponseFilterResult> stage = encryptionFilter.onShareFetchResponse(ShareFetchResponseData.HIGHEST_SUPPORTED_VERSION,
                new ResponseHeaderData(), fetchResponseData, context);

        // Then
        assertThat(stage).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause().isSameAs(exception);
    }

    @Test
    void topicNameMappingLookupFailureOnShareFetch() {
        // Given
        var fetchResponseData = buildShareFetchResponseData(new ShareFetchableTopicResponse()
                .setTopicId(ENCRYPTED_TOPIC_ID)
                .setPartitions(List.of(new ShareFetchResponseData.PartitionData().setRecords(makeRecord(HELLO_PLAIN_WORLD)))));
        ResponseHeaderData headerData = new ResponseHeaderData();
        MockFilterContext context = MockFilterContext.builder(headerData, fetchResponseData)
                .withTopicNameError(ENCRYPTED_TOPIC_ID, new TopicNameMappingException(Errors.UNKNOWN_TOPIC_OR_PARTITION)).build();

        // When
        CompletionStage<ResponseFilterResult> stage = encryptionFilter.onShareFetchResponse(ShareFetchResponseData.HIGHEST_SUPPORTED_VERSION,
                new ResponseHeaderData(), fetchResponseData, context);

        // Then
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult -> {
            MockFilterContextAssert.assertThat(responseFilterResult).isForwardResponse()
                    .hasMessageInstanceOfSatisfying(ShareFetchResponseData.class, shareFetchResponseData -> {
                        assertThat(shareFetchResponseData.responses()).hasSize(1).allSatisfy(topicResponse -> {
                            assertThat(topicResponse.partitions()).hasSize(1).allSatisfy(partitionData -> {
                                assertThat(partitionData.errorCode()).isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
                            });
                        });
                    });
        });
    }

    @Test
    void shouldPropagateUnknownKeyToClientAsResourceUnknownError() {
        // Given
        var fetchResponseData = buildFetchResponseData(new FetchableTopicResponse()
                .setTopic(ENCRYPTED_TOPIC)
                .setPartitions(List.of(new PartitionData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))));
        RuntimeException exception = new UnknownKeyException("boom");
        when(decryptionManager.decrypt(any(), anyInt(), any(), any())).thenReturn(CompletableFuture.failedFuture(exception));
        ResponseHeaderData headerData = new ResponseHeaderData();
        MockFilterContext context = MockFilterContext.builder(headerData, fetchResponseData).withTopicName(ENCRYPTED_TOPIC_ID, ENCRYPTED_TOPIC).build();

        // When
        CompletionStage<ResponseFilterResult> stage = encryptionFilter.onFetchResponse(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
                headerData, fetchResponseData, context);

        // Then
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult -> {
            MockFilterContextAssert.assertThat(responseFilterResult).isForwardResponse()
                    .hasMessageInstanceOfSatisfying(FetchResponseData.class, fetchResponseData1 -> {
                        assertThat(fetchResponseData1.responses()).singleElement()
                                .satisfies(ftr -> assertHasSingleEmptyPartitionData(ftr, ENCRYPTED_TOPIC, Errors.RESOURCE_NOT_FOUND));
                    });
        });
    }

    @Test
    void shouldPropagateUnknownKeyToClientAsResourceUnknownErrorShareFetch() {
        // Given
        var fetchResponseData = buildShareFetchResponseData(new ShareFetchableTopicResponse()
                .setTopicId(ENCRYPTED_TOPIC_ID)
                .setPartitions(List.of(new ShareFetchResponseData.PartitionData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))));
        RuntimeException exception = new UnknownKeyException("boom");
        when(decryptionManager.decrypt(any(), anyInt(), any(), any())).thenReturn(CompletableFuture.failedFuture(exception));
        ResponseHeaderData headerData = new ResponseHeaderData();
        MockFilterContext context = MockFilterContext.builder(headerData, fetchResponseData).withTopicName(ENCRYPTED_TOPIC_ID, ENCRYPTED_TOPIC).build();

        // When
        CompletionStage<ResponseFilterResult> stage = encryptionFilter.onShareFetchResponse(ShareFetchResponseData.HIGHEST_SUPPORTED_VERSION,
                headerData, fetchResponseData, context);

        // Then
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult -> {
            MockFilterContextAssert.assertThat(responseFilterResult).isForwardResponse()
                    .hasMessageInstanceOfSatisfying(ShareFetchResponseData.class, shareFetchResponseData -> {
                        assertThat(shareFetchResponseData.errorCode()).isEqualTo(Errors.NONE.code());
                        assertThat(shareFetchResponseData.responses().stream().toList()).singleElement().satisfies(shareFetchableTopicResponse -> {
                            assertHasSingleEmptyPartitionData(shareFetchableTopicResponse, ENCRYPTED_TOPIC_ID, Errors.RESOURCE_NOT_FOUND);
                        });
                    });
        });
    }

    @Test
    void shouldIdentifyTheTopicPartitionThatSufferedUnknownKey() {
        // Given
        var missingKeyResponse = new FetchableTopicResponse()
                .setTopic(ENCRYPTED_TOPIC)
                .setPartitions(List.of(new PartitionData().setRecords(makeRecord(HELLO_CIPHER_WORLD))));
        var anotherResponse = new FetchableTopicResponse()
                .setTopic("anotherTopic")
                .setPartitions(List.of(new PartitionData().setRecords(makeRecord(HELLO_CIPHER_WORLD))));

        var fetchResponseData = buildFetchResponseData(anotherResponse, missingKeyResponse);
        RuntimeException exception = new UnknownKeyException("boom");
        when(decryptionManager.decrypt(eq(ENCRYPTED_TOPIC), anyInt(), any(), any())).thenReturn(CompletableFuture.failedFuture(exception));
        when(decryptionManager.decrypt(eq(anotherResponse.topic()), anyInt(), any(), any())).thenReturn(CompletableFuture.completedStage(makeRecord(HELLO_PLAIN_WORLD)));
        ResponseHeaderData headerData = new ResponseHeaderData();
        MockFilterContext context = MockFilterContext.builder(headerData, fetchResponseData).build();

        // When
        CompletionStage<ResponseFilterResult> stage = encryptionFilter.onFetchResponse(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
                headerData, fetchResponseData, context);

        // Then
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult -> {
            MockFilterContextAssert.assertThat(responseFilterResult).isForwardResponse()
                    .hasMessageInstanceOfSatisfying(FetchResponseData.class, fetchResponseData1 -> {
                        assertThat(fetchResponseData1.responses()).hasSize(2)
                                .satisfies(ftr -> assertHasSingleEmptyPartitionData(ftr, anotherResponse.topic(), Errors.NONE), atIndex(0))
                                .satisfies(ftr -> assertHasSingleEmptyPartitionData(ftr, ENCRYPTED_TOPIC, Errors.RESOURCE_NOT_FOUND), atIndex(1));
                    });
        });
    }

    @Test
    void shouldIdentifyTheTopicPartitionThatSufferedUnknownKeyShareFetch() {
        // Given
        Uuid anotherTopicId = Uuid.randomUuid();
        String anotherTopicName = "anotherTopic";
        var missingKeyResponse = new ShareFetchableTopicResponse()
                .setTopicId(ENCRYPTED_TOPIC_ID)
                .setPartitions(List.of(new ShareFetchResponseData.PartitionData().setRecords(makeRecord(HELLO_CIPHER_WORLD))));
        var anotherResponse = new ShareFetchableTopicResponse()
                .setTopicId(anotherTopicId)
                .setPartitions(List.of(new ShareFetchResponseData.PartitionData().setRecords(makeRecord(HELLO_CIPHER_WORLD))));

        var fetchResponseData = buildShareFetchResponseData(anotherResponse, missingKeyResponse);
        RuntimeException exception = new UnknownKeyException("boom");
        when(decryptionManager.decrypt(eq(ENCRYPTED_TOPIC), anyInt(), any(), any())).thenReturn(CompletableFuture.failedFuture(exception));
        when(decryptionManager.decrypt(eq(anotherTopicName), anyInt(), any(), any())).thenReturn(CompletableFuture.completedStage(makeRecord(HELLO_PLAIN_WORLD)));
        ResponseHeaderData headerData = new ResponseHeaderData();
        MockFilterContext context = MockFilterContext.builder(headerData, fetchResponseData).withTopicName(anotherTopicId, anotherTopicName)
                .withTopicName(ENCRYPTED_TOPIC_ID, ENCRYPTED_TOPIC).build();

        // When
        CompletionStage<ResponseFilterResult> stage = encryptionFilter.onShareFetchResponse(ShareFetchResponseData.HIGHEST_SUPPORTED_VERSION,
                headerData, fetchResponseData, context);

        // Then
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult -> {
            MockFilterContextAssert.assertThat(responseFilterResult).isForwardResponse()
                    .hasMessageInstanceOfSatisfying(ShareFetchResponseData.class, fetchResponse -> {
                        assertThat(fetchResponse.errorCode()).isEqualTo(Errors.NONE.code());
                        assertThat(fetchResponse.responses().stream().toList())
                                .hasSize(2)
                                .satisfies(ftr -> assertHasSingleEmptyPartitionData(ftr, anotherTopicId, Errors.NONE), atIndex(0))
                                .satisfies(ftr -> assertHasSingleEmptyPartitionData(ftr, ENCRYPTED_TOPIC_ID, Errors.RESOURCE_NOT_FOUND), atIndex(1));
                    });
        });
    }

    private static void assertHasSingleEmptyPartitionData(FetchableTopicResponse ftr, String expectedTopicName, Errors expectedError) {
        assertThat(ftr.topic()).isEqualTo(expectedTopicName);
        assertThat(ftr.partitions())
                .singleElement()
                .satisfies(pd -> {
                    assertThat(pd.errorCode()).isEqualTo(expectedError.code());
                    assertThat(pd.records()).isSameAs(MemoryRecords.EMPTY);
                });
    }

    private static void assertHasSingleEmptyPartitionData(ShareFetchableTopicResponse ftr, Uuid expectedTopicId, Errors expectedError) {
        assertThat(ftr.topicId()).isEqualTo(expectedTopicId);
        assertThat(ftr.partitions())
                .singleElement()
                .satisfies(pd -> {
                    assertThat(pd.errorCode()).isEqualTo(expectedError.code());
                    assertThat(pd.records()).isSameAs(MemoryRecords.EMPTY);
                });
    }

    /**
     * Kms exceptions (apart from UnknownKeyException) may be transient.  Causing the connection to close
     * will mean the client tries to fetch again.
     */
    @Test
    void shouldCloseConnectionIfDecryptFailsForOtherReason() {
        // Given
        var fetchResponseData = buildFetchResponseData(new FetchableTopicResponse()
                .setTopic(ENCRYPTED_TOPIC)
                .setPartitions(List.of(new PartitionData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))));
        RuntimeException exception = new KmsException("boom");
        when(decryptionManager.decrypt(any(), anyInt(), any(), any())).thenReturn(CompletableFuture.failedFuture(exception));
        ResponseHeaderData headerData = new ResponseHeaderData();
        MockFilterContext context = MockFilterContext.builder(headerData, fetchResponseData).build();

        // When
        CompletionStage<ResponseFilterResult> stage = encryptionFilter.onFetchResponse(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
                headerData, fetchResponseData, context);

        // Then
        assertThat(stage)
                .failsWithin(Duration.ZERO)
                .withThrowableThat()
                .withCause(exception);
    }

    @Test
    void shouldDecryptEncryptedRecords() {
        // Given
        var encryptedFetchResponse = buildFetchResponseData(new FetchableTopicResponse()
                .setTopic(ENCRYPTED_TOPIC)
                .setPartitions(List.of(new PartitionData().setRecords(makeRecord(ENCRYPTED_MESSAGE_BYTES)))));

        MemoryRecords decryptedRecords = RecordTestUtils.singleElementMemoryRecords("key", "value");
        when(decryptionManager.decrypt(any(), anyInt(), any(), any())).thenReturn(CompletableFuture.completedFuture(decryptedRecords));
        ResponseHeaderData headerData = new ResponseHeaderData();
        MockFilterContext context = MockFilterContext.builder(headerData, encryptedFetchResponse).build();

        // When
        CompletionStage<ResponseFilterResult> response = encryptionFilter.onFetchResponse(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
                headerData, encryptedFetchResponse, context);

        // Then
        assertThat(response).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult -> {
            MockFilterContextAssert.assertThat(responseFilterResult).isForwardResponse().hasMessageInstanceOfSatisfying(FetchResponseData.class, fetchResponse -> {
                assertThat(fetchResponse.responses()).hasSize(1);
                FetchableTopicResponse onlyTopicResponse = fetchResponse.responses().iterator().next();
                assertThat(onlyTopicResponse.topic()).isEqualTo(ENCRYPTED_TOPIC);
                assertThat(onlyTopicResponse.partitions()).hasSize(1);
                PartitionData onlyPartition = onlyTopicResponse.partitions().iterator().next();
                assertThat(onlyPartition.records()).isSameAs(decryptedRecords);
            });
        });
    }

    @Test
    void shouldDecryptEncryptedRecordsWhenFetchUsesTopicId() {
        // Given
        var encryptedFetchResponse = buildFetchResponseData(new FetchableTopicResponse()
                .setTopicId(ENCRYPTED_TOPIC_ID)
                .setPartitions(List.of(new PartitionData().setRecords(makeRecord(ENCRYPTED_MESSAGE_BYTES)))));

        MemoryRecords decryptedRecords = RecordTestUtils.singleElementMemoryRecords("key", "value");
        when(decryptionManager.decrypt(any(), anyInt(), any(), any())).thenReturn(CompletableFuture.completedFuture(decryptedRecords));
        ResponseHeaderData headerData = new ResponseHeaderData();
        MockFilterContext context = MockFilterContext.builder(headerData, encryptedFetchResponse).withTopicName(ENCRYPTED_TOPIC_ID, ENCRYPTED_TOPIC).build();

        // When
        CompletionStage<ResponseFilterResult> response = encryptionFilter.onFetchResponse(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
                headerData, encryptedFetchResponse, context);

        // Then
        assertThat(response).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult -> {
            MockFilterContextAssert.assertThat(responseFilterResult).isForwardResponse().hasMessageInstanceOfSatisfying(FetchResponseData.class, fetchResponse -> {
                assertThat(fetchResponse.responses()).hasSize(1);
                FetchableTopicResponse onlyTopicResponse = fetchResponse.responses().iterator().next();
                assertThat(onlyTopicResponse.topicId()).isEqualTo(ENCRYPTED_TOPIC_ID);
                assertThat(onlyTopicResponse.partitions()).hasSize(1);
                PartitionData onlyPartition = onlyTopicResponse.partitions().iterator().next();
                assertThat(onlyPartition.records()).isSameAs(decryptedRecords);
            });
        });

        verify(decryptionManager).decrypt(eq(ENCRYPTED_TOPIC), anyInt(), any(), any());
    }

    @Test
    void shouldSetAllFetchedPartitionsToErrorIfTopicIdCannotBeMappedToName() {
        // Given
        var encryptedFetchResponse = buildFetchResponseData(new FetchableTopicResponse()
                .setTopicId(ENCRYPTED_TOPIC_ID)
                .setPartitions(List.of(new PartitionData().setRecords(makeRecord(ENCRYPTED_MESSAGE_BYTES)))));

        ResponseHeaderData headerData = new ResponseHeaderData();
        // Given Context doesn't contain name for ENCRYPTED_TOPIC_ID
        MockFilterContext context = MockFilterContext.builder(headerData, encryptedFetchResponse).build();

        // When
        CompletionStage<ResponseFilterResult> response = encryptionFilter.onFetchResponse(FetchResponseData.HIGHEST_SUPPORTED_VERSION,
                headerData, encryptedFetchResponse, context);

        // Then
        assertThat(response).succeedsWithin(Duration.ZERO).satisfies(responseFilterResult -> {
            MockFilterContextAssert.assertThat(responseFilterResult).isForwardResponse().hasMessageInstanceOfSatisfying(FetchResponseData.class, fetchResponse -> {
                assertThat(fetchResponse.responses()).hasSize(1);
                FetchableTopicResponse onlyTopicResponse = fetchResponse.responses().iterator().next();
                assertThat(onlyTopicResponse.topicId()).isEqualTo(ENCRYPTED_TOPIC_ID);
                assertThat(onlyTopicResponse.partitions()).hasSize(1);
                PartitionData onlyPartition = onlyTopicResponse.partitions().iterator().next();
                assertThat(onlyPartition.errorCode()).isEqualTo(Errors.UNKNOWN_TOPIC_ID.code());
                assertThat(onlyPartition.records()).isEqualTo(MemoryRecords.EMPTY);
                assertThat(onlyPartition.highWatermark()).isEqualTo(-1);
            });
        });

        verify(decryptionManager, never()).decrypt(eq(ENCRYPTED_TOPIC), anyInt(), any(), any());
    }

    @Test
    void shouldEncryptTopic() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setName(ENCRYPTED_TOPIC)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))));
        RequestHeaderData headerData = new RequestHeaderData();
        MockFilterContext context = MockFilterContext.builder(headerData, produceRequestData).build();

        // When
        CompletionStage<RequestFilterResult> resultStage = encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION,
                new RequestHeaderData(), produceRequestData, context);

        // Then
        assertThat(resultStage).succeedsWithin(Duration.ZERO).satisfies(requestFilterResult -> {
            MockFilterContextAssert.assertThat(requestFilterResult).isForwardRequest()
                    .hasMessageInstanceOfSatisfying(ProduceRequestData.class, data -> {
                        assertThat(data).has(produceRequestMatching(pr -> pr.topicData().stream().anyMatch(td -> ENCRYPTED_TOPIC.equals(td.name()))));
                    });
        });
    }

    @Test
    void shouldEncryptTopicById() {
        // Given
        var produceRequestData = buildProduceRequestData(new TopicProduceData()
                .setTopicId(ENCRYPTED_TOPIC_ID)
                .setPartitionData(List.of(new PartitionProduceData().setRecords(makeRecord(HELLO_CIPHER_WORLD)))));
        RequestHeaderData headerData = new RequestHeaderData();
        MockFilterContext context = MockFilterContext.builder(headerData, produceRequestData).withTopicName(ENCRYPTED_TOPIC_ID, ENCRYPTED_TOPIC).build();

        // When
        CompletionStage<RequestFilterResult> resultStage = encryptionFilter.onProduceRequest(ProduceRequestData.HIGHEST_SUPPORTED_VERSION,
                new RequestHeaderData(), produceRequestData, context);

        // Then
        assertThat(resultStage).succeedsWithin(Duration.ZERO).satisfies(requestFilterResult -> {
            MockFilterContextAssert.assertThat(requestFilterResult).isForwardRequest()
                    .hasMessageInstanceOfSatisfying(ProduceRequestData.class, data -> {
                        assertThat(data).has(produceRequestMatching(pr -> pr.topicData().stream().anyMatch(td -> ENCRYPTED_TOPIC_ID.equals(td.topicId()))));
                    });
        });
    }

    private static FetchResponseData buildFetchResponseData(FetchableTopicResponse... topicResponses) {
        var data = new FetchResponseData();
        data.responses().addAll(Arrays.asList(topicResponses));
        return data;
    }

    private static ShareFetchResponseData buildShareFetchResponseData(ShareFetchableTopicResponse... topicResponses) {
        var data = new ShareFetchResponseData();
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
