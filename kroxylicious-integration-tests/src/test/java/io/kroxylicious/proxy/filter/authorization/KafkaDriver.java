/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import io.kroxylicious.test.Response;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.test.requestresponsetestdef.KafkaApiMessageConverter.Converter;

import static io.kroxylicious.proxy.filter.authorization.AuthzIT.getRequest;
import static io.kroxylicious.proxy.filter.authorization.AuthzIT.prettyJsonString;
import static io.kroxylicious.test.requestresponsetestdef.KafkaApiMessageConverter.requestConverterFor;
import static io.kroxylicious.test.requestresponsetestdef.KafkaApiMessageConverter.responseConverterFor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test driver for low-level but complex sequential gestures, for example executing FindCoordinator requests
 * until a co-ordinator is available.
 */
class KafkaDriver {
    private final BaseClusterFixture cluster;
    private final KafkaClient kafkaClient;
    private final String username;
    static final Logger LOG = LoggerFactory.getLogger(KafkaDriver.class);

    /**
     *
     * @param cluster cluster under test
     * @param kafkaClient authenticated client
     */
    KafkaDriver(BaseClusterFixture cluster, KafkaClient kafkaClient, String username) {
        this.cluster = cluster;
        this.kafkaClient = kafkaClient;
        this.username = username;
    }

    private static FindCoordinatorRequestData findCoordinatorRequestData(short findCoordinatorVersion, CoordinatorType coordinatorType, String key) {
        FindCoordinatorRequestData result = new FindCoordinatorRequestData();

        if (findCoordinatorVersion >= 1) {
            result.setKeyType(coordinatorType.id());
        }
        if (findCoordinatorVersion >= 4) {
            result.coordinatorKeys().add(key);
        }
        else {
            result.setKey(key);
        }
        return result;
    }

    private static JoinGroupRequestData joinGroupRequestData(short joinGroupVersion, String protocolType, String groupId, String groupInstanceId) {
        JoinGroupRequestData result = new JoinGroupRequestData();
        result.setGroupId(groupId);
        result.setMemberId("");
        result.setSessionTimeoutMs(10_000);
        if (joinGroupVersion >= 1) {
            result.setRebalanceTimeoutMs(2_000);
        }
        if (joinGroupVersion >= 5) {
            result.setGroupInstanceId(groupInstanceId);
        }
        if (joinGroupVersion >= 8) {
            result.setReason("Hello, world");
        }
        result.setProtocolType(protocolType);
        result.protocols().add(new JoinGroupRequestData.JoinGroupRequestProtocol().setName("proto").setMetadata(new byte[]{ 1 }));
        return result;
    }

    private static SyncGroupRequestData syncGroupRequestData(short syncGroupVersion, String groupId, String groupInstanceId,
                                                             String protocolType, int generation, String memberId) {
        SyncGroupRequestData result = new SyncGroupRequestData();
        result.setGroupId(groupId);
        if (syncGroupVersion >= 3) {
            result.setGroupInstanceId(groupInstanceId);
        }
        result.setMemberId(memberId);
        if (syncGroupVersion >= 5) {
            result.setProtocolType(protocolType);
            result.setProtocolName("evwrv");
        }
        result.setGenerationId(generation);
        result.assignments().add(new SyncGroupRequestData.SyncGroupRequestAssignment()
                .setMemberId(memberId)
                .setAssignment(new byte[]{ 42 }));
        return result;
    }

    @SuppressWarnings({"BusyWait", "java:S2925"})
    FindCoordinatorResponseData findCoordinator(CoordinatorType coordinatorType, String key) {
        do {
            short findCoordinatorVersion = (short) 1;
            FindCoordinatorRequestData request = findCoordinatorRequestData(findCoordinatorVersion, coordinatorType, key);
            FindCoordinatorResponseData response = sendRequest(request, findCoordinatorVersion,
                    FindCoordinatorResponseData.class);
            Errors actual = Errors.forCode(response.errorCode());
            if (actual == Errors.COORDINATOR_NOT_AVAILABLE) {
                try {
                    Thread.sleep(10);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                continue;
            }
            assertThat(actual)
                    .as("FindCoordinator response from %s (errorMessage=%s)", cluster, response.errorMessage())
                    .isEqualTo(Errors.NONE);
            return response;
        } while (true);
    }

    JoinGroupResponseData joinGroup(String protocolType, String groupId, String groupInstanceId) {
        short joinGroupVersion = (short) 5;
        AtomicReference<JoinGroupResponseData> finalResponse = new AtomicReference<>();
        Awaitility.await().pollInterval(20, MILLISECONDS).untilAsserted(() -> {
            JoinGroupRequestData request = joinGroupRequestData(joinGroupVersion, protocolType, groupId, groupInstanceId);
            JoinGroupResponseData response = sendRequest(request, joinGroupVersion, JoinGroupResponseData.class);
            assertThat(Errors.forCode(response.errorCode()))
                    .as("JoinGroup response from %s", cluster)
                    .isEqualTo(Errors.NONE);
            finalResponse.set(response);
        });
        return finalResponse.get();
    }

    public <S extends ApiMessage, T extends ApiMessage> T sendRequest(S request,
                                                                      short apiVersion,
                                                                      Class<T> responseClass) {
        ApiKeys apiKeys = ApiKeys.forId(request.apiKey());
        Converter requestConverter = requestConverterFor(apiKeys.messageType);
        LOG.info("{} {} request: {} >> {}",
                username,
                apiKeys,
                prettyJsonString(requestConverter.writer().apply(request, apiVersion)),
                cluster.name());
        Response res = kafkaClient.getSync(getRequest(apiVersion, request));
        ApiMessage responseMessage = res.payload().message();
        assertThat(responseMessage).isInstanceOf(responseClass);
        var response = responseClass.cast(responseMessage);
        Converter responseConverter = responseConverterFor(apiKeys.messageType);
        LOG.info("{} {} response: {} << {}",
                username,
                apiKeys,
                prettyJsonString(responseConverter.writer().apply(response, apiVersion)),
                cluster.name());
        return response;
    }

    SyncGroupResponseData syncGroup(String groupId, String groupInstanceId,
                                    String protocolType, int generation, String memberId) {
        short syncGroupVersion = (short) 3;
        SyncGroupRequestData request = syncGroupRequestData(syncGroupVersion, groupId, groupInstanceId, protocolType, generation, memberId);
        SyncGroupResponseData response = sendRequest(request, syncGroupVersion, SyncGroupResponseData.class);
        assertThat(Errors.forCode(response.errorCode()))
                .as("SyncGroup response from %s", cluster)
                .isEqualTo(Errors.NONE);
        return response;
    }

    ProducerIdAndEpoch initProducerId(String transactionalId) {
        AtomicReference<ProducerIdAndEpoch> producerIdAndEpoch = new AtomicReference<>(ProducerIdAndEpoch.NONE);
        Awaitility.await().pollInterval(10, MILLISECONDS).untilAsserted(() -> {
            InitProducerIdRequestData request = new InitProducerIdRequestData();
            request.setTransactionalId(transactionalId);
            ProducerIdAndEpoch pep = producerIdAndEpoch.get();
            request.setProducerId(pep.producerId);
            request.setProducerEpoch(pep.epoch);
            request.setTransactionTimeoutMs(10000);
            InitProducerIdResponseData response = sendRequest(request, (short) 5, InitProducerIdResponseData.class);
            assertThat(Errors.forCode(response.errorCode())).isEqualTo(Errors.NONE);
            producerIdAndEpoch.set(new ProducerIdAndEpoch(response.producerId(), response.producerEpoch()));
        });
        return producerIdAndEpoch.get();
    }

    AddPartitionsToTxnResponseData addPartitionsToTransaction(String transactionalId, ProducerIdAndEpoch producerIdAndEpoch,
                                                              Map<String, Collection<Integer>> topicPartitions) {
        AddPartitionsToTxnRequestData request = new AddPartitionsToTxnRequestData();
        request.setV3AndBelowProducerEpoch(producerIdAndEpoch.epoch);
        request.setV3AndBelowTransactionalId(transactionalId);
        request.setV3AndBelowProducerId(producerIdAndEpoch.producerId);
        topicPartitions.forEach((topicName, partitionIds) -> {
            AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic topic = new AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic();
            topic.setName(topicName);
            for (Integer partitionId : partitionIds) {
                topic.partitions().add(partitionId);
            }
            request.v3AndBelowTopics().add(topic);
        });
        AddPartitionsToTxnResponseData response = sendRequest(request, (short) 3, AddPartitionsToTxnResponseData.class);
        assertThat(Errors.forCode(response.errorCode())).isEqualTo(Errors.NONE);
        response.resultsByTransaction().forEach(transactionResult -> {
            for (AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult topicResult : transactionResult.topicResults()) {
                topicResult.resultsByPartition().forEach(partition -> {
                    assertThat(Errors.forCode(partition.partitionErrorCode())).isEqualTo(Errors.NONE);
                });
            }
        });
        return response;
    }

    AddOffsetsToTxnResponseData addOffsetsToTxn(String transactionalId, ProducerIdAndEpoch producerIdAndEpoch, String groupId) {
        AddOffsetsToTxnRequestData request = new AddOffsetsToTxnRequestData();
        request.setTransactionalId(transactionalId);
        request.setProducerId(producerIdAndEpoch.producerId);
        request.setProducerEpoch(producerIdAndEpoch.epoch);
        request.setGroupId(groupId);
        AddOffsetsToTxnResponseData response = sendRequest(request, (short) 4, AddOffsetsToTxnResponseData.class);
        assertThat(Errors.forCode(response.errorCode())).isEqualTo(Errors.NONE);
        return response;
    }
}
