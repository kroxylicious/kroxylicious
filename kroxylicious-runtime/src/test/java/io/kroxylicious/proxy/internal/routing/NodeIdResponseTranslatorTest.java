/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBroker;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData.Coordinator;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseData.LeaderIdAndEpoch;
import org.apache.kafka.common.message.ProduceResponseData.NodeEndpoint;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class NodeIdResponseTranslatorTest {

    private static final String ROUTE_A = "route-a";
    private static final String ROUTE_B = "route-b";

    private final NodeIdMapping mapping = new BijectiveNodeIdMapping(Map.of(ROUTE_A, 0, ROUTE_B, 1), 2);

    @Test
    void shouldTranslateMetadataBrokers() {
        var data = new MetadataResponseData();
        data.brokers().add(new MetadataResponseBroker().setNodeId(0).setHost("h0").setPort(9092));
        data.brokers().add(new MetadataResponseBroker().setNodeId(1).setHost("h1").setPort(9093));
        data.setControllerId(0);

        NodeIdResponseTranslator.translate(data, (short) 12, mapping, ROUTE_A);

        assertThat(data.brokers().find(mapping.toVirtual(ROUTE_A, 0))).isNotNull();
        assertThat(data.brokers().find(mapping.toVirtual(ROUTE_A, 1))).isNotNull();
        assertThat(data.controllerId()).isEqualTo(mapping.toVirtual(ROUTE_A, 0));
    }

    @Test
    void shouldTranslateMetadataPartitionNodeIds() {
        var data = new MetadataResponseData();
        data.setControllerId(0);

        var partition = new MetadataResponsePartition()
                .setPartitionIndex(0)
                .setLeaderId(1)
                .setReplicaNodes(new ArrayList<>(List.of(0, 1, 2)))
                .setIsrNodes(new ArrayList<>(List.of(0, 1)))
                .setOfflineReplicas(new ArrayList<>(List.of(2)));
        var topic = new MetadataResponseTopic().setName("test");
        topic.partitions().add(partition);
        data.topics().add(topic);

        NodeIdResponseTranslator.translate(data, (short) 12, mapping, ROUTE_B);

        assertThat(partition.leaderId()).isEqualTo(mapping.toVirtual(ROUTE_B, 1));
        assertThat(partition.replicaNodes()).containsExactly(
                mapping.toVirtual(ROUTE_B, 0),
                mapping.toVirtual(ROUTE_B, 1),
                mapping.toVirtual(ROUTE_B, 2));
        assertThat(partition.isrNodes()).containsExactly(
                mapping.toVirtual(ROUTE_B, 0),
                mapping.toVirtual(ROUTE_B, 1));
        assertThat(partition.offlineReplicas()).containsExactly(
                mapping.toVirtual(ROUTE_B, 2));
    }

    @Test
    void shouldTranslateProduceResponseV10NodeEndpoints() {
        var data = new ProduceResponseData();
        data.nodeEndpoints().add(new NodeEndpoint().setNodeId(0).setHost("h0").setPort(9092));
        data.nodeEndpoints().add(new NodeEndpoint().setNodeId(1).setHost("h1").setPort(9093));

        var partitionResp = new PartitionProduceResponse()
                .setIndex(0)
                .setCurrentLeader(new LeaderIdAndEpoch().setLeaderId(1).setLeaderEpoch(5));
        var topicResp = new TopicProduceResponse().setName("test");
        topicResp.partitionResponses().add(partitionResp);
        data.responses().add(topicResp);

        NodeIdResponseTranslator.translate(data, (short) 10, mapping, ROUTE_A);

        assertThat(data.nodeEndpoints().find(mapping.toVirtual(ROUTE_A, 0))).isNotNull();
        assertThat(data.nodeEndpoints().find(mapping.toVirtual(ROUTE_A, 1))).isNotNull();
        assertThat(partitionResp.currentLeader().leaderId()).isEqualTo(mapping.toVirtual(ROUTE_A, 1));
    }

    @Test
    void shouldNotTranslateMetadataControllerIdWhenMinusOne() {
        var data = new MetadataResponseData().setControllerId(-1);

        NodeIdResponseTranslator.translate(data, (short) 12, mapping, ROUTE_A);

        assertThat(data.controllerId()).isEqualTo(-1);
    }

    @Test
    void shouldNotTranslateMetadataPartitionLeaderIdWhenMinusOne() {
        var data = new MetadataResponseData().setControllerId(0);
        var partition = new MetadataResponsePartition()
                .setPartitionIndex(0)
                .setLeaderId(-1)
                .setReplicaNodes(new ArrayList<>(List.of(0)))
                .setIsrNodes(new ArrayList<>())
                .setOfflineReplicas(new ArrayList<>());
        var topic = new MetadataResponseTopic().setName("test");
        topic.partitions().add(partition);
        data.topics().add(topic);

        NodeIdResponseTranslator.translate(data, (short) 12, mapping, ROUTE_A);

        assertThat(partition.leaderId()).isEqualTo(-1);
    }

    @Test
    void shouldNotTranslateProduceResponseBelowV10() {
        var data = new ProduceResponseData();
        var partitionResp = new PartitionProduceResponse().setIndex(0);
        var topicResp = new TopicProduceResponse().setName("test");
        topicResp.partitionResponses().add(partitionResp);
        data.responses().add(topicResp);

        NodeIdResponseTranslator.translate(data, (short) 9, mapping, ROUTE_A);

        assertThat(data.nodeEndpoints()).isEmpty();
    }

    @Test
    void shouldNotTranslateProduceLeaderIdWhenMinusOne() {
        var data = new ProduceResponseData();
        var partitionResp = new PartitionProduceResponse()
                .setIndex(0)
                .setCurrentLeader(new LeaderIdAndEpoch().setLeaderId(-1).setLeaderEpoch(-1));
        var topicResp = new TopicProduceResponse().setName("test");
        topicResp.partitionResponses().add(partitionResp);
        data.responses().add(topicResp);

        NodeIdResponseTranslator.translate(data, (short) 10, mapping, ROUTE_A);

        assertThat(partitionResp.currentLeader().leaderId()).isEqualTo(-1);
    }

    @Test
    void shouldTranslateFindCoordinatorV3() {
        var data = new FindCoordinatorResponseData()
                .setNodeId(2)
                .setHost("h2")
                .setPort(9094);

        NodeIdResponseTranslator.translate(data, (short) 3, mapping, ROUTE_B);

        assertThat(data.nodeId()).isEqualTo(mapping.toVirtual(ROUTE_B, 2));
    }

    @Test
    void shouldTranslateFindCoordinatorV4Coordinators() {
        var data = new FindCoordinatorResponseData();
        data.coordinators().add(new Coordinator().setNodeId(0).setHost("h0").setPort(9092));
        data.coordinators().add(new Coordinator().setNodeId(1).setHost("h1").setPort(9093));

        NodeIdResponseTranslator.translate(data, (short) 4, mapping, ROUTE_A);

        assertThat(data.coordinators().get(0).nodeId()).isEqualTo(mapping.toVirtual(ROUTE_A, 0));
        assertThat(data.coordinators().get(1).nodeId()).isEqualTo(mapping.toVirtual(ROUTE_A, 1));
    }

    @Test
    void shouldNotTranslateFindCoordinatorWhenNodeIdNegative() {
        var data = new FindCoordinatorResponseData().setNodeId(-1);

        NodeIdResponseTranslator.translate(data, (short) 3, mapping, ROUTE_A);

        assertThat(data.nodeId()).isEqualTo(-1);
    }

    @Test
    void shouldTranslateDescribeClusterBrokers() {
        var data = new DescribeClusterResponseData();
        data.setControllerId(1);
        data.brokers().add(new DescribeClusterBroker().setBrokerId(0).setHost("h0").setPort(9092));
        data.brokers().add(new DescribeClusterBroker().setBrokerId(1).setHost("h1").setPort(9093));

        NodeIdResponseTranslator.translate(data, (short) 0, mapping, ROUTE_B);

        assertThat(data.controllerId()).isEqualTo(mapping.toVirtual(ROUTE_B, 1));
        assertThat(data.brokers().find(mapping.toVirtual(ROUTE_B, 0))).isNotNull();
        assertThat(data.brokers().find(mapping.toVirtual(ROUTE_B, 1))).isNotNull();
    }

    @Test
    void shouldNotTranslateDescribeClusterControllerWhenNegative() {
        var data = new DescribeClusterResponseData().setControllerId(-1);

        NodeIdResponseTranslator.translate(data, (short) 0, mapping, ROUTE_A);

        assertThat(data.controllerId()).isEqualTo(-1);
    }

    @Test
    void shouldBeNoOpForUnrelatedResponseTypes() {
        var data = new CreateTopicsResponseData();
        data.setThrottleTimeMs(42);

        NodeIdResponseTranslator.translate(data, (short) 7, mapping, ROUTE_A);

        assertThat(data.throttleTimeMs()).isEqualTo(42);
    }

    @ParameterizedTest
    @ValueSource(shorts = { 12, 13, 14, 15 })
    void shouldTranslateFetchResponseCurrentLeaderV12toV15(short apiVersion) {
        var data = new FetchResponseData();
        var partitionData = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setCurrentLeader(new FetchResponseData.LeaderIdAndEpoch().setLeaderId(1).setLeaderEpoch(5));
        var topicResponse = new FetchResponseData.FetchableTopicResponse().setTopic("test");
        topicResponse.partitions().add(partitionData);
        data.responses().add(topicResponse);

        NodeIdResponseTranslator.translate(data, apiVersion, mapping, ROUTE_A);

        assertThat(partitionData.currentLeader().leaderId()).isEqualTo(mapping.toVirtual(ROUTE_A, 1));
    }

    @Test
    void shouldTranslateFetchResponseV16NodeEndpointsAndCurrentLeader() {
        var data = new FetchResponseData();
        data.nodeEndpoints().add(new FetchResponseData.NodeEndpoint().setNodeId(0).setHost("h0").setPort(9092));
        data.nodeEndpoints().add(new FetchResponseData.NodeEndpoint().setNodeId(1).setHost("h1").setPort(9093));
        var partitionData = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setCurrentLeader(new FetchResponseData.LeaderIdAndEpoch().setLeaderId(1).setLeaderEpoch(5));
        var topicResponse = new FetchResponseData.FetchableTopicResponse().setTopic("test");
        topicResponse.partitions().add(partitionData);
        data.responses().add(topicResponse);

        NodeIdResponseTranslator.translate(data, (short) 16, mapping, ROUTE_A);

        assertThat(data.nodeEndpoints().find(mapping.toVirtual(ROUTE_A, 0))).isNotNull();
        assertThat(data.nodeEndpoints().find(mapping.toVirtual(ROUTE_A, 1))).isNotNull();
        assertThat(partitionData.currentLeader().leaderId()).isEqualTo(mapping.toVirtual(ROUTE_A, 1));
    }

    @Test
    void shouldNotTranslateFetchResponseCurrentLeaderBeforeV12() {
        var data = new FetchResponseData();
        var partitionData = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setCurrentLeader(new FetchResponseData.LeaderIdAndEpoch().setLeaderId(1).setLeaderEpoch(5));
        var topicResponse = new FetchResponseData.FetchableTopicResponse().setTopic("test");
        topicResponse.partitions().add(partitionData);
        data.responses().add(topicResponse);

        NodeIdResponseTranslator.translate(data, (short) 11, mapping, ROUTE_A);

        assertThat(partitionData.currentLeader().leaderId()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(shorts = { 11, 12, 13, 14, 15 })
    void shouldTranslateFetchResponsePreferredReadReplicaV11Plus(short apiVersion) {
        var data = new FetchResponseData();
        var partitionData = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setPreferredReadReplica(1);
        var topicResponse = new FetchResponseData.FetchableTopicResponse().setTopic("test");
        topicResponse.partitions().add(partitionData);
        data.responses().add(topicResponse);

        NodeIdResponseTranslator.translate(data, apiVersion, mapping, ROUTE_A);

        assertThat(partitionData.preferredReadReplica()).isEqualTo(mapping.toVirtual(ROUTE_A, 1));
    }

    @Test
    void shouldNotTranslateFetchResponsePreferredReadReplicaBeforeV11() {
        var data = new FetchResponseData();
        var partitionData = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setPreferredReadReplica(1);
        var topicResponse = new FetchResponseData.FetchableTopicResponse().setTopic("test");
        topicResponse.partitions().add(partitionData);
        data.responses().add(topicResponse);

        NodeIdResponseTranslator.translate(data, (short) 10, mapping, ROUTE_A);

        assertThat(partitionData.preferredReadReplica()).isEqualTo(1);
    }

    @Test
    void shouldNotTranslateFetchResponsePreferredReadReplicaWhenMinusOne() {
        var data = new FetchResponseData();
        var partitionData = new FetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setPreferredReadReplica(-1);
        var topicResponse = new FetchResponseData.FetchableTopicResponse().setTopic("test");
        topicResponse.partitions().add(partitionData);
        data.responses().add(topicResponse);

        NodeIdResponseTranslator.translate(data, (short) 11, mapping, ROUTE_A);

        assertThat(partitionData.preferredReadReplica()).isEqualTo(-1);
    }

    @Test
    void shouldNotTranslateFetchResponseNodeEndpointsBeforeV16() {
        var data = new FetchResponseData();

        NodeIdResponseTranslator.translate(data, (short) 15, mapping, ROUTE_A);

        assertThat(data.nodeEndpoints()).isEmpty();
    }

    @Test
    void shouldTranslateShareFetchCurrentLeaderId() {
        var data = new ShareFetchResponseData();
        var partitionData = new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setCurrentLeader(new ShareFetchResponseData.LeaderIdAndEpoch().setLeaderId(1).setLeaderEpoch(5));
        var topicResponse = new ShareFetchResponseData.ShareFetchableTopicResponse();
        topicResponse.partitions().add(partitionData);
        data.responses().add(topicResponse);

        NodeIdResponseTranslator.translate(data, (short) 0, mapping, ROUTE_A);

        assertThat(partitionData.currentLeader().leaderId()).isEqualTo(mapping.toVirtual(ROUTE_A, 1));
    }

    @Test
    void shouldNotTranslateShareFetchCurrentLeaderIdWhenMinusOne() {
        var data = new ShareFetchResponseData();
        var partitionData = new ShareFetchResponseData.PartitionData()
                .setPartitionIndex(0)
                .setCurrentLeader(new ShareFetchResponseData.LeaderIdAndEpoch().setLeaderId(-1).setLeaderEpoch(-1));
        var topicResponse = new ShareFetchResponseData.ShareFetchableTopicResponse();
        topicResponse.partitions().add(partitionData);
        data.responses().add(topicResponse);

        NodeIdResponseTranslator.translate(data, (short) 0, mapping, ROUTE_A);

        assertThat(partitionData.currentLeader().leaderId()).isEqualTo(-1);
    }

    @Test
    void shouldTranslateShareFetchNodeEndpoints() {
        var data = new ShareFetchResponseData();
        data.nodeEndpoints().add(new ShareFetchResponseData.NodeEndpoint().setNodeId(0).setHost("h0").setPort(9092));
        data.nodeEndpoints().add(new ShareFetchResponseData.NodeEndpoint().setNodeId(1).setHost("h1").setPort(9093));

        NodeIdResponseTranslator.translate(data, (short) 0, mapping, ROUTE_B);

        assertThat(data.nodeEndpoints().find(mapping.toVirtual(ROUTE_B, 0))).isNotNull();
        assertThat(data.nodeEndpoints().find(mapping.toVirtual(ROUTE_B, 1))).isNotNull();
    }

    @Test
    void shouldTranslateShareAcknowledgeCurrentLeaderId() {
        var data = new ShareAcknowledgeResponseData();
        var partitionData = new ShareAcknowledgeResponseData.PartitionData()
                .setPartitionIndex(0)
                .setCurrentLeader(new ShareAcknowledgeResponseData.LeaderIdAndEpoch().setLeaderId(1).setLeaderEpoch(5));
        var topicResponse = new ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse();
        topicResponse.partitions().add(partitionData);
        data.responses().add(topicResponse);

        NodeIdResponseTranslator.translate(data, (short) 0, mapping, ROUTE_A);

        assertThat(partitionData.currentLeader().leaderId()).isEqualTo(mapping.toVirtual(ROUTE_A, 1));
    }

    @Test
    void shouldNotTranslateShareAcknowledgeCurrentLeaderIdWhenMinusOne() {
        var data = new ShareAcknowledgeResponseData();
        var partitionData = new ShareAcknowledgeResponseData.PartitionData()
                .setPartitionIndex(0)
                .setCurrentLeader(new ShareAcknowledgeResponseData.LeaderIdAndEpoch().setLeaderId(-1).setLeaderEpoch(-1));
        var topicResponse = new ShareAcknowledgeResponseData.ShareAcknowledgeTopicResponse();
        topicResponse.partitions().add(partitionData);
        data.responses().add(topicResponse);

        NodeIdResponseTranslator.translate(data, (short) 0, mapping, ROUTE_A);

        assertThat(partitionData.currentLeader().leaderId()).isEqualTo(-1);
    }

    @Test
    void shouldTranslateShareAcknowledgeNodeEndpoints() {
        var data = new ShareAcknowledgeResponseData();
        data.nodeEndpoints().add(new ShareAcknowledgeResponseData.NodeEndpoint().setNodeId(0).setHost("h0").setPort(9092));
        data.nodeEndpoints().add(new ShareAcknowledgeResponseData.NodeEndpoint().setNodeId(1).setHost("h1").setPort(9093));

        NodeIdResponseTranslator.translate(data, (short) 0, mapping, ROUTE_B);

        assertThat(data.nodeEndpoints().find(mapping.toVirtual(ROUTE_B, 0))).isNotNull();
        assertThat(data.nodeEndpoints().find(mapping.toVirtual(ROUTE_B, 1))).isNotNull();
    }
}
