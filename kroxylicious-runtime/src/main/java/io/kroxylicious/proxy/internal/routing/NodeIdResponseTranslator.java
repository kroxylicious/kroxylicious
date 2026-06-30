/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.List;

import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeClusterResponseData.DescribeClusterBrokerCollection;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBrokerCollection;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseData.NodeEndpointCollection;
import org.apache.kafka.common.message.ShareAcknowledgeResponseData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Translates target-cluster node IDs to virtual node IDs in response
 * bodies, in-place. Only response types that contain node IDs are
 * affected; all others are no-ops.
 * <p>
 * No per-field guards for negative values are needed here: the
 * {@link NodeIdMapping} contract guarantees that negative node IDs
 * (Kafka protocol sentinels such as {@code -1} "no leader") are
 * returned unchanged by every mapping implementation.
 */
final class NodeIdResponseTranslator {

    private NodeIdResponseTranslator() {
    }

    static void translate(ApiMessage body,
                          short apiVersion,
                          NodeIdMapping mapping,
                          String route) {
        if (body instanceof MetadataResponseData md) {
            translateMetadata(md, mapping, route);
        }
        else if (body instanceof ProduceResponseData pd) {
            translateProduce(pd, apiVersion, mapping, route);
        }
        else if (body instanceof FindCoordinatorResponseData fc) {
            translateFindCoordinator(fc, apiVersion, mapping, route);
        }
        else if (body instanceof DescribeClusterResponseData dc) {
            translateDescribeCluster(dc, mapping, route);
        }
        else if (body instanceof FetchResponseData fd) {
            translateFetch(fd, apiVersion, mapping, route);
        }
        else if (body instanceof ShareFetchResponseData sfd) {
            translateShareFetch(sfd, mapping, route);
        }
        else if (body instanceof ShareAcknowledgeResponseData sad) {
            translateShareAcknowledge(sad, mapping, route);
        }
    }

    private static void translateMetadata(MetadataResponseData data,
                                          NodeIdMapping mapping,
                                          String route) {
        data.setControllerId(mapping.toVirtual(route, data.controllerId()));

        var translatedBrokers = new MetadataResponseBrokerCollection(data.brokers().size());
        // Must copy and duplicate: mutating nodeId in-place corrupts the ImplicitLinkedHashCollection's hash table.
        for (var broker : List.copyOf(data.brokers())) {
            var translated = broker.duplicate();
            translated.setNodeId(mapping.toVirtual(route, broker.nodeId()));
            translatedBrokers.add(translated);
        }
        data.setBrokers(translatedBrokers);

        for (var topic : data.topics()) {
            for (var partition : topic.partitions()) {
                partition.setLeaderId(mapping.toVirtual(route, partition.leaderId()));
                translateIntList(partition.replicaNodes(), mapping, route);
                translateIntList(partition.isrNodes(), mapping, route);
                translateIntList(partition.offlineReplicas(), mapping, route);
            }
        }
    }

    private static void translateProduce(ProduceResponseData data,
                                         short apiVersion,
                                         NodeIdMapping mapping,
                                         String route) {
        if (apiVersion < 10) {
            return;
        }
        for (var topicResponse : data.responses()) {
            for (var partitionResponse : topicResponse.partitionResponses()) {
                var leader = partitionResponse.currentLeader();
                leader.setLeaderId(mapping.toVirtual(route, leader.leaderId()));
            }
        }
        var translatedEndpoints = new NodeEndpointCollection(data.nodeEndpoints().size());
        // Must copy and duplicate: mutating nodeId in-place corrupts the ImplicitLinkedHashCollection's hash table.
        for (var ne : List.copyOf(data.nodeEndpoints())) {
            var translated = ne.duplicate();
            translated.setNodeId(mapping.toVirtual(route, ne.nodeId()));
            translatedEndpoints.add(translated);
        }
        data.setNodeEndpoints(translatedEndpoints);
    }

    private static void translateFindCoordinator(FindCoordinatorResponseData data,
                                                 short apiVersion,
                                                 NodeIdMapping mapping,
                                                 String route) {
        if (apiVersion >= 4) {
            for (var coordinator : data.coordinators()) {
                coordinator.setNodeId(mapping.toVirtual(route, coordinator.nodeId()));
            }
        }
        else {
            data.setNodeId(mapping.toVirtual(route, data.nodeId()));
        }
    }

    private static void translateDescribeCluster(DescribeClusterResponseData data,
                                                 NodeIdMapping mapping,
                                                 String route) {
        data.setControllerId(mapping.toVirtual(route, data.controllerId()));
        var translatedBrokers = new DescribeClusterBrokerCollection(data.brokers().size());
        // Must copy and duplicate: mutating brokerId in-place corrupts the ImplicitLinkedHashCollection's hash table.
        for (var broker : List.copyOf(data.brokers())) {
            var translated = broker.duplicate();
            translated.setBrokerId(mapping.toVirtual(route, broker.brokerId()));
            translatedBrokers.add(translated);
        }
        data.setBrokers(translatedBrokers);
    }

    private static void translateFetch(FetchResponseData data,
                                       short apiVersion,
                                       NodeIdMapping mapping,
                                       String route) {
        if (apiVersion >= 12) {
            for (var topicResponse : data.responses()) {
                for (var partitionData : topicResponse.partitions()) {
                    var leader = partitionData.currentLeader();
                    leader.setLeaderId(mapping.toVirtual(route, leader.leaderId()));
                }
            }
        }
        if (apiVersion >= 16) {
            var translatedEndpoints = new FetchResponseData.NodeEndpointCollection(data.nodeEndpoints().size());
            // Must copy and duplicate: mutating nodeId in-place corrupts the ImplicitLinkedHashCollection's hash table.
            for (var ne : List.copyOf(data.nodeEndpoints())) {
                var translated = ne.duplicate();
                translated.setNodeId(mapping.toVirtual(route, ne.nodeId()));
                translatedEndpoints.add(translated);
            }
            data.setNodeEndpoints(translatedEndpoints);
        }
    }

    private static void translateShareFetch(ShareFetchResponseData data,
                                            NodeIdMapping mapping,
                                            String route) {
        if (data.nodeEndpoints() == null) {
            return;
        }
        var translatedEndpoints = new ShareFetchResponseData.NodeEndpointCollection(data.nodeEndpoints().size());
        // Must copy and duplicate: mutating nodeId in-place corrupts the ImplicitLinkedHashCollection's hash table.
        for (var ne : List.copyOf(data.nodeEndpoints())) {
            var translated = ne.duplicate();
            translated.setNodeId(mapping.toVirtual(route, ne.nodeId()));
            translatedEndpoints.add(translated);
        }
        data.setNodeEndpoints(translatedEndpoints);
    }

    private static void translateShareAcknowledge(ShareAcknowledgeResponseData data,
                                                  NodeIdMapping mapping,
                                                  String route) {
        if (data.nodeEndpoints() == null) {
            return;
        }
        var translatedEndpoints = new ShareAcknowledgeResponseData.NodeEndpointCollection(data.nodeEndpoints().size());
        // Must copy and duplicate: mutating nodeId in-place corrupts the ImplicitLinkedHashCollection's hash table.
        for (var ne : List.copyOf(data.nodeEndpoints())) {
            var translated = ne.duplicate();
            translated.setNodeId(mapping.toVirtual(route, ne.nodeId()));
            translatedEndpoints.add(translated);
        }
        data.setNodeEndpoints(translatedEndpoints);
    }

    private static void translateIntList(List<Integer> nodeIds,
                                         NodeIdMapping mapping,
                                         String route) {
        for (int i = 0; i < nodeIds.size(); i++) {
            nodeIds.set(i, mapping.toVirtual(route, nodeIds.get(i)));
        }
    }
}
