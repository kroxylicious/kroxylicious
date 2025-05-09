/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.ingress;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRangesBuilder;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;

/**
 * IngressDefinitionBuilder is responsible for creating richer IngressDefinition objects from the raw
 * Custom Resources that define it.
 */
class IngressDefinitionBuilder {

    public static final List<NodeIdRanges> DEFAULT_NODE_ID_RANGES = List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build());

    private IngressDefinitionBuilder() {
    }

    public static Stream<IngressDefinition> buildIngressDefinitions(KafkaProxy primary, ClusterResolutionResult clusterResult) {
        VirtualKafkaCluster cluster = clusterResult.cluster();
        return clusterResult.ingressResolutionResults().stream()
                .flatMap(
                        ingressResolutionResult -> {
                            Optional<KafkaProxyIngress> maybeIngress = ingressResolutionResult.ingressResolutionResult().maybeReferentResource();
                            if (maybeIngress.isPresent()) {
                                Optional<KafkaService> maybeService = clusterResult.serviceResolutionResult().maybeReferentResource();
                                // todo, maybe we should not include the case where the service does not resolve, it's optimistic to assume its using the default node id range
                                List<NodeIdRanges> nodeIdRanges = maybeService.map(s -> s.getSpec().getNodeIdRanges()).orElse(DEFAULT_NODE_ID_RANGES);
                                return Stream.of(buildIngressDefinition(primary, cluster, maybeIngress.get(), nodeIdRanges));
                            }
                            else {
                                // skip unresolved ingresses
                                return Stream.empty();
                            }
                        });
    }

    private static IngressDefinition buildIngressDefinition(KafkaProxy primary, VirtualKafkaCluster cluster, KafkaProxyIngress ingress, List<NodeIdRanges> nodeIdRanges) {
        ClusterIP clusterIP = ingress.getSpec().getClusterIP();
        if (clusterIP != null) {
            return new ClusterIPIngressDefinition(ingress, cluster, primary, nodeIdRanges);
        }
        else {
            throw new IllegalStateException("ingress must have clusterIP specified");
        }
    }
}
