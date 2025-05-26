/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.ingress;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRangesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ProxyResolutionResult;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.operator.ProxyDeploymentDependentResource.PROXY_PORT_START;
import static io.kroxylicious.kubernetes.operator.ProxyDeploymentDependentResource.PROXY_SHARED_TLS_PORT;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;

/**
 * IngressAllocator is responsible for:
 * <ul>
 *     <li>building a ProxyIngressModel with logical Ingresses created per VirtualCluster</li>
 *     <li>allocating identifying container ports to Ingresses</li>
 *     <li>deciding which Ingresses are in conflict with each other and selecting which to accept</li>
 * </ul>
 */
public class IngressAllocator {
    private IngressAllocator() {
    }

    private static final List<NodeIdRanges> DEFAULT_NODE_ID_RANGES = List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build());

    /**
     * Allocates a ProxyIngressModel. The aim is to deterministically produce a model of the ports that will
     * be used by the proxy container. We want this to be as stable as possible, so we will allocate ports
     * to potentially unacceptable virtual clusters if we can.
     *
     * @param primary primary being reconciled
     * @param resolutionResult
     * @return non-null ProxyIngressModel
     */
    public static ProxyIngressModel allocateProxyIngressModel(KafkaProxy primary,
                                                              ProxyResolutionResult resolutionResult) {
        AtomicInteger exclusivePorts = new AtomicInteger(PROXY_PORT_START);
        // include broken clusters in the model, so that if they are healed the ports will stay the same
        Stream<ClusterResolutionResult> virtualKafkaClusterStream = resolutionResult.allResolutionResultsInClusterNameOrder();
        List<ProxyIngressModel.VirtualClusterIngressModel> list = virtualKafkaClusterStream
                .map(it -> new ProxyIngressModel.VirtualClusterIngressModel(it.cluster(), allocateIngressModel(primary, it, exclusivePorts)))
                .toList();
        return new ProxyIngressModel(list);
    }

    private static List<ProxyIngressModel.IngressModelResult> allocateIngressModel(KafkaProxy primary, ClusterResolutionResult it, AtomicInteger ports) {
        Stream<IngressDefinition> ingressDefinitions = buildIngressDefinitions(primary, it);
        return ingressDefinitions.map(ingressDefinition -> {
            int toAllocate = ingressDefinition.numIdentifyingPortsRequired();
            Integer firstIdentifyingPort = null;
            Integer lastIdentifyingPort = null;
            Integer sharedTlsPort = null;
            IngressConflictException exception = null;
            if (toAllocate != 0) {
                if (ports.get() != PROXY_PORT_START) {
                    exception = new IngressConflictException(name(ingressDefinition.ingress()),
                            "Currently we do not support a virtual cluster with multiple ingresses that need unique ports to identify which node the "
                                    + "client is connecting to. We currently do not have a sufficient strategy for port allocation for this case. See https://github.com/kroxylicious/kroxylicious/issues/1902");
                }
                firstIdentifyingPort = ports.get();
                lastIdentifyingPort = ports.addAndGet(toAllocate) - 1;
            }
            else if (ingressDefinition.requiresSharedTLSPort()) {
                sharedTlsPort = PROXY_SHARED_TLS_PORT;
            }
            else {
                throw new IllegalStateException("ingress didn't required identifying ports or a shared TLS port");
            }

            IngressModel ingressModel = ingressDefinition.createIngressModel(firstIdentifyingPort, lastIdentifyingPort, sharedTlsPort);
            return new ProxyIngressModel.IngressModelResult(ingressModel, exception);
        }).toList();
    }

    static Stream<IngressDefinition> buildIngressDefinitions(KafkaProxy primary, ClusterResolutionResult clusterResult) {
        VirtualKafkaCluster cluster = clusterResult.cluster();
        return clusterResult.ingressResolutionResults().stream()
                .flatMap(
                        ingressResolutionResult -> {
                            Optional<KafkaProxyIngress> maybeIngress = ingressResolutionResult.ingressResolutionResult().maybeReferentResource();
                            if (maybeIngress.isPresent()) {
                                Optional<KafkaService> maybeService = clusterResult.serviceResolutionResult().maybeReferentResource();
                                // todo, maybe we should not include the case where the service does not resolve, it's optimistic to assume its using the default node id range
                                List<NodeIdRanges> nodeIdRanges = maybeService.map(s -> s.getSpec().getNodeIdRanges()).orElse(DEFAULT_NODE_ID_RANGES);
                                return Stream.of(buildIngressDefinition(primary, cluster, maybeIngress.get(), nodeIdRanges, ingressResolutionResult.ingress().getTls()));
                            }
                            else {
                                // skip unresolved ingresses
                                return Stream.empty();
                            }
                        });
    }

    private static IngressDefinition buildIngressDefinition(KafkaProxy primary,
                                                            VirtualKafkaCluster cluster,
                                                            KafkaProxyIngress ingress,
                                                            List<NodeIdRanges> nodeIdRanges,
                                                            @Nullable Tls tls) {
        ClusterIP clusterIP = ingress.getSpec().getClusterIP();
        if (clusterIP != null) {
            return new ClusterIPIngressDefinition(ingress, cluster, primary, nodeIdRanges, tls);
        }
        else {
            throw new IllegalStateException("ingress must have clusterIP specified");
        }
    }
}
