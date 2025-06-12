/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.LoadBalancer;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.OpenShiftRoutes;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRangesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.operator.model.networking.ProxyNetworkingModel.ClusterIngressNetworkingModelResult;
import io.kroxylicious.kubernetes.operator.model.networking.ProxyNetworkingModel.ClusterNetworkingModel;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.IngressResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ProxyResolutionResult;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.operator.ProxyDeploymentDependentResource.PROXY_PORT_START;
import static io.kroxylicious.kubernetes.operator.ProxyDeploymentDependentResource.SHARED_SNI_PORT;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;

/**
 * NetworkingPlanner is responsible for:
 * <ul>
 *     <li>planning a ProxyNetworkingModel, which is a logical model of all resources/configuration needed to connect clients to all virtual kafka clusters of the proxy</li>
 *     <li>allocating identifying container ports to the model</li>
 *     <li>detecting ClusterIngressNetworkingModels that are in conflict with each other, and selecting which to accept</li>
 * </ul>
 * We aim to produce a stable model, where port changes are minimised, therefore we may consider broken clusters even though
 * their model will not be manifested as resources.
 */
public class NetworkingPlanner {
    private NetworkingPlanner() {
    }

    private static final List<NodeIdRanges> DEFAULT_NODE_ID_RANGES = List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build());

    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkingPlanner.class);

    /**
     * Allocates a ProxyNetworkingModel. The aim is to deterministically produce a model of the ports that will
     * be used by the proxy container. We want this to be as stable as possible, so we will allocate ports
     * to potentially unacceptable virtual clusters if we can.
     *
     * @param primary primary being reconciled
     * @param proxyResolutionResult
     * @return non-null ProxyIngressModel
     */
    public static ProxyNetworkingModel planNetworking(KafkaProxy primary,
                                                      ProxyResolutionResult proxyResolutionResult) {
        AtomicInteger identifyingPorts = new AtomicInteger(PROXY_PORT_START);
        // include broken clusters in the model, so that if they are healed the ports will stay the same
        Stream<ClusterResolutionResult> virtualKafkaClusterStream = proxyResolutionResult.allResolutionResultsInClusterNameOrder();
        List<ClusterNetworkingModel> list = virtualKafkaClusterStream
                .map(it -> planClusterNetworking(primary, it, identifyingPorts))
                .toList();
        return new ProxyNetworkingModel(list);
    }

    private static ClusterNetworkingModel planClusterNetworking(KafkaProxy primary,
                                                                ClusterResolutionResult clusterResolutionResult,
                                                                AtomicInteger identifyingPorts) {
        Stream<ClusterIngressNetworkingDefinition> networkingDefinitions = planClusterIngressNetworkingDefinitions(primary, clusterResolutionResult);
        List<ClusterIngressNetworkingModelResult> ingressResults = networkingDefinitions.map(networkingDefinition -> {
            int toAllocate = networkingDefinition.numIdentifyingPortsRequired();
            Integer firstIdentifyingPort = null;
            Integer lastIdentifyingPort = null;
            Integer sharedSniPort = null;
            IngressConflictException exception = null;
            if (toAllocate != 0) {
                if (identifyingPorts.get() != PROXY_PORT_START) {
                    exception = new IngressConflictException(name(networkingDefinition.ingress()),
                            "Currently we do not support a virtual cluster with multiple ingresses that need unique ports to identify which node the "
                                    + "client is connecting to. We currently do not have a sufficient strategy for port allocation for this case. See https://github.com/kroxylicious/kroxylicious/issues/1902");
                }
                firstIdentifyingPort = identifyingPorts.get();
                lastIdentifyingPort = identifyingPorts.addAndGet(toAllocate) - 1;
            }
            if (networkingDefinition.requiresSharedSniPort()) {
                sharedSniPort = SHARED_SNI_PORT;
            }
            ClusterIngressNetworkingModel networkingModel = networkingDefinition.createNetworkingModel(firstIdentifyingPort, lastIdentifyingPort, sharedSniPort);
            return new ClusterIngressNetworkingModelResult(networkingModel, exception);
        }).toList();
        return new ClusterNetworkingModel(clusterResolutionResult.cluster(), ingressResults);
    }

    static Stream<ClusterIngressNetworkingDefinition> planClusterIngressNetworkingDefinitions(KafkaProxy primary,
                                                                                              ClusterResolutionResult clusterResolutionResult) {
        VirtualKafkaCluster cluster = clusterResolutionResult.cluster();
        return clusterResolutionResult.ingressResolutionResults().stream()
                .flatMap(
                        ingressResolutionResult -> {
                            Optional<KafkaProxyIngress> maybeIngress = ingressResolutionResult.ingressResolutionResult().maybeReferentResource();
                            Optional<KafkaService> maybeService = clusterResolutionResult.serviceResolutionResult().maybeReferentResource();
                            // skip if ingress or service unresolved
                            return maybeIngress.map(ingress -> planClusterIngressNetworkingDefinition(primary, ingressResolutionResult, ingress, maybeService, cluster))
                                    .orElseGet(Stream::empty);
                        });
    }

    @NonNull
    private static Stream<ClusterIngressNetworkingDefinition> planClusterIngressNetworkingDefinition(KafkaProxy primary,
                                                                                                     IngressResolutionResult ingressResolutionResult,
                                                                                                     KafkaProxyIngress ingress,
                                                                                                     Optional<KafkaService> service,
                                                                                                     VirtualKafkaCluster cluster) {
        // todo we should change this so that we skip if KafkaService is not resolved
        List<NodeIdRanges> nodeIdRanges = service.map(s -> s.getSpec().getNodeIdRanges()).orElse(DEFAULT_NODE_ID_RANGES);
        try {
            ClusterIngressNetworkingDefinition definition = clusterIngressNetworkingDefinition(primary, cluster, ingress, nodeIdRanges,
                    ingressResolutionResult.ingress().getTls());
            return Stream.of(
                    definition);
        }
        catch (NetworkPlanningException e) {
            LOGGER.warn("skipping ingress {} for cluster {} due to planning exception", name(ingress), name(cluster), e);
            return Stream.empty();
        }
    }

    private static ClusterIngressNetworkingDefinition clusterIngressNetworkingDefinition(KafkaProxy primary,
                                                                                         VirtualKafkaCluster cluster,
                                                                                         KafkaProxyIngress ingress,
                                                                                         List<NodeIdRanges> nodeIdRanges,
                                                                                         @Nullable Tls tls) {
        ClusterIP clusterIP = ingress.getSpec().getClusterIP();
        LoadBalancer loadBalancer = ingress.getSpec().getLoadBalancer();
        OpenShiftRoutes openShiftRoutes = ingress.getSpec().getOpenShiftRoutes();
        if (clusterIP != null) {
            switch (clusterIP.getProtocol()) {
                case TCP -> {
                    return new TcpClusterIPClusterIngressNetworkingDefinition(ingress, cluster, primary, nodeIdRanges);
                }
                case TLS -> {
                    return new TlsClusterIPClusterIngressNetworkingDefinition(ingress, cluster, primary, nodeIdRanges, tls);
                }
                default -> throw new IllegalStateException("Unexpected clusterIP protocol: " + clusterIP.getProtocol());
            }
        }
        else if (loadBalancer != null) {
            return new LoadBalancerClusterIngressNetworkingDefinition(ingress, cluster, loadBalancer, tls);
        }
        else if (openShiftRoutes != null) {
            return new OpenShiftRoutesClusterIngressNetworkingDefinition(ingress, cluster, primary, nodeIdRanges, openShiftRoutes, tls);
        }
        else {
            throw new NetworkPlanningException("ingress must have clusterIP or loadBalancer specified");
        }
    }

    /**
     * ClusterIngressNetworkingDefinition definition for a single VirtualKafkaCluster, KafkaProxyIngress pair that:
     * <ol>
     *     <li>declares the requirements of the Ingress (e.g. how many identifying ports it requires)</li>
     *     <li>can instantiate IngressModel</li>
     * </ol>
     * Corresponds to a single ingress item in the VirtualKafkaCluster spec.ingresses
     */
    private interface ClusterIngressNetworkingDefinition {

        /**
         * The raw resource that was translated into this definition
         * @return resource
         */
        KafkaProxyIngress ingress();

        /**
         * Create an ClusterIngressNetworkingModel with identifying ports allocated to it. Identifying meaning that
         * the port on the container is expected to unambiguously identify which node the client is connecting to.
         * I.e. using a port-per-broker strategy at the proxy.
         *
         * @param firstIdentifyingPort the first identifying port allocated to this Ingress
         * @param lastIdentifyingPort the last identifying port (inclusive) allocated to this Ingress
         * @param sharedSniPort the shared SNI port (if definition required SNI port)
         * @return a non-null ClusterIngressNetworkingModel
         */
        ClusterIngressNetworkingModel createNetworkingModel(@Nullable Integer firstIdentifyingPort, @Nullable Integer lastIdentifyingPort,
                                                            @Nullable Integer sharedSniPort);

        /**
         * Some Ingress strategies require a set of ports in the proxy pod to be unique and exclusive so that the Proxy
         * can use the client's connection port to identify the cluster and upstream node id they want to communicate with.
         *
         * @return the number of identifying ports this ingress requires
         */
        default int numIdentifyingPortsRequired() {
            return 0;
        }

        default boolean requiresSharedSniPort() {
            return false;
        }
    }

    private record TcpClusterIPClusterIngressNetworkingDefinition(
                                                                  KafkaProxyIngress ingress,
                                                                  VirtualKafkaCluster cluster,
                                                                  KafkaProxy primary,
                                                                  List<NodeIdRanges> nodeIdRanges)
            implements ClusterIngressNetworkingDefinition {

        @Override
        public ClusterIngressNetworkingModel createNetworkingModel(@Nullable Integer firstIdentifyingPort, @Nullable Integer lastIdentifyingPort,
                                                                   @Nullable Integer sharedSniPort) {
            validateNotNull(firstIdentifyingPort, "firstIdentifyingPort must be non null for ClusterIP ingress");
            validateNotNull(lastIdentifyingPort, "lastIdentifyingPort must be non null for ClusterIP ingress");
            return new TcpClusterIPClusterIngressNetworkingModel(primary, cluster, ingress, nodeIdRanges, firstIdentifyingPort, lastIdentifyingPort);
        }

        @Override
        public int numIdentifyingPortsRequired() {
            return TcpClusterIPClusterIngressNetworkingModel.numIdentifyingPortsRequired(nodeIdRanges);
        }

    }

    private record TlsClusterIPClusterIngressNetworkingDefinition(
                                                                  KafkaProxyIngress ingress,
                                                                  VirtualKafkaCluster cluster,
                                                                  KafkaProxy primary,
                                                                  List<NodeIdRanges> nodeIdRanges,
                                                                  @Nullable Tls tls)
            implements ClusterIngressNetworkingDefinition {

        @Override
        public ClusterIngressNetworkingModel createNetworkingModel(@Nullable Integer firstIdentifyingPort, @Nullable Integer lastIdentifyingPort,
                                                                   @Nullable Integer sharedSniPort) {
            validateNotNull(sharedSniPort, "sharedSniPort must be non null for TLS ClusterIP ingress");
            validateNotNull(tls, "tls must be non null for TLS ClusterIP ingress");
            return new TlsClusterIPClusterIngressNetworkingModel(primary, cluster, ingress, nodeIdRanges, tls, sharedSniPort);
        }

        @Override
        public boolean requiresSharedSniPort() {
            return true;
        }
    }

    private record LoadBalancerClusterIngressNetworkingDefinition(
                                                                  KafkaProxyIngress ingress,
                                                                  VirtualKafkaCluster cluster,
                                                                  LoadBalancer loadBalancer,
                                                                  Tls tls)
            implements ClusterIngressNetworkingDefinition {

        private LoadBalancerClusterIngressNetworkingDefinition {
            validateNotNull(tls, "LoadBalancer requires TLS to be provided by the virtualkafkacluster");
        }

        @Override
        public ClusterIngressNetworkingModel createNetworkingModel(@Nullable Integer firstIdentifyingPort, @Nullable Integer lastIdentifyingPort,
                                                                   @Nullable Integer sharedSniPort) {
            Objects.requireNonNull(sharedSniPort);
            return new LoadBalancerClusterIngressNetworkingModel(cluster, ingress, loadBalancer, tls, sharedSniPort);
        }

        @Override
        public boolean requiresSharedSniPort() {
            return true;
        }
    }

    private record OpenShiftRoutesClusterIngressNetworkingDefinition(
                                                                     KafkaProxyIngress ingress,
                                                                     VirtualKafkaCluster cluster,
                                                                     KafkaProxy proxy,
                                                                     List<NodeIdRanges> nodeIdRanges,
                                                                     OpenShiftRoutes openShiftRoutes,
                                                                     Tls tls)
            implements ClusterIngressNetworkingDefinition {

        private OpenShiftRoutesClusterIngressNetworkingDefinition {
            validateNotNull(tls, "OpenShiftRoutes requires TLS to be provided by the virtualkafkacluster");
        }

        @Override
        public ClusterIngressNetworkingModel createNetworkingModel(@Nullable Integer firstIdentifyingPort, @Nullable Integer lastIdentifyingPort,
                                                                   @Nullable Integer sharedSniPort) {
            Objects.requireNonNull(sharedSniPort);
            return new OpenShiftRoutesClusterIngressNetworkingModel(cluster, ingress, proxy, openShiftRoutes, nodeIdRanges, tls, sharedSniPort);
        }

        @Override
        public boolean requiresSharedSniPort() {
            return true;
        }
    }

    private static void validateNotNull(@Nullable Object object, String message) {
        if (object == null) {
            throw new NetworkPlanningException(message);
        }
    }

}
