/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.LoadBalancerStatus;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceStatus;
import io.fabric8.kubernetes.client.utils.KubernetesResourceUtil;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;

import io.kroxylicious.kubernetes.api.common.AnyLocalRefBuilder;
import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.FilterRef;
import io.kroxylicious.kubernetes.api.common.IngressRef;
import io.kroxylicious.kubernetes.api.common.KafkaServiceRef;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.common.Protocol;
import io.kroxylicious.kubernetes.api.common.ProxyRef;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.LoadBalancer;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.OpenShiftRoute;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Ingresses;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.IngressesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.ingresses.LoadBalancerIngressPointsBuilder;
import io.kroxylicious.kubernetes.operator.Annotations;
import io.kroxylicious.kubernetes.operator.OperatorLoggingKeys;
import io.kroxylicious.kubernetes.operator.ProxyConfigStateData;
import io.kroxylicious.kubernetes.operator.ResourceState;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.StatusFactory;
import io.kroxylicious.kubernetes.operator.checksum.Crc32ChecksumGenerator;
import io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator;
import io.kroxylicious.kubernetes.operator.eventsource.EventSourceBuilder;
import io.kroxylicious.kubernetes.operator.eventsource.ReferenceDependency;
import io.kroxylicious.kubernetes.operator.informer.SharedInformerManager;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult.DanglingReference;
import io.kroxylicious.kubernetes.operator.resolver.DependencyResolver;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.fabric8.kubernetes.api.model.HasMetadata.getKind;
import static io.kroxylicious.kubernetes.api.common.Condition.Type.ResolvedRefs;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.hasFreshResolvedRefsFalseCondition;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.hasKind;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.isSecret;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toLocalRef;
import static io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy.ProxyConfigStateDependentResource.CONFIG_STATE_CONFIG_MAP_SUFFIX;

/**
 * Reconciles a {@link VirtualKafkaCluster} by checking whether the resources
 * referenced by the {@code spec.proxyRef.name}, {@code spec.targetClusterRef.name},
 * {@code spec.ingresses[].ingressRef.name} and {@code spec.filterRefs[].name} actually exist,
 * setting a {@link Condition.Type#ResolvedRefs} {@link Condition} accordingly.
 */
public final class VirtualKafkaClusterReconciler implements
        Reconciler<VirtualKafkaCluster> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualKafkaClusterReconciler.class);
    static final String KUBERNETES_SERVICES_EVENT_SOURCE_NAME = "kubernetesServices";
    private static final String KAFKA_PROXY_INGRESS_KIND = getKind(KafkaProxyIngress.class);
    static final String KAFKA_PROXY_KIND = getKind(KafkaProxy.class);
    static final String VIRTUAL_KAFKA_CLUSTER_KIND = getKind(VirtualKafkaCluster.class);
    private static final String KAFKA_SERVICE_KIND = getKind(KafkaService.class);
    private static final String KAFKA_PROTOCOL_FILTER_KIND = getKind(KafkaProtocolFilter.class);

    /** Every a primary depends on a {@code KafkaProxy} via {@code spec.proxyRef} */
    static final ReferenceDependency<VirtualKafkaCluster, KafkaProxy> PROXY_DEPENDENCY = ReferenceDependency.each(VirtualKafkaCluster.class)
            .dependsOn(KafkaProxy.class, "proxy", VirtualKafkaClusterReconciler::clusterToProxy);
    /** Every a primary depends on the {@code ${spec.proxyRef.name}-config-state} {@code ConfigMap} containing the proxy configuration */
    static final ReferenceDependency<VirtualKafkaCluster, ConfigMap> PROXY_CONFIG_DEPENDENCY = ReferenceDependency.each(VirtualKafkaCluster.class)
            .dependsOn(ConfigMap.class, "proxyConfig", VirtualKafkaClusterReconciler::clusterToProxyConfigurationConfigMap);
    /** Every a primary depends on a {@code KafkaService} via {@code spec.targetKafkaServiceRef} */
    static final ReferenceDependency<VirtualKafkaCluster, KafkaService> KAFKA_SERVICE_DEPENDENCY = ReferenceDependency.each(VirtualKafkaCluster.class)
            .dependsOn(KafkaService.class, "kafkaService", VirtualKafkaClusterReconciler::clusterToKafkaService);
    /** Every a primary depends on {@code KafkaProxyIngresses} via {@code spec.ingresses.ingressRef} */
    static final ReferenceDependency<VirtualKafkaCluster, KafkaProxyIngress> INGRESSES_DEPENDENCY = ReferenceDependency.each(VirtualKafkaCluster.class)
            .dependsOnEach(KafkaProxyIngress.class, "ingresses", VirtualKafkaClusterReconciler::clusterToIngresses);
    /** Every a primary depends on {@code KafkaProtocolFilters} via {@code spec.filterRefs} */
    static final ReferenceDependency<VirtualKafkaCluster, KafkaProtocolFilter> FILTERS_DEPENDENCY = ReferenceDependency.each(VirtualKafkaCluster.class)
            .dependsOnEach(KafkaProtocolFilter.class, "filters", VirtualKafkaClusterReconciler::clusterToProtocolFilters);
    /** Every a primary may depend on a {@code Secret} containing a certificate via {@code spec.ingresses.tls.certificateRef} */
    static final ReferenceDependency<VirtualKafkaCluster, Secret> CERT_SECRETS_DEPENDENCY = ReferenceDependency.each(VirtualKafkaCluster.class)
            .dependsOnEach(Secret.class, "certSecrets", VirtualKafkaClusterReconciler::clusterToCertSecrets);
    /** Every a primary may depend on a {@code ConfigMap} containing a trust store via {@code spec.ingresses.tls.trustAnchorRef} */
    public static final ReferenceDependency<VirtualKafkaCluster, ConfigMap> TRUST_CONFIG_MAPS = ReferenceDependency.each(VirtualKafkaCluster.class)
            .dependsOnEach(ConfigMap.class, "trustConfigMaps", VirtualKafkaClusterReconciler::clusterToTrustConfigMaps);
    /** Every a primary may depend on a {@code Secret} containing a trust store via {@code spec.ingresses.tls.trustAnchorRef} */
    static final ReferenceDependency<VirtualKafkaCluster, Secret> TRUST_SECRETS = ReferenceDependency.each(VirtualKafkaCluster.class)
            .dependsOnEach(Secret.class, "trustSecrets", VirtualKafkaClusterReconciler::clusterToTrustSecrets);
    private final VirtualKafkaClusterStatusFactory statusFactory;
    private final DependencyResolver resolver;
    private final SharedInformerManager sharedInformerManager;

    /**
     * Constructs the reconciler with the given clock, dependency resolver, and shared informer manager.
     * @param clock the clock used for status timestamps
     * @param resolver the dependency resolver for custom resource references
     * @param sharedInformerManager the shared informer manager for watch coordination
     */
    public VirtualKafkaClusterReconciler(Clock clock, DependencyResolver resolver, SharedInformerManager sharedInformerManager) {
        this.statusFactory = new VirtualKafkaClusterStatusFactory(clock);
        this.resolver = resolver;
        this.sharedInformerManager = sharedInformerManager;
    }

    /**
     * Creates a new status factory for VirtualKafkaCluster resources.
     * @param clock the clock used for status timestamps
     * @return a new status factory
     */
    public static StatusFactory<VirtualKafkaCluster> newStatusFactory(Clock clock) {
        return new VirtualKafkaClusterStatusFactory(clock);
    }

    @Override
    public UpdateControl<VirtualKafkaCluster> reconcile(VirtualKafkaCluster cluster, Context<VirtualKafkaCluster> context) {
        ClusterResolutionResult clusterResolutionResult = resolver.resolveClusterRefs(cluster, context);

        UpdateControl<VirtualKafkaCluster> reconciliationResult;
        if (clusterResolutionResult.allReferentsFullyResolved()) {
            VirtualKafkaCluster updatedCluster = checkClusterIngressTlsSettings(cluster, context);
            if (updatedCluster == null) {
                updatedCluster = maybeCombineStatusWithClusterConfigMap(cluster, context);
                MetadataChecksumGenerator checksumGenerator = context.managedWorkflowAndDependentResourceContext()
                        .get(MetadataChecksumGenerator.CHECKSUM_CONTEXT_KEY, MetadataChecksumGenerator.class)
                        .orElse(new Crc32ChecksumGenerator());
                clusterResolutionResult.allResolvedReferents().forEach(checksumGenerator::appendMetadata);

                appendSecretsFromCertificateRefs(context, cluster, checksumGenerator);

                Annotations.annotateWithReferentChecksum(updatedCluster, checksumGenerator.encode());
                reconciliationResult = UpdateControl.patchResourceAndStatus(updatedCluster);
            }
            else {
                reconciliationResult = UpdateControl.patchStatus(updatedCluster);
            }
        }
        else {
            reconciliationResult = UpdateControl.patchStatus(handleResolutionProblems(cluster, clusterResolutionResult));
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.atInfo()
                    .addKeyValue(OperatorLoggingKeys.NAMESPACE, namespace(cluster))
                    .addKeyValue(OperatorLoggingKeys.NAME, name(cluster))
                    .log("Completed reconciliation");
        }
        return reconciliationResult;
    }

    private static void appendSecretsFromCertificateRefs(Context<VirtualKafkaCluster> context, VirtualKafkaCluster updatedCluster,
                                                         @NonNull MetadataChecksumGenerator checksumGenerator) {
        LOGGER.atDebug().log("Including secrets from ingress TLS in checksum");
        updatedCluster.getSpec()
                .getIngresses()
                .stream()
                .map(Ingresses::getTls)
                .filter(Objects::nonNull)
                .map(Tls::getCertificateRef)
                .flatMap(certificateRef -> context.getSecondaryResourcesAsStream(Secret.class)
                        .filter(secret -> KubernetesResourceUtil.getName(secret).equals(certificateRef.getName())))
                .forEach(checksumGenerator::appendMetadata);

        LOGGER.atDebug().log("Including TrustAnchors from ingress TLS in checksum");
        updatedCluster.getSpec()
                .getIngresses()
                .stream()
                .map(Ingresses::getTls)
                .filter(Objects::nonNull)
                .map(Tls::getTrustAnchorRef)
                .filter(Objects::nonNull)
                .flatMap(trustAnchorRef -> resolveTrustAnchor(context, trustAnchorRef)).forEach(checksumGenerator::appendMetadata);
    }

    private static Stream<? extends HasMetadata> resolveTrustAnchor(Context<VirtualKafkaCluster> context, TrustAnchorRef trustAnchorRef) {
        Class<? extends HasMetadata> resourceClass = isSecret(trustAnchorRef) ? Secret.class : ConfigMap.class;

        return context.getSecondaryResourcesAsStream(resourceClass)
                .filter(resource -> KubernetesResourceUtil.getName(resource)
                        .equals(trustAnchorRef.getRef().getName()));
    }

    /**
     * Checks the validity of the cluster's ingress TLS settings. Specifically it checks whether
     * the TLS specification given by the KafkaProxyIngress matches what has been provided in
     * the ingress section of the VirtualKafkaCluster. It also verifies that the resource providing
     * the TLS certificate meets certain rules.  If any of those conditions are false, a
     * condition is added to the resource and the modified resource returned. If the resource is
     * valid, null is returned.
     *
     * @param cluster virtual kafka cluster
     * @param context context
     * @return null if ingress tls settings are valid, a modified resource otherwise.
     */
    @Nullable
    private VirtualKafkaCluster checkClusterIngressTlsSettings(VirtualKafkaCluster cluster, Context<VirtualKafkaCluster> context) {
        var tlsConsistencyCheck = cluster.getSpec().getIngresses().stream()
                .map(ingress -> checkTlsConfigConsistency(context, cluster, ingress))
                .filter(Objects::nonNull)
                .findFirst();
        if (tlsConsistencyCheck.isPresent()) {
            return tlsConsistencyCheck.get();
        }

        VirtualKafkaCluster updatedVirtualCluster = checkIngressCertificateRefs(cluster, context);
        if (updatedVirtualCluster == null) {
            updatedVirtualCluster = checkIngressTrustAnchorRefs(cluster, context);
        }
        return updatedVirtualCluster;
    }

    @Nullable
    private VirtualKafkaCluster checkIngressCertificateRefs(VirtualKafkaCluster cluster, Context<VirtualKafkaCluster> context) {
        var certRefs = cluster.getSpec().getIngresses().stream()
                .flatMap(ingress -> Optional.ofNullable(ingress.getTls()).stream())
                .map(Tls::getCertificateRef)
                .toList();
        if (!certRefs.isEmpty()) {
            var certRefCheck = certRefs.stream()
                    .map(certRef -> {
                        var path = "spec.ingresses[].tls.certificateRef";
                        return ResourcesUtil.checkCertRef(cluster, certRef, path, statusFactory, context, CERT_SECRETS_DEPENDENCY.eventSourceName()).resource();
                    })
                    .filter(Objects::nonNull)
                    .findFirst();
            if (certRefCheck.isPresent()) {
                return certRefCheck.get();
            }
        }
        return null;
    }

    @Nullable
    private VirtualKafkaCluster checkIngressTrustAnchorRefs(VirtualKafkaCluster cluster, Context<VirtualKafkaCluster> context) {
        var trustRefs = cluster.getSpec().getIngresses().stream()
                .flatMap(ingress -> Optional.ofNullable(ingress.getTls()).stream())
                .map(Tls::getTrustAnchorRef)
                .filter(Objects::nonNull)
                .toList();
        if (!trustRefs.isEmpty()) {
            var trustAnchorCheck = trustRefs.stream()
                    .map(trustAnchorRef -> {
                        var path = "spec.ingresses[].tls.trustAnchor";
                        String eventSourceName = isSecret(trustAnchorRef)
                                ? TRUST_SECRETS.eventSourceName()
                                : TRUST_CONFIG_MAPS.eventSourceName();
                        return ResourcesUtil.checkTrustAnchorRef(cluster, context, eventSourceName, trustAnchorRef, path,
                                statusFactory).resource();
                    })
                    .filter(Objects::nonNull)
                    .findFirst();
            if (trustAnchorCheck.isPresent()) {
                return trustAnchorCheck.get();
            }
        }
        return null;
    }

    private VirtualKafkaCluster maybeCombineStatusWithClusterConfigMap(VirtualKafkaCluster cluster, Context<VirtualKafkaCluster> context) {

        var ingresses = buildIngressStatus(cluster, context);

        ResourceState resolvedRefsTrueResourceState = ResourceState.of(statusFactory.newTrueCondition(cluster, Condition.Type.ResolvedRefs));
        return context
                .getSecondaryResource(ConfigMap.class, PROXY_CONFIG_DEPENDENCY.eventSourceName())
                .flatMap(cm -> Optional.ofNullable(cm.getData()))
                .map(ProxyConfigStateData::new)
                .flatMap(data -> data.getStatusPatchForCluster(name(cluster)))
                .map(patch -> {
                    var patchResourceState = ResourceState.fromList(patch.getStatus().getConditions());
                    return statusFactory.clusterStatusPatch(cluster, resolvedRefsTrueResourceState.replacementFor(patchResourceState), ingresses);
                })
                .orElse(statusFactory.clusterStatusPatch(cluster, resolvedRefsTrueResourceState, ingresses));

    }

    record ServiceBootstrap(Service service, Annotations.ClusterIngressBootstrapServers bootstrap) {
        boolean matchesIngress(String ingressName, String clusterName) {
            return bootstrap.matchesIngress(ingressName, clusterName);
        }
    }

    private List<io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.Ingresses> buildIngressStatus(VirtualKafkaCluster cluster,
                                                                                                                 Context<VirtualKafkaCluster> context) {
        var bootstraps = context.getSecondaryResources(Service.class).stream()
                .flatMap(service -> Annotations.readBootstrapServersFrom(service).stream().map(bootstrap -> new ServiceBootstrap(service, bootstrap)))
                .toList();
        return cluster.getSpec().getIngresses()
                .stream()
                .flatMap(
                        ingress -> {
                            Optional<ServiceBootstrap> bootstrap = bootstraps.stream().filter(
                                    b -> b.matchesIngress(ingress.getIngressRef().getName(), name(cluster)))
                                    .findFirst();
                            return bootstrap.map(serviceBootstrap -> buildStatusIngress(ingress, serviceBootstrap)).orElseGet(Stream::empty);
                        })
                .toList();
    }

    private static Stream<io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.Ingresses> buildStatusIngress(Ingresses ingress,
                                                                                                                          ServiceBootstrap serviceBootstrap) {
        var serverCert = Optional.ofNullable(ingress.getTls()).map(Tls::getCertificateRef);
        var builder = new IngressesBuilder()
                .withName(ingress.getIngressRef().getName())
                .withBootstrapServer(serviceBootstrap.bootstrap().bootstrapServers())
                .withProtocol(serverCert.isEmpty() ? Protocol.TCP : Protocol.TLS);
        List<LoadBalancerIngress> loadBalancerIngresses = Optional.ofNullable(serviceBootstrap.service().getStatus())
                .map(ServiceStatus::getLoadBalancer).map(LoadBalancerStatus::getIngress).orElse(List.of());
        loadBalancerIngresses.stream()
                .map(loadBalancerIngress -> new LoadBalancerIngressPointsBuilder().withIp(loadBalancerIngress.getIp())
                        .withHostname(loadBalancerIngress.getHostname()).build())
                .forEach(builder::addToLoadBalancerIngressPoints);
        return Stream.of(builder.build());
    }

    private VirtualKafkaCluster handleResolutionProblems(VirtualKafkaCluster cluster,
                                                         ClusterResolutionResult clusterResolutionResult) {
        LocalRef<VirtualKafkaCluster> clusterRef = toLocalRef(cluster);
        var unresolvedIngressProxies = clusterResolutionResult.allDanglingReferences()
                .filter(DanglingReference.hasReferrerKind(KAFKA_PROXY_INGRESS_KIND).and(DanglingReference.hasReferentKind(KAFKA_PROXY_KIND))).collect(Collectors.toSet());
        if (clusterResolutionResult.allDanglingReferences().anyMatch(DanglingReference.hasReferrer(clusterRef))) {
            Stream<String> proxyMsg = refsMessage("spec.proxyRef references ", cluster,
                    clusterResolutionResult.allDanglingReferences()
                            .filter(DanglingReference.hasReferrer(clusterRef).and(DanglingReference.hasReferentKind(KAFKA_PROXY_KIND)))
                            .map(DanglingReference::absentRef));
            Stream<String> serviceMsg = refsMessage("spec.targetKafkaServiceRef references ", cluster,
                    clusterResolutionResult.allDanglingReferences()
                            .filter(DanglingReference.hasReferrer(clusterRef).and(DanglingReference.hasReferentKind(KAFKA_SERVICE_KIND)))
                            .map(DanglingReference::absentRef));
            Stream<String> ingressMsg = refsMessage("spec.ingresses[].ingressRef references ", cluster,
                    clusterResolutionResult.allDanglingReferences()
                            .filter(DanglingReference.hasReferrer(clusterRef).and(DanglingReference.hasReferentKind(KAFKA_PROXY_INGRESS_KIND)))
                            .map(DanglingReference::absentRef));
            Stream<String> filterMsg = refsMessage("spec.filterRefs references ", cluster,
                    clusterResolutionResult.allDanglingReferences()
                            .filter(DanglingReference.hasReferrer(clusterRef).and(DanglingReference.hasReferentKind(KAFKA_PROTOCOL_FILTER_KIND)))
                            .map(DanglingReference::absentRef));
            return statusFactory.newFalseConditionStatusPatch(cluster, Condition.Type.ResolvedRefs, Condition.REASON_REFS_NOT_FOUND,
                    joiningMessages(proxyMsg, serviceMsg, ingressMsg, filterMsg));
        }
        else if (clusterResolutionResult.allResolvedReferents().anyMatch(ResourcesUtil::hasFreshResolvedRefsFalseCondition) || !unresolvedIngressProxies.isEmpty()) {
            Stream<String> serviceMsg = refsMessage("spec.targetKafkaServiceRef references ", cluster,
                    clusterResolutionResult.allResolvedReferents().filter(hasFreshResolvedRefsFalseCondition().and(hasKind(KAFKA_SERVICE_KIND)))
                            .map(ResourcesUtil::toLocalRef));
            Stream<String> ingressMsg = refsMessage("spec.ingresses[].ingressRef references ", cluster,
                    clusterResolutionResult.allResolvedReferents()
                            .filter(hasFreshResolvedRefsFalseCondition().and(hasKind(KAFKA_PROXY_INGRESS_KIND))).map(ResourcesUtil::toLocalRef));
            Stream<String> filterMsg = refsMessage("spec.filterRefs references ", cluster,
                    clusterResolutionResult.allResolvedReferents()
                            .filter(hasFreshResolvedRefsFalseCondition().and(hasKind(KAFKA_PROTOCOL_FILTER_KIND)))
                            .map(ResourcesUtil::toLocalRef));
            Stream<String> ingressProxyMessage = refsMessage("a spec.ingresses[].ingressRef had an inconsistent or missing proxyRef ", cluster,
                    clusterResolutionResult.allDanglingReferences()
                            .filter(DanglingReference.hasReferrerKind(KAFKA_PROXY_INGRESS_KIND).and(DanglingReference.hasReferentKind(KAFKA_PROXY_KIND)))
                            .map(DanglingReference::absentRef));
            return statusFactory.newFalseConditionStatusPatch(cluster, Condition.Type.ResolvedRefs,
                    Condition.REASON_TRANSITIVE_REFS_NOT_FOUND,
                    joiningMessages(serviceMsg, ingressMsg, filterMsg, ingressProxyMessage));
        }
        else {
            return statusFactory.newFalseConditionStatusPatch(cluster, Condition.Type.ResolvedRefs, Condition.REASON_INVALID,
                    joiningMessages(Stream.of("unknown dependency resolution issue")));
        }
    }

    @SafeVarargs
    private static String joiningMessages(
                                          Stream<String>... serviceMsg) {
        return Stream.of(serviceMsg).flatMap(Function.identity()).collect(Collectors.joining("; "));
    }

    private static Stream<String> refsMessage(
                                              String prefix,
                                              VirtualKafkaCluster cluster,
                                              Stream<LocalRef<?>> refs) {
        List<LocalRef<?>> sortedRefs = refs.sorted().toList();
        return sortedRefs.isEmpty() ? Stream.of()
                : Stream.of(
                        prefix + sortedRefs.stream()
                                .map(ref -> ResourcesUtil.namespacedSlug(ref, cluster))
                                .collect(Collectors.joining(", ")));
    }

    @Override
    public List<EventSource<?, VirtualKafkaCluster>> prepareEventSources(EventSourceContext<VirtualKafkaCluster> context) {
        return List.of(
                PROXY_DEPENDENCY
                        .eventSourceBuilder(context)
                        // reconcile on every proxy change regardless of the proxy's own reconcile state
                        // (VKC consumes no reconciled proxy state, so a guard would add latency, and is not needed for correctness)
                        .withoutFreshStatus()
                        .build(),
                PROXY_CONFIG_DEPENDENCY
                        .eventSourceBuilder(context)
                        .withSharedInformer(sharedInformerManager)
                        .build(),
                KAFKA_SERVICE_DEPENDENCY
                        .eventSourceBuilder(context)
                        .build(),
                INGRESSES_DEPENDENCY
                        .eventSourceBuilder(context)
                        .build(),
                FILTERS_DEPENDENCY
                        .eventSourceBuilder(context)
                        .build(),
                EventSourceBuilder.forSecondary(context, Service.class)
                        .withName(KUBERNETES_SERVICES_EVENT_SOURCE_NAME)
                        .withMappers(new VirtualKafkaClusterPrimaryToKubernetesServiceSecondaryMapper(),
                                new KubernetesServicesSecondaryToVirtualKafkaClusterPrimaryMapper(context))
                        .build(),
                CERT_SECRETS_DEPENDENCY
                        .eventSourceBuilder(context)
                        .withSharedInformer(sharedInformerManager)
                        .build(),
                TRUST_CONFIG_MAPS
                        .eventSourceBuilder(context)
                        .withSharedInformer(sharedInformerManager)
                        .build(),
                TRUST_SECRETS
                        .eventSourceBuilder(context)
                        .withSharedInformer(sharedInformerManager)
                        .build());
    }

    @NonNull
    private static Optional<ProxyRef> clusterToProxy(VirtualKafkaCluster cluster) {
        return Optional.of(cluster.getSpec().getProxyRef());
    }

    @NonNull
    private static List<FilterRef> clusterToProtocolFilters(VirtualKafkaCluster cluster) {
        return Optional.ofNullable(cluster.getSpec())
                .map(VirtualKafkaClusterSpec::getFilterRefs).orElse(List.of());
    }

    @NonNull
    private static Optional<KafkaServiceRef> clusterToKafkaService(VirtualKafkaCluster cluster) {
        return Optional.of(cluster.getSpec().getTargetKafkaServiceRef());
    }

    @SuppressWarnings("unchecked")
    private static Optional<LocalRef<ConfigMap>> clusterToProxyConfigurationConfigMap(VirtualKafkaCluster cluster) {
        return Optional.of((LocalRef<ConfigMap>) (LocalRef<?>) new AnyLocalRefBuilder()
                .withGroup("").withKind("ConfigMap")
                .withName(cluster.getSpec().getProxyRef().getName() + CONFIG_STATE_CONFIG_MAP_SUFFIX)
                .build());
    }

    private static Collection<IngressRef> clusterToIngresses(VirtualKafkaCluster cluster) {
        return Optional.ofNullable(cluster.getSpec())
                .map(VirtualKafkaClusterSpec::getIngresses)
                .stream().flatMap(List::stream)
                .map(Ingresses::getIngressRef).toList();
    }

    @SuppressWarnings("unchecked")
    private static Collection<LocalRef<Secret>> clusterToCertSecrets(VirtualKafkaCluster cluster) {
        return cluster.getSpec().getIngresses().stream()
                .flatMap(ingress -> Optional.ofNullable(ingress.getTls()).stream())
                .map(ingresses -> (LocalRef<Secret>) (LocalRef<?>) ingresses.getCertificateRef())
                .toList();
    }

    @SuppressWarnings("unchecked")
    private static Collection<LocalRef<Secret>> clusterToTrustSecrets(VirtualKafkaCluster cluster) {
        return cluster.getSpec().getIngresses().stream()
                .flatMap(ingress -> Optional.ofNullable(ingress.getTls()).stream())
                .map(Tls::getTrustAnchorRef)
                .filter(Objects::nonNull)
                .map(TrustAnchorRef::getRef)
                .filter(ResourcesUtil::isSecret)
                .map(ref -> (LocalRef<Secret>) (LocalRef<?>) ref)
                .toList();
    }

    @SuppressWarnings("unchecked")
    private static Collection<LocalRef<ConfigMap>> clusterToTrustConfigMaps(VirtualKafkaCluster cluster) {
        return cluster.getSpec().getIngresses().stream()
                .flatMap(ingress -> Optional.ofNullable(ingress.getTls()).stream())
                .map(Tls::getTrustAnchorRef)
                .filter(Objects::nonNull)
                .map(TrustAnchorRef::getRef)
                .filter(ResourcesUtil::isConfigMap)
                .map(ref -> (LocalRef<ConfigMap>) (LocalRef<?>) ref)
                .toList();
    }

    static void logIgnoredEvent(HasMetadata hasMetadata) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.atDebug()
                    .addKeyValue("kind", HasMetadata.getKind(hasMetadata.getClass()))
                    .addKeyValue("resource", ResourcesUtil.toLocalRef(hasMetadata))
                    .log("Ignoring event from resource with stale status");
        }
    }

    @Override
    public ErrorStatusUpdateControl<VirtualKafkaCluster> updateErrorStatus(VirtualKafkaCluster cluster, Context<VirtualKafkaCluster> context, Exception e) {
        // ResolvedRefs to UNKNOWN
        ErrorStatusUpdateControl<VirtualKafkaCluster> uc = ErrorStatusUpdateControl
                .patchStatus(statusFactory.newUnknownConditionStatusPatch(cluster, Condition.Type.ResolvedRefs, e));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.atInfo()
                    .addKeyValue(OperatorLoggingKeys.NAMESPACE, namespace(cluster))
                    .addKeyValue(OperatorLoggingKeys.NAME, name(cluster))
                    .addKeyValue(OperatorLoggingKeys.ERROR, e.toString())
                    .log("Completed reconciliation with error");
        }
        return uc;
    }

    @Nullable
    private VirtualKafkaCluster checkTlsConfigConsistency(Context<VirtualKafkaCluster> context, VirtualKafkaCluster cluster, Ingresses clusterIngress) {
        var ingressName = Objects.requireNonNull(clusterIngress.getIngressRef().getName());
        var proxyIngressOpt = context.getSecondaryResource(KafkaProxyIngress.class, INGRESSES_DEPENDENCY.eventSourceName(), ingressName);

        if (proxyIngressOpt.isPresent()) {
            KafkaProxyIngress proxyIngress = proxyIngressOpt.get();

            var proxyIngressDefinesTls = getIngressProtocol(proxyIngress) == Protocol.TLS;
            var clusterDefinesTls = clusterIngress.getTls() != null;
            if (clusterDefinesTls != proxyIngressDefinesTls) {
                var slug = ResourcesUtil.namespacedSlug(toLocalRef(proxyIngress), proxyIngress);
                var name = clusterIngress.getIngressRef().getName();
                var message = proxyIngressDefinesTls
                        ? "spec.ingresses[].tls: Inconsistent TLS configuration. %s requires the use of TLS but the cluster ingress (%s) does not define a tls object."
                                .formatted(
                                        slug, name)
                        : "spec.ingresses[].tls: Inconsistent TLS configuration. %s requires the use of TCP but the cluster ingress (%s) defines a tls object.".formatted(
                                slug, name);
                return statusFactory.newFalseConditionStatusPatch(cluster, ResolvedRefs,
                        Condition.REASON_INVALID_REFERENCED_RESOURCE, message);
            }
        }
        return null;
    }

    private static Protocol getIngressProtocol(KafkaProxyIngress proxyIngress) {
        ClusterIP clusterIP = proxyIngress.getSpec().getClusterIP();
        if (clusterIP != null) {
            return clusterIP.getProtocol();
        }
        LoadBalancer loadBalancer = proxyIngress.getSpec().getLoadBalancer();
        if (loadBalancer != null) {
            return Protocol.TLS;
        }
        OpenShiftRoute openShiftRoute = proxyIngress.getSpec().getOpenShiftRoute();
        if (openShiftRoute != null) {
            return Protocol.TLS;
        }
        throw new IllegalStateException("No protocol could be determined for " + proxyIngress);
    }

}
