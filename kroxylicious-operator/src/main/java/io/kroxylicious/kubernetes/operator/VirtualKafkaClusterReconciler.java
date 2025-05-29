/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.client.utils.KubernetesResourceUtil;
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.api.common.AnyLocalRefBuilder;
import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.IngressRefBuilder;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Ingresses;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.Ingresses.Protocol;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.IngressesBuilder;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.operator.checksum.Crc32ChecksumGenerator;
import io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult.DanglingReference;
import io.kroxylicious.kubernetes.operator.resolver.DependencyResolver;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.fabric8.kubernetes.api.model.HasMetadata.getKind;
import static io.kroxylicious.kubernetes.api.common.Condition.Type.ResolvedRefs;
import static io.kroxylicious.kubernetes.operator.ProxyConfigStateDependentResource.CONFIG_STATE_CONFIG_MAP_SUFFIX;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.hasFreshResolvedRefsFalseCondition;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.hasKind;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toLocalRef;
import static io.kroxylicious.kubernetes.operator.model.networking.ClusterIPClusterIngressNetworkingModel.bootstrapServiceName;

/**
 * Reconciles a {@link VirtualKafkaCluster} by checking whether the resources
 * referenced by the {@code spec.proxyRef.name}, {@code spec.targetClusterRef.name},
 * {@code spec.ingresses[].ingressRef.name} and {@code spec.filterRefs[].name} actually exist,
 * setting a {@link Condition.Type#ResolvedRefs} {@link Condition} accordingly.
 */
public final class VirtualKafkaClusterReconciler implements
        Reconciler<VirtualKafkaCluster> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualKafkaClusterReconciler.class);
    static final String PROXY_EVENT_SOURCE_NAME = "proxy";
    static final String PROXY_CONFIG_STATE_SOURCE_NAME = "proxy-config-state";
    static final String SERVICES_EVENT_SOURCE_NAME = "services";
    static final String INGRESSES_EVENT_SOURCE_NAME = "ingresses";
    static final String FILTERS_EVENT_SOURCE_NAME = "filters";
    static final String SECRETS_EVENT_SOURCE_NAME = "secrets";
    static final String CONFIGMAPS_EVENT_SOURCE_NAME = "configmaps";
    static final String KUBERNETES_SERVICES_EVENT_SOURCE_NAME = "kubernetesServices";
    private static final String KAFKA_PROXY_INGRESS_KIND = getKind(KafkaProxyIngress.class);
    private static final String KAFKA_PROXY_KIND = getKind(KafkaProxy.class);
    private static final String VIRTUAL_KAFKA_CLUSTER_KIND = getKind(VirtualKafkaCluster.class);
    private static final String KAFKA_SERVICE_KIND = getKind(KafkaService.class);
    private static final String KAFKA_PROTOCOL_FILTER_KIND = getKind(KafkaProtocolFilter.class);
    private final VirtualKafkaClusterStatusFactory statusFactory;
    private final DependencyResolver resolver;

    public VirtualKafkaClusterReconciler(Clock clock, DependencyResolver resolver) {
        this.statusFactory = new VirtualKafkaClusterStatusFactory(clock);
        this.resolver = resolver;
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

                KubernetesResourceUtil.getOrCreateAnnotations(updatedCluster)
                        .put(MetadataChecksumGenerator.REFERENT_CHECKSUM_ANNOTATION,
                                checksumGenerator.encode());
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
            LOGGER.info("Completed reconciliation of {}/{}", namespace(cluster), name(cluster));
        }
        return reconciliationResult;
    }

    private static void appendSecretsFromCertificateRefs(Context<VirtualKafkaCluster> context, VirtualKafkaCluster updatedCluster,
                                                         @NonNull MetadataChecksumGenerator checksumGenerator) {
        LOGGER.debug("Including secrets from ingress TLS in checksum");
        updatedCluster.getSpec().getIngresses().stream()
                .map(Ingresses::getTls)
                .filter(Objects::nonNull)
                .map(Tls::getCertificateRef)
                .flatMap(certificateRef -> context.getSecondaryResourcesAsStream(Secret.class)
                        .filter(secret -> secret.getMetadata().getName().equals(certificateRef.getName())))

                .forEach(checksumGenerator::appendMetadata);
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
                        return ResourcesUtil.checkCertRef(cluster, certRef, path, statusFactory, context, SECRETS_EVENT_SOURCE_NAME).resource();
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
                        return ResourcesUtil.checkTrustAnchorRef(cluster, context, CONFIGMAPS_EVENT_SOURCE_NAME, trustAnchorRef, path, statusFactory).resource();
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
                .getSecondaryResource(ConfigMap.class, PROXY_CONFIG_STATE_SOURCE_NAME)
                .flatMap(cm -> Optional.ofNullable(cm.getData()))
                .map(ProxyConfigStateData::new)
                .flatMap(data -> data.getStatusPatchForCluster(name(cluster)))
                .map(patch -> {
                    var patchResourceState = ResourceState.fromList(patch.getStatus().getConditions());
                    return statusFactory.clusterStatusPatch(cluster, resolvedRefsTrueResourceState.replacementFor(patchResourceState), ingresses);
                })
                .orElse(statusFactory.clusterStatusPatch(cluster, resolvedRefsTrueResourceState, ingresses));

    }

    private List<io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.Ingresses> buildIngressStatus(VirtualKafkaCluster cluster,
                                                                                                                 Context<VirtualKafkaCluster> context) {
        var existingKubernetesServices = context.getSecondaryResources(Service.class)
                .stream()
                .collect(Collectors.toMap(ref -> {
                    var kubeServiceIngressOwner = Optional.ofNullable(ref)
                            .flatMap(service -> extractOwnerRefFromKubernetesService(service, KAFKA_PROXY_INGRESS_KIND))
                            .orElseThrow();
                    return new IngressRefBuilder().withName(kubeServiceIngressOwner.getName()).build();
                }, Function.identity()));

        var ingressServiceMap = cluster.getSpec().getIngresses()
                .stream()
                .collect(Collectors.toMap(Function.identity(),
                        ingress -> Optional.ofNullable(existingKubernetesServices.get(ingress.getIngressRef()))));

        return ingressServiceMap.entrySet()
                .stream()
                .filter(entry -> entry.getValue().isPresent())
                .map(entry -> {
                    var ingress = entry.getKey();
                    var kubernetesService = entry.getValue().get();
                    var serverCert = Optional.ofNullable(ingress.getTls()).map(Tls::getCertificateRef);
                    var builder = new IngressesBuilder()
                            .withName(ingress.getIngressRef().getName())
                            .withBootstrapServer(getBootstrapServer(kubernetesService))
                            .withProtocol(serverCert.isEmpty() ? Protocol.TCP : Protocol.TLS);
                    return builder.build();
                }).toList();
    }

    private String getBootstrapServer(Service kubenetesService) {
        var metadata = kubenetesService.getMetadata();
        var bootstrapPort = kubenetesService.getSpec().getPorts().stream()
                .map(ServicePort::getPort)
                .findFirst()
                .orElseThrow();
        return metadata.getName() + "." + metadata.getNamespace() + ".svc.cluster.local:" + bootstrapPort;
    }

    private Optional<OwnerReference> extractOwnerRefFromKubernetesService(Service service, String ownerKind) {
        return service.getMetadata()
                .getOwnerReferences()
                .stream()
                .filter(or -> ownerKind.equals(or.getKind()))
                .findFirst();
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
        InformerEventSourceConfiguration<KafkaProxy> clusterToProxy = InformerEventSourceConfiguration.from(
                KafkaProxy.class,
                VirtualKafkaCluster.class)
                .withName(PROXY_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefAsResourceId(cluster, cluster.getSpec().getProxyRef()))
                .withSecondaryToPrimaryMapper(proxy -> ResourcesUtil.findReferrers(context,
                        proxy,
                        VirtualKafkaCluster.class,
                        cluster -> Optional.of(cluster.getSpec().getProxyRef())))
                .build();

        InformerEventSourceConfiguration<ConfigMap> clusterToProxyConfigState = InformerEventSourceConfiguration.from(
                ConfigMap.class,
                VirtualKafkaCluster.class)
                .withName(PROXY_CONFIG_STATE_SOURCE_NAME)
                .withPrimaryToSecondaryMapper(VirtualKafkaClusterReconciler::toConfigStateResourceName)
                .withSecondaryToPrimaryMapper(configMap -> ResourcesUtil.findReferrers(context,
                        configMap,
                        VirtualKafkaCluster.class,
                        cluster -> Optional.of(new AnyLocalRefBuilder().withGroup("").withKind("ConfigMap")
                                .withName(cluster.getSpec().getProxyRef().getName() + CONFIG_STATE_CONFIG_MAP_SUFFIX)
                                .build())))
                .build();

        InformerEventSourceConfiguration<KafkaService> clusterToService = InformerEventSourceConfiguration.from(
                KafkaService.class,
                VirtualKafkaCluster.class)
                .withName(SERVICES_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefAsResourceId(cluster,
                        cluster.getSpec().getTargetKafkaServiceRef()))
                .withSecondaryToPrimaryMapper(kafkaServiceSecondaryToPrimaryMapper(context))
                .build();

        InformerEventSourceConfiguration<KafkaProxyIngress> clusterToIngresses = InformerEventSourceConfiguration.from(
                KafkaProxyIngress.class,
                VirtualKafkaCluster.class)
                .withName(INGRESSES_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefsAsResourceIds(cluster,
                        Optional.ofNullable(cluster.getSpec())
                                .map(VirtualKafkaClusterSpec::getIngresses)
                                .stream().flatMap(List::stream)
                                .map(Ingresses::getIngressRef).toList()))
                .withSecondaryToPrimaryMapper(ingressSecondaryToPrimaryMapper(context))
                .build();

        InformerEventSourceConfiguration<KafkaProtocolFilter> clusterToFilters = InformerEventSourceConfiguration.from(
                KafkaProtocolFilter.class,
                VirtualKafkaCluster.class)
                .withName(FILTERS_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> ResourcesUtil.localRefsAsResourceIds(cluster,
                        Optional.ofNullable(cluster.getSpec()).map(VirtualKafkaClusterSpec::getFilterRefs).orElse(List.of())))
                .withSecondaryToPrimaryMapper(filterSecondaryToPrimaryMapper(context))
                .build();

        InformerEventSourceConfiguration<Service> clusterToKubeService = InformerEventSourceConfiguration.from(
                Service.class,
                VirtualKafkaCluster.class)
                .withName(KUBERNETES_SERVICES_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper((VirtualKafkaCluster cluster) -> {
                    var name = cluster.getMetadata().getName();
                    return cluster.getSpec().getIngresses()
                            .stream()
                            .map(Ingresses::getIngressRef)
                            .flatMap(ir -> ResourcesUtil
                                    .localRefAsResourceId(cluster, new AnyLocalRefBuilder().withName(bootstrapServiceName(cluster, ir.getName())).build()).stream())
                            .collect(Collectors.toSet());
                })
                .withSecondaryToPrimaryMapper(kubenetesService -> Optional.of(kubenetesService)
                        .flatMap(service -> extractOwnerRefFromKubernetesService(service, VIRTUAL_KAFKA_CLUSTER_KIND))
                        .map(ownerRef -> new ResourceID(ownerRef.getName(), kubenetesService.getMetadata().getNamespace()))
                        .map(Set::of).orElse(Set.of()))
                .build();

        InformerEventSourceConfiguration<Secret> clusterToSecret = InformerEventSourceConfiguration.from(
                Secret.class,
                VirtualKafkaCluster.class)
                .withName(SECRETS_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper(virtualKafkaClusterToSecret())
                .withSecondaryToPrimaryMapper(secretToVirtualKafkaCluster(context))
                .build();

        InformerEventSourceConfiguration<ConfigMap> clusterToConfigMap = InformerEventSourceConfiguration.from(
                ConfigMap.class,
                VirtualKafkaCluster.class)
                .withName(CONFIGMAPS_EVENT_SOURCE_NAME)
                .withPrimaryToSecondaryMapper(virtualKafkaClusterToConfigMap())
                .withSecondaryToPrimaryMapper(configMapToVirtualKafkaCluster(context))
                .build();

        return List.of(
                new InformerEventSource<>(clusterToProxy, context),
                new InformerEventSource<>(clusterToProxyConfigState, context),
                new InformerEventSource<>(clusterToIngresses, context),
                new InformerEventSource<>(clusterToService, context),
                new InformerEventSource<>(clusterToFilters, context),
                new InformerEventSource<>(clusterToKubeService, context),
                new InformerEventSource<>(clusterToSecret, context),
                new InformerEventSource<>(clusterToConfigMap, context));
    }

    @VisibleForTesting
    static PrimaryToSecondaryMapper<VirtualKafkaCluster> virtualKafkaClusterToSecret() {
        return (VirtualKafkaCluster cluster) -> ResourcesUtil.localRefsAsResourceIds(cluster,
                cluster.getSpec().getIngresses().stream()
                        .flatMap(ingress -> Optional.ofNullable(ingress.getTls()).stream())
                        .map(Tls::getCertificateRef)
                        .toList());
    }

    @VisibleForTesting
    static SecondaryToPrimaryMapper<Secret> secretToVirtualKafkaCluster(EventSourceContext<VirtualKafkaCluster> context) {
        return secret -> ResourcesUtil.findReferrersMulti(context,
                secret,
                VirtualKafkaCluster.class,
                cluster -> cluster.getSpec().getIngresses().stream()
                        .flatMap(ingress -> Optional.ofNullable(ingress.getTls()).stream())
                        .map(Tls::getCertificateRef).toList());
    }

    @VisibleForTesting
    static PrimaryToSecondaryMapper<VirtualKafkaCluster> virtualKafkaClusterToConfigMap() {
        return (VirtualKafkaCluster cluster) -> ResourcesUtil.localRefsAsResourceIds(cluster,
                cluster.getSpec().getIngresses().stream()
                        .flatMap(ingress -> Optional.ofNullable(ingress.getTls()).stream())
                        .map(Tls::getTrustAnchorRef)
                        .filter(Objects::nonNull)
                        .map(TrustAnchorRef::getRef)
                        .toList());
    }

    @VisibleForTesting
    static SecondaryToPrimaryMapper<ConfigMap> configMapToVirtualKafkaCluster(EventSourceContext<VirtualKafkaCluster> context) {
        return configMap -> ResourcesUtil.findReferrersMulti(context,
                configMap,
                VirtualKafkaCluster.class,
                cluster -> cluster.getSpec().getIngresses().stream()
                        .flatMap(ingress -> Optional.ofNullable(ingress.getTls()).stream())
                        .map(Tls::getTrustAnchorRef)
                        .filter(Objects::nonNull)
                        .map(TrustAnchorRef::getRef)
                        .toList());
    }

    @VisibleForTesting
    static SecondaryToPrimaryMapper<KafkaProtocolFilter> filterSecondaryToPrimaryMapper(EventSourceContext<VirtualKafkaCluster> context) {
        return filter -> {
            if (!ResourcesUtil.isStatusFresh(filter)) {
                logIgnoredEvent(filter);
                return Set.of();
            }
            return ResourcesUtil.findReferrersMulti(context,
                    filter,
                    VirtualKafkaCluster.class,
                    cluster -> cluster.getSpec().getFilterRefs());
        };
    }

    @VisibleForTesting
    static SecondaryToPrimaryMapper<KafkaProxyIngress> ingressSecondaryToPrimaryMapper(EventSourceContext<VirtualKafkaCluster> context) {
        return ingress -> {
            if (!ResourcesUtil.isStatusFresh(ingress)) {
                logIgnoredEvent(ingress);
                return Set.of();
            }
            return ResourcesUtil.findReferrersMulti(context,
                    ingress,
                    VirtualKafkaCluster.class,
                    cluster -> cluster.getSpec().getIngresses().stream().map(Ingresses::getIngressRef).toList());
        };
    }

    @VisibleForTesting
    static SecondaryToPrimaryMapper<KafkaService> kafkaServiceSecondaryToPrimaryMapper(EventSourceContext<VirtualKafkaCluster> context) {
        return service -> {
            if (!ResourcesUtil.isStatusFresh(service)) {
                logIgnoredEvent(service);
                return Set.of();
            }
            return ResourcesUtil.findReferrers(context,
                    service,
                    VirtualKafkaCluster.class,
                    cluster -> Optional.of(cluster.getSpec().getTargetKafkaServiceRef()));
        };
    }

    private static void logIgnoredEvent(HasMetadata hasMetadata) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Ignoring event from {} with stale status: {}", HasMetadata.getKind(hasMetadata.getClass()), ResourcesUtil.toLocalRef(hasMetadata));
        }
    }

    @Override
    public ErrorStatusUpdateControl<VirtualKafkaCluster> updateErrorStatus(VirtualKafkaCluster cluster, Context<VirtualKafkaCluster> context, Exception e) {
        // ResolvedRefs to UNKNOWN
        ErrorStatusUpdateControl<VirtualKafkaCluster> uc = ErrorStatusUpdateControl
                .patchStatus(statusFactory.newUnknownConditionStatusPatch(cluster, Condition.Type.ResolvedRefs, e));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{} with error {}", namespace(cluster), name(cluster), e.toString());
        }
        return uc;
    }

    private static Set<ResourceID> toConfigStateResourceName(VirtualKafkaCluster cluster) {
        return ResourcesUtil.localRefAsResourceId(cluster, cluster.getSpec().getProxyRef())
                .stream().map(x -> new ResourceID(x.getName() + CONFIG_STATE_CONFIG_MAP_SUFFIX, x.getNamespace().orElse(null))).collect(Collectors.toSet());
    }

    @Nullable
    private VirtualKafkaCluster checkTlsConfigConsistency(Context<VirtualKafkaCluster> context, VirtualKafkaCluster cluster, Ingresses clusterIngress) {
        var ingressName = Objects.requireNonNull(clusterIngress.getIngressRef().getName());
        var proxyIngressOpt = ResourcesUtil.findOnlyResourceNamed(ingressName, context.getSecondaryResources(KafkaProxyIngress.class));

        if (proxyIngressOpt.isPresent()) {
            KafkaProxyIngress proxyIngress = proxyIngressOpt.get();

            var proxyIngressDefinesTls = proxyIngressOpt
                    .map(KafkaProxyIngress::getSpec)
                    .map(KafkaProxyIngressSpec::getClusterIP)
                    .map(ClusterIP::getProtocol)
                    .orElse(ClusterIP.Protocol.TCP) == ClusterIP.Protocol.TLS;
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

}
