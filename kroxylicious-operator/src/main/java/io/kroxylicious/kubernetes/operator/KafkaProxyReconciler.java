/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.javaoperatorsdk.operator.OperatorException;
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ContextInitializer;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Workflow;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.api.common.AnyLocalRef;
import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.FilterRef;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.tls.TrustAnchorRef;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterSpec;
import io.kroxylicious.kubernetes.operator.model.ProxyModel;
import io.kroxylicious.kubernetes.operator.model.ProxyModelBuilder;
import io.kroxylicious.kubernetes.operator.model.ingress.ProxyIngressModel;
import io.kroxylicious.kubernetes.operator.resolver.ProxyResolutionResult;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.admin.EndpointsConfiguration;
import io.kroxylicious.proxy.config.admin.ManagementConfiguration;
import io.kroxylicious.proxy.config.admin.PrometheusMetricsConfig;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.PlatformTrustProvider;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toLocalRef;

// @formatter:off
@Workflow(dependents = {
        @Dependent(
                name = KafkaProxyReconciler.CONFIG_STATE_DEP,
                type = ProxyConfigStateDependentResource.class
        ),
        @Dependent(
                name = KafkaProxyReconciler.CONFIG_DEP,
                reconcilePrecondition = ProxyConfigReconcilePrecondition.class,
                dependsOn = { KafkaProxyReconciler.CONFIG_STATE_DEP },
                type = ProxyConfigDependentResource.class
        ),
        @Dependent(
                name = KafkaProxyReconciler.DEPLOYMENT_DEP,
                type = ProxyDeploymentDependentResource.class,
                dependsOn = { KafkaProxyReconciler.CONFIG_DEP },
                readyPostcondition = DeploymentReadyCondition.class
        ),
        @Dependent(
                name = KafkaProxyReconciler.CLUSTERS_DEP,
                type = ClusterServiceDependentResource.class,
                dependsOn = { KafkaProxyReconciler.DEPLOYMENT_DEP }
        )
})
// @formatter:on
public class KafkaProxyReconciler implements
        Reconciler<KafkaProxy>,
        ContextInitializer<KafkaProxy> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProxyReconciler.class);

    public static final String CONFIG_STATE_DEP = "config-state";
    public static final String CONFIG_DEP = "config";
    public static final String DEPLOYMENT_DEP = "deployment";
    public static final String CLUSTERS_DEP = "clusters";

    private final Clock clock;
    private final SecureConfigInterpolator secureConfigInterpolator;
    private final KafkaProxyStatusFactory statusFactory;

    public KafkaProxyReconciler(Clock clock, SecureConfigInterpolator secureConfigInterpolator) {
        this.statusFactory = new KafkaProxyStatusFactory(Objects.requireNonNull(clock));
        this.clock = clock;
        this.secureConfigInterpolator = secureConfigInterpolator;
    }

    @Override
    public void initContext(
                            KafkaProxy proxy,
                            Context<KafkaProxy> context) {
        ProxyModelBuilder proxyModelBuilder = ProxyModelBuilder.contextBuilder();
        ProxyModel model = proxyModelBuilder.build(proxy, context);
        ConfigurationFragment<Configuration> fragment;
        try {
            fragment = generateProxyConfig(model);
        }
        catch (IllegalConfigurationException ice) {
            fragment = null;
        }

        KafkaProxyContext.init(context,
                new VirtualKafkaClusterStatusFactory(clock),
                model,
                fragment);
    }

    private ConfigurationFragment<Configuration> generateProxyConfig(ProxyModel model) {

        var allFilterDefinitions = buildFilterDefinitions(model);
        Map<String, ConfigurationFragment<NamedFilterDefinition>> namedDefinitions = allFilterDefinitions.stream()
                .collect(Collectors.toMap(
                        cf -> cf.fragment().name(),
                        Function.identity()));

        var virtualClusters = buildVirtualClusters(namedDefinitions.keySet(), model);

        List<NamedFilterDefinition> referencedFilters = virtualClusters.stream()
                .flatMap(vcFragment -> Optional.ofNullable(vcFragment.fragment().filters()).stream().flatMap(Collection::stream))
                .distinct()
                .map(filterName -> namedDefinitions.get(filterName).fragment()).toList();

        var allVolumes = Stream.concat(allFilterDefinitions.stream(), virtualClusters.stream())
                .flatMap(fd -> fd.volumes().stream())
                .collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(Volume::getName).reversed())));

        var allMounts = Stream.concat(allFilterDefinitions.stream(), virtualClusters.stream())
                .flatMap(fd -> fd.mounts().stream())
                .collect(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(VolumeMount::getMountPath).reversed())));

        // TODO add the volume & mounts for each KafkaService's spec.tls

        return new ConfigurationFragment<>(
                new Configuration(
                        new ManagementConfiguration(null, null, new EndpointsConfiguration(new PrometheusMetricsConfig())),
                        referencedFilters,
                        null, // no defaultFilters <= each of the virtualClusters specifies its own
                        virtualClusters.stream().map(ConfigurationFragment::fragment).toList(),
                        List.of(),
                        false,
                        // micrometer
                        Optional.empty()),
                allVolumes,
                allMounts);
    }

    private static List<ConfigurationFragment<VirtualCluster>> buildVirtualClusters(Set<String> successfullyBuiltFilterNames, ProxyModel model) {
        return model.clustersWithValidIngresses().stream()
                .filter(cluster -> Optional.ofNullable(cluster.getSpec().getFilterRefs()).stream().flatMap(Collection::stream).allMatch(
                        filters -> successfullyBuiltFilterNames.contains(filterDefinitionName(filters))))
                .map(cluster -> buildVirtualCluster(cluster, model.resolutionResult().kafkaServiceRef(cluster).orElseThrow(), model.ingressModel()))
                .toList();
    }

    private List<ConfigurationFragment<NamedFilterDefinition>> buildFilterDefinitions(ProxyModel model) {
        List<ConfigurationFragment<NamedFilterDefinition>> filterDefinitions = new ArrayList<>();
        Set<NamedFilterDefinition> uniqueValues = new HashSet<>();
        for (VirtualKafkaCluster cluster : model.clustersWithValidIngresses()) {
            for (ConfigurationFragment<NamedFilterDefinition> namedFilterDefinitionAndFiles : filterDefinitions(cluster, model.resolutionResult())) {
                if (uniqueValues.add(namedFilterDefinitionAndFiles.fragment())) {
                    filterDefinitions.add(namedFilterDefinitionAndFiles);
                }
            }
        }
        filterDefinitions.sort(Comparator.comparing(cf -> cf.fragment().name()));
        return filterDefinitions;
    }

    private static List<String> filterNamesForCluster(VirtualKafkaCluster cluster) {
        return Optional.ofNullable(cluster.getSpec().getFilterRefs())
                .orElse(List.of())
                .stream()
                .map(KafkaProxyReconciler::filterDefinitionName)
                .toList();
    }

    private static String filterDefinitionName(FilterRef filterCrRef) {
        return filterCrRef.getName() + "." + filterCrRef.getKind() + "." + filterCrRef.getGroup();
    }

    private List<ConfigurationFragment<NamedFilterDefinition>> filterDefinitions(VirtualKafkaCluster cluster,
                                                                                 ProxyResolutionResult resolutionResult) {

        return Optional.ofNullable(cluster.getSpec().getFilterRefs()).orElse(List.of()).stream().map(filterCrRef -> {

            String filterDefinitionName = filterDefinitionName(filterCrRef);

            var filterCr = resolutionResult.filter(filterCrRef).orElseThrow();
            var spec = filterCr.getSpec();
            String type = spec.getType();
            SecureConfigInterpolator.InterpolationResult interpolationResult = interpolateConfig(spec);
            return new ConfigurationFragment<>(new NamedFilterDefinition(filterDefinitionName, type, interpolationResult.config()),
                    interpolationResult.volumes(),
                    interpolationResult.mounts());

        }).toList();
    }

    private SecureConfigInterpolator.InterpolationResult interpolateConfig(KafkaProtocolFilterSpec spec) {
        Object configTemplate = Objects.requireNonNull(spec.getConfigTemplate(), "ConfigTemplate is required in the KafkaProtocolFilterSpec");
        return secureConfigInterpolator.interpolate(configTemplate);
    }

    private static ConfigurationFragment<VirtualCluster> buildVirtualCluster(VirtualKafkaCluster cluster,
                                                                             KafkaService kafkaServiceRef,
                                                                             ProxyIngressModel ingressModel) {

        ProxyIngressModel.VirtualClusterIngressModel virtualClusterIngressModel = ingressModel.clusterIngressModel(cluster).orElseThrow();

        return buildTargetCluster(kafkaServiceRef).map(targetCluster -> new VirtualCluster(
                ResourcesUtil.name(cluster),
                targetCluster,
                null,
                Optional.empty(),
                virtualClusterIngressModel.gateways(),
                false,
                false,
                filterNamesForCluster(cluster)));
    }

    private static ConfigurationFragment<TargetCluster> buildTargetCluster(KafkaService kafkaServiceRef) {
        String bootstrap = kafkaServiceRef.getSpec().getBootstrapServers();

        var tls = buildTargetClusterTls(kafkaServiceRef);
        TargetCluster targetCluster = new TargetCluster(bootstrap, tls.map(ConfigurationFragment::fragment));
        return new ConfigurationFragment<>(
                targetCluster,
                tls.map(ConfigurationFragment::volumes).orElse(Set.of()),
                tls.map(ConfigurationFragment::mounts).orElse(Set.of()));
    }

    private static Optional<ConfigurationFragment<Tls>> buildTargetClusterTls(KafkaService kafkaServiceRef) {
        return Optional.ofNullable(kafkaServiceRef.getSpec()).map(KafkaServiceSpec::getTls).map(serviceTls -> {
            Path volumeMountPath = Path.of("/opt/kroxylicious/target-cluster/client-certs");
            Optional<AnyLocalRef> tlsCertRef = Optional.ofNullable(serviceTls.getCertificateRef())
                    .filter(KafkaProxyReconciler::isSecret);

            var tlsCertVolume = tlsCertRef
                    .map(ref -> new VolumeBuilder()
                            .withName(ResourcesUtil.volumeName("", "secrets", ref.getName()))
                            .withNewSecret()
                            .withSecretName(ref.getName())
                            .endSecret()
                            .build());
            var tlsCertMount = tlsCertRef
                    .map(ref -> new VolumeMountBuilder()
                            .withName(ResourcesUtil.volumeName("", "secrets", ref.getName()))
                            .withMountPath(volumeMountPath.resolve(ref.getName()).toString())
                            .withReadOnly(true)
                            .build());
            var tlsKeyPath = tlsCertRef
                    .map(ref -> volumeMountPath.resolve(ref.getName()).resolve("tls.key"));
            var tlsCertPath = tlsCertRef
                    .map(ref -> volumeMountPath.resolve(ref.getName()).resolve("tls.crt"));

            Path trustedCertsPath = Path.of("/opt/kroxylicious/target-cluster/trusted-certs");
            var trustAnchorRef = Optional.ofNullable(serviceTls.getTrustAnchorRef())
                    .filter(KafkaProxyReconciler::isConfigMap);
            var trustAnchorVolume = trustAnchorRef.map(ref -> new VolumeBuilder()
                    .withName(ResourcesUtil.volumeName("", "configmaps", ref.getName()))
                    .withNewConfigMap()
                    .withName(ref.getName())
                    .endConfigMap()
                    .build());
            var trustAnchorMount = trustAnchorRef.map(ref -> new VolumeMountBuilder()
                    .withName(ResourcesUtil.volumeName("", "configmaps", ref.getName()))
                    .withMountPath(trustedCertsPath.resolve(ref.getName()).toString())
                    .withReadOnly(true)
                    .build());
            var trustAnchorPath = trustAnchorRef.map(ref -> trustedCertsPath.resolve(ref.getName()).resolve(ref.getKey()));

            var cipherSuites = Optional.ofNullable(serviceTls.getCipherSuites())
                    .map(cs -> new AllowDeny<>(cs.getAllowed(), new HashSet<>(cs.getDenied())))
                    .orElse(null);
            var protocols = Optional.ofNullable(serviceTls.getProtocols())
                    .map(proto -> new AllowDeny<>(proto.getAllowed(), new HashSet<>(proto.getDenied())))
                    .orElse(null);
            return new ConfigurationFragment<>(new Tls(
                    tlsKeyPath.isPresent() ? new KeyPair(tlsKeyPath.get().toString(), tlsCertPath.get().toString(), null) : null,
                    trustAnchorPath.isPresent() ? new TrustStore(trustAnchorPath.get().toString(), null, "PEM") : PlatformTrustProvider.INSTANCE,
                    cipherSuites,
                    protocols),
                    Stream.concat(tlsCertVolume.stream(), trustAnchorVolume.stream()).collect(Collectors.toSet()),
                    Stream.concat(tlsCertMount.stream(), trustAnchorMount.stream()).collect(Collectors.toSet()));
        });
    }

    private static boolean isSecret(AnyLocalRef ref) {
        return (ref.getKind() == null || ref.getKind().isEmpty() || "Secret".equals(ref.getKind()))
                && (ref.getGroup() == null || ref.getGroup().isEmpty());
    }

    private static boolean isConfigMap(TrustAnchorRef ref) {
        return (ref.getKind() == null || ref.getKind().isEmpty() || "ConfigMap".equals(ref.getKind()))
                && (ref.getGroup() == null || ref.getGroup().isEmpty());
    }

    /**
     * The happy path, where all the dependent resources expressed a desired
     */
    @Override
    public UpdateControl<KafkaProxy> reconcile(KafkaProxy primary,
                                               Context<KafkaProxy> context) {
        var uc = UpdateControl.patchStatus(statusFactory.newTrueConditionStatusPatch(primary, Condition.Type.Ready));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{}", namespace(primary), name(primary));
        }
        return uc;
    }

    /**
     * The unhappy path, where some dependent resource threw an exception
     */
    @Override
    public ErrorStatusUpdateControl<KafkaProxy> updateErrorStatus(KafkaProxy proxy,
                                                                  Context<KafkaProxy> context,
                                                                  Exception e) {
        if (e instanceof StaleReferentStatusException || e instanceof OperatorException && e.getCause() instanceof StaleReferentStatusException) {
            LOGGER.debug("Completed reconciliation of {}/{} with stale referent", namespace(proxy), name(proxy), e);
            return ErrorStatusUpdateControl.noStatusUpdate();
        }
        var uc = ErrorStatusUpdateControl.patchStatus(statusFactory.newUnknownConditionStatusPatch(proxy, Condition.Type.Ready, e));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Completed reconciliation of {}/{} with error {}", namespace(proxy), name(proxy), e.toString());
        }
        return uc;

    }

    @Override
    public List<EventSource<?, KafkaProxy>> prepareEventSources(EventSourceContext<KafkaProxy> context) {
        return List.of(
                buildFilterEventSource(context),
                buildVirtualKafkaClusterEventSource(context),
                buildKafkaServiceEventSource(context),
                buildKafkaProxyIngressEventSource(context));
    }

    private static InformerEventSource<VirtualKafkaCluster, KafkaProxy> buildVirtualKafkaClusterEventSource(EventSourceContext<KafkaProxy> context) {
        InformerEventSourceConfiguration<VirtualKafkaCluster> configuration = InformerEventSourceConfiguration.from(VirtualKafkaCluster.class, KafkaProxy.class)
                .withSecondaryToPrimaryMapper(clusterToProxyMapper(context))
                .withPrimaryToSecondaryMapper(proxyToClusterMapper(context))
                .build();
        return new InformerEventSource<>(configuration, context);
    }

    private static InformerEventSource<KafkaProxyIngress, KafkaProxy> buildKafkaProxyIngressEventSource(EventSourceContext<KafkaProxy> context) {
        InformerEventSourceConfiguration<KafkaProxyIngress> configuration = InformerEventSourceConfiguration.from(KafkaProxyIngress.class, KafkaProxy.class)
                .withSecondaryToPrimaryMapper(ingressToProxyMapper(context))
                .withPrimaryToSecondaryMapper(proxyToIngressMapper(context))
                .build();
        return new InformerEventSource<>(configuration, context);
    }

    private static InformerEventSource<KafkaService, KafkaProxy> buildKafkaServiceEventSource(EventSourceContext<KafkaProxy> context) {
        InformerEventSourceConfiguration<KafkaService> configuration = InformerEventSourceConfiguration.from(KafkaService.class, KafkaProxy.class)
                .withSecondaryToPrimaryMapper(kafkaServiceRefToProxyMapper(context))
                .withPrimaryToSecondaryMapper(proxyToKafkaServiceMapper(context))
                .build();
        return new InformerEventSource<>(configuration, context);
    }

    @VisibleForTesting
    static SecondaryToPrimaryMapper<KafkaService> kafkaServiceRefToProxyMapper(EventSourceContext<KafkaProxy> context) {
        return kafkaService -> {
            // we do not want to trigger reconciliation of any proxy if the ingress has not been reconciled
            if (!ResourcesUtil.isStatusFresh(kafkaService)) {
                LOGGER.debug("Ignoring event from KafkaService with stale status: {}", ResourcesUtil.toLocalRef(kafkaService));
                return Set.of();
            }
            // find all virtual clusters that reference this kafkaServiceRef

            Set<? extends LocalRef<KafkaProxy>> proxyRefs = ResourcesUtil.resourcesInSameNamespace(context, kafkaService, VirtualKafkaCluster.class)
                    .filter(vkc -> vkc.getSpec().getTargetKafkaServiceRef().equals(ResourcesUtil.toLocalRef(kafkaService)))
                    .map(VirtualKafkaCluster::getSpec)
                    .map(VirtualKafkaClusterSpec::getProxyRef)
                    .collect(Collectors.toSet());

            Set<ResourceID> proxyIds = ResourcesUtil.filteredResourceIdsInSameNamespace(context, kafkaService, KafkaProxy.class,
                    proxy -> proxyRefs.contains(toLocalRef(proxy)));
            LOGGER.debug("Event source KafkaService SecondaryToPrimaryMapper got {}", proxyIds);
            return proxyIds;
        };
    }

    /**
     * @param context context
     * @return mapper
     */
    @VisibleForTesting
    static PrimaryToSecondaryMapper<KafkaProxy> proxyToKafkaServiceMapper(EventSourceContext<KafkaProxy> context) {
        return primary -> {
            // Load all the virtual clusters for the KafkaProxy, then extract all the referenced KafkaService resource ids.
            Set<? extends LocalRef<KafkaService>> clusterRefs = ResourcesUtil.resourcesInSameNamespace(context, primary, VirtualKafkaCluster.class)
                    .filter(vkc -> {
                        LocalRef<KafkaProxy> proxyRef = vkc.getSpec().getProxyRef();
                        return proxyRef.equals(toLocalRef(primary));
                    })
                    .map(VirtualKafkaCluster::getSpec)
                    .map(VirtualKafkaClusterSpec::getTargetKafkaServiceRef)
                    .collect(Collectors.toSet());

            Set<ResourceID> kafkaServiceRefs = ResourcesUtil.filteredResourceIdsInSameNamespace(context, primary, KafkaService.class,
                    cluster -> clusterRefs.contains(toLocalRef(cluster)));
            LOGGER.debug("Event source KafkaService PrimaryToSecondaryMapper got {}", kafkaServiceRefs);
            return kafkaServiceRefs;
        };
    }

    private static InformerEventSource<KafkaProtocolFilter, KafkaProxy> buildFilterEventSource(EventSourceContext<KafkaProxy> context) {

        var configuration = InformerEventSourceConfiguration.from(KafkaProtocolFilter.class, KafkaProxy.class)
                .withSecondaryToPrimaryMapper(filterToProxy(context))
                .withPrimaryToSecondaryMapper(proxyToFilters(context))
                .build();

        return new InformerEventSource<>(configuration, context);
    }

    @VisibleForTesting
    static PrimaryToSecondaryMapper<KafkaProxy> proxyToFilters(EventSourceContext<KafkaProxy> context) {
        return (KafkaProxy proxy) -> {
            Set<ResourceID> filterReferences = ResourcesUtil.resourcesInSameNamespace(context, proxy, VirtualKafkaCluster.class)
                    .filter(clusterReferences(proxy))
                    .flatMap(cluster -> Optional.ofNullable(cluster.getSpec().getFilterRefs()).orElse(List.of()).stream())
                    .map(filter -> new ResourceID(filter.getName(), namespace(proxy)))
                    .collect(Collectors.toSet());
            LOGGER.debug("KafkaProxy {} has references to filters {}", ResourceID.fromResource(proxy), filterReferences);
            return filterReferences;
        };
    }

    @VisibleForTesting
    static PrimaryToSecondaryMapper<KafkaProxy> proxyToClusterMapper(EventSourceContext<KafkaProxy> context) {
        return proxy -> {
            Set<ResourceID> virtualClustersInProxyNamespace = ResourcesUtil.filteredResourceIdsInSameNamespace(context, proxy, VirtualKafkaCluster.class,
                    clusterReferences(proxy));
            LOGGER.debug("Event source VirtualKafkaCluster PrimaryToSecondaryMapper got {}", virtualClustersInProxyNamespace);
            return virtualClustersInProxyNamespace;
        };
    }

    @VisibleForTesting
    static SecondaryToPrimaryMapper<VirtualKafkaCluster> clusterToProxyMapper(EventSourceContext<KafkaProxy> context) {
        return cluster -> {
            // we do not want to trigger reconciliation of any proxy if the cluster has not been reconciled
            if (!ResourcesUtil.isStatusFresh(cluster)) {
                LOGGER.debug("Ignoring event from cluster with stale status: {}", ResourcesUtil.toLocalRef(cluster));
                return Set.of();
            }
            // we need to reconcile all proxies when a virtual kafka cluster changes in case the proxyRef is updated, we need to update
            // the previously referenced proxy too.
            Set<ResourceID> proxyIds = ResourcesUtil.filteredResourceIdsInSameNamespace(context, cluster, KafkaProxy.class, proxy -> true);
            LOGGER.debug("Event source VirtualKafkaCluster SecondaryToPrimaryMapper got {}", proxyIds);
            return proxyIds;
        };
    }

    @VisibleForTesting
    static SecondaryToPrimaryMapper<KafkaProxyIngress> ingressToProxyMapper(EventSourceContext<KafkaProxy> context) {
        return ingress -> {
            // we do not want to trigger reconciliation of any proxy if the ingress has not been reconciled
            if (!ResourcesUtil.isStatusFresh(ingress)) {
                LOGGER.debug("Ignoring event from ingress with stale status: {}", ResourcesUtil.toLocalRef(ingress));
                return Set.of();
            }
            // we need to reconcile all proxies when a kafka proxy ingress changes in case the proxyRef is updated, we need to update
            // the previously referenced proxy too.
            Set<ResourceID> proxyIds = ResourcesUtil.filteredResourceIdsInSameNamespace(context, ingress, KafkaProxy.class, proxy -> true);
            LOGGER.debug("Event source KafkaProxyIngress SecondaryToPrimaryMapper got {}", proxyIds);
            return proxyIds;
        };
    }

    @VisibleForTesting
    static PrimaryToSecondaryMapper<KafkaProxy> proxyToIngressMapper(EventSourceContext<KafkaProxy> context) {
        return primary -> {
            Set<ResourceID> ingressesInProxyNamespace = ResourcesUtil.filteredResourceIdsInSameNamespace(context, primary, KafkaProxyIngress.class,
                    ingressReferences(primary));
            LOGGER.debug("Event source KafkaProxyIngress PrimaryToSecondaryMapper got {}", ingressesInProxyNamespace);
            return ingressesInProxyNamespace;
        };
    }

    @VisibleForTesting
    static SecondaryToPrimaryMapper<KafkaProtocolFilter> filterToProxy(EventSourceContext<KafkaProxy> context) {
        return (KafkaProtocolFilter filter) -> {
            // we do not want to trigger reconciliation of any proxy if the filter has not been reconciled
            if (!ResourcesUtil.isStatusFresh(filter)) {
                LOGGER.debug("Ignoring event from filter with stale status: {}", ResourcesUtil.toLocalRef(filter));
                return Set.of();
            }
            // filters don't point to a proxy, but must be in the same namespace as the proxy/proxies which reference the,
            // so when a filter changes we reconcile all the proxies in the same namespace
            Set<ResourceID> proxiesInFilterNamespace = ResourcesUtil.filteredResourceIdsInSameNamespace(context, filter, KafkaProxy.class, proxy -> true);
            LOGGER.debug("Event source SecondaryToPrimaryMapper got {}", proxiesInFilterNamespace);
            return proxiesInFilterNamespace;
        };
    }

    private static Predicate<VirtualKafkaCluster> clusterReferences(KafkaProxy proxy) {
        return cluster -> cluster.getSpec().getProxyRef().getName().equals(name(proxy));
    }

    private static Predicate<KafkaProxyIngress> ingressReferences(KafkaProxy proxy) {
        return ingress -> ingress.getSpec().getProxyRef().getName().equals(name(proxy));
    }
}
// @formatter:off
