/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.client.ResourceNotFoundException;
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
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.api.common.CertificateRef;
import io.kroxylicious.kubernetes.api.common.CipherSuites;
import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.common.Protocols;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilterSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls.TlsClientAuthentication;
import io.kroxylicious.kubernetes.operator.DeploymentReadyCondition;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.SecureConfigInterpolator;
import io.kroxylicious.kubernetes.operator.StaleReferentStatusException;
import io.kroxylicious.kubernetes.operator.model.ProxyModel;
import io.kroxylicious.kubernetes.operator.model.ProxyModelBuilder;
import io.kroxylicious.kubernetes.operator.model.networking.ClusterIngressNetworkingModel;
import io.kroxylicious.kubernetes.operator.model.networking.ProxyNetworkingModel;
import io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster.VirtualKafkaClusterStatusFactory;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NodeIdentificationStrategyFactory;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.SniHostIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.config.admin.EndpointsConfiguration;
import io.kroxylicious.proxy.config.admin.ManagementConfiguration;
import io.kroxylicious.proxy.config.admin.PrometheusMetricsConfig;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.KeyProvider;
import io.kroxylicious.proxy.config.tls.ServerOptions;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TlsClientAuth;
import io.kroxylicious.proxy.config.tls.TrustProvider;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

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
    private static final String SECRET_PLURAL = "secrets";
    private static final String CONFIGMAP_PLURAL = "configmaps";
    public static final Path MOUNTS_BASE_DIR = Path.of("/opt/kroxylicious/");
    private static final Path TARGET_CLUSTER_MOUNTS_BASE = MOUNTS_BASE_DIR.resolve("target-cluster");
    private static final Path CLIENT_CERTS_BASE_DIR = TARGET_CLUSTER_MOUNTS_BASE.resolve("client-certs");
    private static final Path CLIENT_TRUSTED_CERTS_BASE_DIR = TARGET_CLUSTER_MOUNTS_BASE.resolve("trusted-certs");
    private static final Path VIRTUAL_CLUSTER_MOUNTS_BASE = MOUNTS_BASE_DIR.resolve("virtual-cluster");
    private static final Path SERVER_CERTS_BASE_DIR = VIRTUAL_CLUSTER_MOUNTS_BASE.resolve("server-certs");
    private static final Path SERVER_TRUSTED_CERTS_BASE_DIR = VIRTUAL_CLUSTER_MOUNTS_BASE.resolve("trusted-certs");

    private final Clock clock;
    private final SecureConfigInterpolator secureConfigInterpolator;
    private final KafkaProxyStatusFactory statusFactory;

    private final Cache<String, Boolean> resourcesWithAbsentSpecs = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofHours(1))
            .maximumSize(100)
            .build();

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
        boolean hasClusters = !model.clustersWithValidNetworking().isEmpty();
        ConfigurationFragment<Configuration> fragment = null;
        if (hasClusters) {
            fragment = generateProxyConfig(model);
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

        return new ConfigurationFragment<>(
                new Configuration(
                        new ManagementConfiguration(null, null, new EndpointsConfiguration(new PrometheusMetricsConfig())),
                        referencedFilters,
                        null, // no defaultFilters <= each of the virtualClusters specifies its own
                        virtualClusters.stream().map(ConfigurationFragment::fragment).toList(),
                        List.of(),
                        false,
                        // micrometer
                        Optional.empty(),
                        null),
                allVolumes,
                allMounts);
    }

    private static List<ConfigurationFragment<VirtualCluster>> buildVirtualClusters(Set<String> successfullyBuiltFilterNames, ProxyModel model) {
        return model.clustersWithValidNetworking().stream()
                .filter(cluster -> cluster.filterResolutionResults().stream().allMatch(
                        filterResult -> successfullyBuiltFilterNames.contains(filterDefinitionName(filterResult.reference()))))
                .map(cluster -> buildVirtualCluster(cluster, model.networkingModel()))
                .toList();
    }

    private List<ConfigurationFragment<NamedFilterDefinition>> buildFilterDefinitions(ProxyModel model) {
        List<ConfigurationFragment<NamedFilterDefinition>> filterDefinitions = new ArrayList<>();
        Set<NamedFilterDefinition> uniqueValues = new HashSet<>();
        for (ClusterResolutionResult cluster : model.clustersWithValidNetworking()) {
            for (ConfigurationFragment<NamedFilterDefinition> namedFilterDefinitionAndFiles : filterDefinitions(cluster)) {
                if (uniqueValues.add(namedFilterDefinitionAndFiles.fragment())) {
                    filterDefinitions.add(namedFilterDefinitionAndFiles);
                }
            }
        }
        filterDefinitions.sort(Comparator.comparing(cf -> cf.fragment().name()));
        return filterDefinitions;
    }

    private static List<String> filterNamesForCluster(ClusterResolutionResult cluster) {
        return cluster.filterResolutionResults().stream()
                .map(ResolutionResult::reference)
                .map(KafkaProxyReconciler::filterDefinitionName)
                .toList();
    }

    private static String filterDefinitionName(LocalRef<?> filterCrRef) {
        return filterCrRef.getName() + "." + filterCrRef.getKind() + "." + filterCrRef.getGroup();
    }

    private List<ConfigurationFragment<NamedFilterDefinition>> filterDefinitions(ClusterResolutionResult cluster) {

        return cluster.filterResolutionResults().stream()
                .map(ResolutionResult::referentResource)
                .map(filterCr -> {
                    String filterDefinitionName = filterDefinitionName(ResourcesUtil.toLocalRef(filterCr));
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

    private static ConfigurationFragment<VirtualCluster> buildVirtualCluster(ClusterResolutionResult cluster,
                                                                             ProxyNetworkingModel ingressModel) {

        ProxyNetworkingModel.ClusterNetworkingModel clusterNetworkingModel = ingressModel.clusterIngressModel(cluster.cluster()).orElseThrow();
        var gatewayFragments = ConfigurationFragment.reduce(clusterNetworkingModel.clusterIngressNetworkingModelResults().stream()
                .map(ProxyNetworkingModel.ClusterIngressNetworkingModelResult::clusterIngressNetworkingModel)
                .map(KafkaProxyReconciler::buildVirtualClusterGateway).toList());

        KafkaService kafkaServiceRef = cluster.serviceResolutionResult().referentResource();
        var virtualClusterConfigurationFragment = gatewayFragments
                .flatMap(clusterCfs -> buildTargetCluster(kafkaServiceRef).map(targetCluster -> new VirtualCluster(
                        name(cluster.cluster()),
                        targetCluster,
                        clusterCfs,
                        false,
                        false,
                        filterNamesForCluster(cluster))));
        return ConfigurationFragment.combine(virtualClusterConfigurationFragment,
                gatewayFragments,
                (virtualCluster, gateways) -> virtualCluster);
    }

    public static ConfigurationFragment<VirtualClusterGateway> buildVirtualClusterGateway(ClusterIngressNetworkingModel gateway) {

        var tlsConfigFragment = gateway.downstreamTls()
                .map(KafkaProxyReconciler::buildTlsFragment)
                .orElse(ConfigurationFragment.empty());

        var volumes = tlsConfigFragment.volumes();
        var mounts = tlsConfigFragment.mounts();

        PortIdentifiesNodeIdentificationStrategy portIdentifiesNode = null;
        SniHostIdentifiesNodeIdentificationStrategy sniHostIdentifiesNode = null;
        NodeIdentificationStrategyFactory nodeIdentificationStrategy = gateway.nodeIdentificationStrategy();
        if (nodeIdentificationStrategy instanceof PortIdentifiesNodeIdentificationStrategy port) {
            portIdentifiesNode = port;
        }
        else if (nodeIdentificationStrategy instanceof SniHostIdentifiesNodeIdentificationStrategy sniHost) {
            sniHostIdentifiesNode = sniHost;
        }
        else {
            throw new IllegalStateException("Unsupported node identification strategy: " + nodeIdentificationStrategy);
        }
        return new ConfigurationFragment<>(new VirtualClusterGateway(name(gateway.ingress()),
                portIdentifiesNode,
                sniHostIdentifiesNode,
                tlsConfigFragment.fragment()), volumes, mounts);
    }

    private static ConfigurationFragment<Optional<Tls>> buildTlsFragment(io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls ingressTls) {
        return ConfigurationFragment.combine(
                buildKeyProvider(ingressTls.getCertificateRef(), SERVER_CERTS_BASE_DIR),
                buildTrustProvider(true, ingressTls.getTrustAnchorRef(), ingressTls.getTlsClientAuthentication(), SERVER_TRUSTED_CERTS_BASE_DIR),
                (keyProviderOpt, trustProvider) -> Optional.of(
                        new Tls(keyProviderOpt.orElse(null),
                                trustProvider.orElse(null),
                                buildCipherSuites(ingressTls.getCipherSuites()).orElse(null),
                                buildProtocols(ingressTls.getProtocols()).orElse(null))));
    }

    private static ConfigurationFragment<TargetCluster> buildTargetCluster(KafkaService kafkaServiceRef) {
        return buildTargetClusterTls(kafkaServiceRef)
                .map(tls -> {
                    if (kafkaServiceRef.getStatus().getBootstrapServers() != null) {
                        return new TargetCluster(kafkaServiceRef.getStatus().getBootstrapServers(), tls);
                    }
                    else {
                        throw new ResourceNotFoundException("Bootstrap server address not found");
                    }
                });
    }

    private static ConfigurationFragment<Optional<Tls>> buildTargetClusterTls(KafkaService kafkaServiceRef) {
        return Optional.ofNullable(kafkaServiceRef.getSpec())
                .map(KafkaServiceSpec::getTls)
                .map(serviceTls -> ConfigurationFragment.combine(
                        buildKeyProvider(serviceTls.getCertificateRef(), CLIENT_CERTS_BASE_DIR),
                        buildTrustProvider(false, serviceTls.getTrustAnchorRef(), null, CLIENT_TRUSTED_CERTS_BASE_DIR),
                        (keyProviderOpt, trustProvider) -> Optional.of(
                                new Tls(keyProviderOpt.orElse(null),
                                        trustProvider.orElse(null),
                                        buildCipherSuites(serviceTls.getCipherSuites()).orElse(null),
                                        buildProtocols(serviceTls.getProtocols()).orElse(null)))))
                .orElse(ConfigurationFragment.empty());
    }

    private static ConfigurationFragment<Optional<KeyProvider>> buildKeyProvider(@Nullable CertificateRef certificateRef, Path parent) {
        return Optional.ofNullable(certificateRef)
                .filter(ResourcesUtil::isSecret)
                .map(ref -> {
                    var volume = new VolumeBuilder()
                            .withName(ResourcesUtil.volumeName("", SECRET_PLURAL, ref.getName()))
                            .withNewSecret()
                            .withSecretName(ref.getName())
                            .endSecret()
                            .build();
                    Path mountPath = parent.resolve(ref.getName());
                    var mount = new VolumeMountBuilder()
                            .withName(ResourcesUtil.volumeName("", SECRET_PLURAL, ref.getName()))
                            .withMountPath(mountPath.toString())
                            .withReadOnly(true)
                            .build();
                    var keyPath = mountPath.resolve("tls.key");
                    var crtPath = mountPath.resolve("tls.crt");
                    return new ConfigurationFragment<>(
                            Optional.<KeyProvider> of(new KeyPair(keyPath.toString(), crtPath.toString(), null)),
                            Set.of(volume),
                            Set.of(mount));
                }).orElse(ConfigurationFragment.empty());
    }

    private static ConfigurationFragment<Optional<TrustProvider>> buildTrustProvider(boolean forServer,
                                                                                     @Nullable TrustAnchorRef trustAnchorRef,
                                                                                     @Nullable TlsClientAuthentication clientAuthentication,
                                                                                     Path parent) {
        return Optional.ofNullable(trustAnchorRef)
                .filter(tar -> ResourcesUtil.isConfigMap(tar.getRef()) || ResourcesUtil.isSecret(tar.getRef()))
                .map(tar -> {
                    var ref = tar.getRef();

                    // VirtualKafkaCluster and KafkaService CRD both default their trustAnchorRef.kind fields to `ConfigMap`

                    // if ref.getKind is null(), we assume that the resource is a ConfigMap
                    boolean isSecret = ref.getKind() != null && ResourcesUtil.isSecret(ref);

                    // Ensure volume name matches the resource type used
                    String volType = isSecret ? SECRET_PLURAL : CONFIGMAP_PLURAL;
                    String volName = ResourcesUtil.volumeName("", volType, ref.getName());

                    var volumeBuilder = new VolumeBuilder()
                            .withName(volName);

                    if (isSecret) {
                        volumeBuilder.withNewSecret().withSecretName(ref.getName()).endSecret();
                    }
                    else {
                        volumeBuilder.withNewConfigMap().withName(ref.getName()).endConfigMap();
                    }

                    Path mountPath = parent.resolve(ref.getName());

                    var mount = new VolumeMountBuilder()
                            .withName(volName)
                            .withMountPath(mountPath.toString())
                            .withReadOnly(true)
                            .build();
                    TrustProvider trustProvider = new TrustStore(
                            mountPath.resolve(tar.getKey()).toString(),
                            null,
                            "PEM",
                            forServer ? buildTlsServerOptions(clientAuthentication) : null);

                    return new ConfigurationFragment<>(Optional.of(trustProvider),
                            Set.of(volumeBuilder.build()),
                            Set.of(mount));
                }).orElse(ConfigurationFragment.empty());
    }

    /**
     * The happy path, where all the dependent resources expressed a desired
     */
    @Override
    public UpdateControl<KafkaProxy> reconcile(KafkaProxy primary,
                                               Context<KafkaProxy> context) {
        reportAbsentSpecIfNecessary(primary, LOGGER);
        Integer readyReplicas = context.getSecondaryResource(Deployment.class, DEPLOYMENT_DEP)
                .map(Deployment::getStatus)
                .map(DeploymentStatus::getReadyReplicas)
                .orElse(0);

        var uc = UpdateControl.patchStatus(statusFactory.newTrueConditionStatusPatch(primary, Condition.Type.Ready, readyReplicas));
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
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Completed reconciliation of {}/{} with stale referent", namespace(proxy), name(proxy), e);
            }
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

    @VisibleForTesting
    void reportAbsentSpecIfNecessary(KafkaProxy proxy, Logger log) {
        var resourceUid = Optional.of(proxy).map(HasMetadata::getMetadata).map(ObjectMeta::getUid);
        resourceUid.ifPresent(uid -> {
            if (proxy.getSpec() == null) {
                if (resourcesWithAbsentSpecs.asMap().putIfAbsent(uid, true) == null) {
                    log.warn("{} has no spec, please add an empty one. "
                            + " Support for spec-less KafkaProxy resources is deprecated and will be removed in a future release.", ResourcesUtil.namespacedSlug(proxy));
                }
            }
            else {
                resourcesWithAbsentSpecs.invalidate(uid);
            }
        });
    }

    private static Optional<AllowDeny<String>> buildProtocols(@Nullable Protocols protocols) {
        return Optional.ofNullable(protocols)
                .map(p -> new AllowDeny<>(p.getAllow(), new HashSet<>(p.getDeny())));
    }

    private static Optional<AllowDeny<String>> buildCipherSuites(@Nullable CipherSuites cipherSuites) {
        return Optional.ofNullable(cipherSuites)
                .map(c -> new AllowDeny<>(c.getAllow(), new HashSet<>(c.getDeny())));
    }

    private static ServerOptions buildTlsServerOptions(@Nullable TlsClientAuthentication clientAuthentication) {
        var knownNames = Arrays.stream(TlsClientAuth.values())
                .map(TlsClientAuth::name)
                .collect(Collectors.toSet());

        var clientAuth = Optional.ofNullable(clientAuthentication)
                .map(TlsClientAuthentication::getValue)
                .filter(knownNames::contains)
                .map(TlsClientAuth::valueOf)
                .orElse(TlsClientAuth.REQUIRED);
        return new ServerOptions(clientAuth);
    }

    private static InformerEventSource<VirtualKafkaCluster, KafkaProxy> buildVirtualKafkaClusterEventSource(EventSourceContext<KafkaProxy> context) {
        InformerEventSourceConfiguration<VirtualKafkaCluster> configuration = InformerEventSourceConfiguration.from(VirtualKafkaCluster.class, KafkaProxy.class)
                .withSecondaryToPrimaryMapper(new VirtualKafkaClusterSecondaryToKafkaProxyPrimaryMapper(context))
                .withPrimaryToSecondaryMapper(new KafkaProxyPrimaryToVirtualKafkaClusterSecondaryMapper(context))
                .build();
        return new InformerEventSource<>(configuration, context);
    }

    private static InformerEventSource<KafkaProxyIngress, KafkaProxy> buildKafkaProxyIngressEventSource(EventSourceContext<KafkaProxy> context) {
        InformerEventSourceConfiguration<KafkaProxyIngress> configuration = InformerEventSourceConfiguration.from(KafkaProxyIngress.class, KafkaProxy.class)
                .withSecondaryToPrimaryMapper(new KafkaProxyIngressSecondaryToKafkaProxyPrimaryMapper(context))
                .withPrimaryToSecondaryMapper(new KafkaProxyPrimaryToKafkaProxyIngressSecondaryMapper(context))
                .build();
        return new InformerEventSource<>(configuration, context);
    }

    private static InformerEventSource<KafkaService, KafkaProxy> buildKafkaServiceEventSource(EventSourceContext<KafkaProxy> context) {
        InformerEventSourceConfiguration<KafkaService> configuration = InformerEventSourceConfiguration.from(KafkaService.class, KafkaProxy.class)
                .withSecondaryToPrimaryMapper(new KafkaServiceSecondaryToKafkaProxyPrimaryMapper(context))
                .withPrimaryToSecondaryMapper(new KafkaProxyPrimaryToKafkaServiceSecondaryMapper(context))
                .build();
        return new InformerEventSource<>(configuration, context);
    }

    private static InformerEventSource<KafkaProtocolFilter, KafkaProxy> buildFilterEventSource(EventSourceContext<KafkaProxy> context) {

        var configuration = InformerEventSourceConfiguration.from(KafkaProtocolFilter.class, KafkaProxy.class)
                .withSecondaryToPrimaryMapper(new KafkaProtocolFilterSecondaryToKafkaProxyPrimaryMapper(context))
                .withPrimaryToSecondaryMapper(new KafkaProxyPrimaryToKafkaProtocolFilterSecondaryMapper(context))
                .build();

        return new InformerEventSource<>(configuration, context);
    }
}
