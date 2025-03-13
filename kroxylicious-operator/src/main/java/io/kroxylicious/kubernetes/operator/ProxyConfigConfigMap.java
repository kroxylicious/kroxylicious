/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.ManagedWorkflowAndDependentResourceContext;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.common.AnyLocalRef;
import io.kroxylicious.kubernetes.api.common.FilterRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterSpec;
import io.kroxylicious.kubernetes.operator.model.ProxyModel;
import io.kroxylicious.kubernetes.operator.model.ProxyModelBuilder;
import io.kroxylicious.kubernetes.operator.model.ingress.ProxyIngressModel;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.admin.EndpointsConfiguration;
import io.kroxylicious.proxy.config.admin.ManagementConfiguration;
import io.kroxylicious.proxy.config.admin.PrometheusMetricsConfig;
import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

/**
 * Generates a Kube {@code ConfigMap} containing the proxy config YAML.
 */
@KubernetesDependent
public class ProxyConfigConfigMap
        extends CRUDKubernetesDependentResource<ConfigMap, KafkaProxy> {

    /**
     * The key of the {@code config.yaml} entry in the desired {@code Secret}.
     */
    public static final String CONFIG_YAML_KEY = "proxy-config.yaml";

    public static final Path SECURE_MOUNTS_PARENT_DIR = Path.of("/opt/kroxylicious/secure");
    public static final String SECURE_VOLUME_KEY = "secure-volumes";
    public static final String SECURE_VOLUME_MOUNT_KEY = "secure-volume-mounts";

    public static final String TARGET_TLS_VOLUME_KEY = "target-tls-volumes";
    public static final String TARGET_TLS_VOLUME_MOUNT_KEY = "target-tls-volume-mounts";

    private static final ObjectMapper OBJECT_MAPPER = ConfigParser.createObjectMapper();

    private static String toYaml(Object filterDefs) {
        try {
            return OBJECT_MAPPER.writeValueAsString(filterDefs);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public ProxyConfigConfigMap() {
        super(ConfigMap.class);
    }

    /**
     * @return The {@code metadata.name} of the desired ConfigMap {@code Secret}.
     */
    static String configMapName(KafkaProxy primary) {
        return ResourcesUtil.name(primary);
    }

    public static List<Volume> secureVolumes(ManagedWorkflowAndDependentResourceContext managedDependentResourceContext) {
        Set<Volume> volumes = managedDependentResourceContext.get(ProxyConfigConfigMap.SECURE_VOLUME_KEY, Set.class).orElse(Set.of());
        Set<Volume> targetTlsVolumes = managedDependentResourceContext.get(ProxyConfigConfigMap.TARGET_TLS_VOLUME_KEY, Set.class).orElse(Set.of());
        Map<String, List<Volume>> collect = Stream.concat(volumes.stream(), targetTlsVolumes.stream()).collect(Collectors.groupingBy(Volume::getName));
        collect.entrySet().stream()
                .filter(entry -> entry.getValue().size() > 1)
                .findFirst().ifPresent(entry -> {
                    throw new IllegalStateException("Two volumes with different definitions share the same name: " + entry.getKey());
                });

        return Stream.concat(volumes.stream(), targetTlsVolumes.stream()).toList();
    }

    public static List<VolumeMount> secureVolumeMounts(ManagedWorkflowAndDependentResourceContext managedDependentResourceContext) {
        Set<VolumeMount> mounts = managedDependentResourceContext.get(ProxyConfigConfigMap.SECURE_VOLUME_MOUNT_KEY, Set.class).orElse(Set.of());
        Set<VolumeMount> targetTlsMounts = managedDependentResourceContext.get(ProxyConfigConfigMap.TARGET_TLS_VOLUME_MOUNT_KEY, Set.class).orElse(Set.of());
        Map<String, List<VolumeMount>> collect = Stream.concat(mounts.stream(), targetTlsMounts.stream()).collect(Collectors.groupingBy(VolumeMount::getMountPath));
        collect.entrySet().stream()
                .filter(entry -> entry.getValue().size() > 1)
                .findFirst().ifPresent(entry -> {
                    throw new IllegalStateException("Two volumes with different definitions share the same name: " + entry.getKey());
                });
        return Stream.concat(mounts.stream(), targetTlsMounts.stream()).toList();
    }

    @Override
    protected ConfigMap desired(KafkaProxy primary,
                                Context<KafkaProxy> context) {
        // @formatter:off
        return new ConfigMapBuilder()
                .editOrNewMetadata()
                    .withName(configMapName(primary))
                    .withNamespace(namespace(primary))
                    .addToLabels(standardLabels(primary))
                    .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(primary)).endOwnerReference()
                .endMetadata()
                .withData(Map.of(CONFIG_YAML_KEY, generateProxyConfig(primary, context)))
                .build();
        // @formatter:on
    }

    String generateProxyConfig(KafkaProxy primary,
                               Context<KafkaProxy> context) {
        ProxyModelBuilder proxyModelBuilder = ProxyModelBuilder.contextBuilder(context);
        ProxyModel model = proxyModelBuilder.build(primary, context);
        List<NamedFilterDefinition> allFilterDefinitions = buildFilterDefinitions(context, model);
        Map<String, NamedFilterDefinition> namedDefinitions = allFilterDefinitions.stream().collect(Collectors.toMap(NamedFilterDefinition::name, f -> f));

        var virtualClusters = buildVirtualClusters(context, namedDefinitions.keySet(), model);

        List<NamedFilterDefinition> referencedFilters = virtualClusters.stream().flatMap(c -> Optional.ofNullable(c.filters()).stream().flatMap(Collection::stream))
                .distinct()
                .map(namedDefinitions::get).toList();

        Configuration configuration = new Configuration(
                new ManagementConfiguration(null, null, new EndpointsConfiguration(new PrometheusMetricsConfig())), referencedFilters,
                null, // no defaultFilters <= each of the virtualClusters specifies its own
                virtualClusters,
                List.of(), false,
                // micrometer
                Optional.empty());

        return toYaml(configuration);
    }

    @NonNull
    private static List<VirtualCluster> buildVirtualClusters(Context<KafkaProxy> context,
                                                             Set<String> successfullyBuiltFilterNames,
                                                             ProxyModel model) {
        return model.clustersWithValidIngresses().stream()
                .filter(cluster -> Optional.ofNullable(cluster.getSpec().getFilterRefs()).stream().flatMap(Collection::stream).allMatch(
                        filters -> successfullyBuiltFilterNames.contains(filterDefinitionName(filters))))
                .flatMap(cluster -> getVirtualCluster(context,
                        cluster, model.resolutionResult().kafkaServiceRef(cluster).orElseThrow(), model.ingressModel()).stream())
                .toList();
    }

    @NonNull

    private List<NamedFilterDefinition> buildFilterDefinitions(Context<KafkaProxy> context,
                                                               ProxyModel model) {
        List<NamedFilterDefinition> filterDefinitions = new ArrayList<>();
        Set<NamedFilterDefinition> uniqueValues = new HashSet<>();
        for (VirtualKafkaCluster cluster : model.clustersWithValidIngresses()) {
            try {
                for (NamedFilterDefinition namedFilterDefinition : filterDefinitions(context, cluster, model.resolutionResult())) {
                    if (uniqueValues.add(namedFilterDefinition)) {
                        filterDefinitions.add(namedFilterDefinition);
                    }
                }
            }
            catch (InvalidClusterException e) {
                SharedKafkaProxyContext.addClusterCondition(context, cluster, e.accepted());
            }
        }
        filterDefinitions.sort(Comparator.comparing(NamedFilterDefinition::name));
        return filterDefinitions;
    }

    private static List<String> filterNamesForCluster(VirtualKafkaCluster cluster) {
        return Optional.ofNullable(cluster.getSpec().getFilterRefs())
                .orElse(List.of())
                .stream()
                .map(ProxyConfigConfigMap::filterDefinitionName)
                .toList();
    }

    @NonNull
    private static String filterDefinitionName(FilterRef filterCrRef) {
        return filterCrRef.getName() + "." + filterCrRef.getKind() + "." + filterCrRef.getGroup();
    }

    @NonNull
    private List<NamedFilterDefinition> filterDefinitions(Context<KafkaProxy> context, VirtualKafkaCluster cluster, ResolutionResult resolutionResult)
            throws InvalidClusterException {

        return Optional.ofNullable(cluster.getSpec().getFilterRefs()).orElse(List.of()).stream().map(filterCrRef -> {

            String filterDefinitionName = filterDefinitionName(filterCrRef);

            var filterCr = resolutionResult.filter(filterCrRef).orElseThrow();
            var spec = filterCr.getSpec();
            String type = spec.getType();
            SecureConfigInterpolator.InterpolationResult interpolationResult = interpolateConfig(context, spec);
            ManagedWorkflowAndDependentResourceContext ctx = context.managedWorkflowAndDependentResourceContext();
            putOrMerged(ctx, SECURE_VOLUME_KEY, interpolationResult.volumes());
            putOrMerged(ctx, SECURE_VOLUME_MOUNT_KEY, interpolationResult.mounts());
            return new NamedFilterDefinition(filterDefinitionName, type, interpolationResult.config());

        }).toList();
    }

    private static <T> void putOrMerged(ManagedWorkflowAndDependentResourceContext ctx, String ctxKey, Set<T> set) {
        Optional<Set<T>> ctxVolumes = (Optional) ctx.get(ctxKey, Set.class);
        if (ctxVolumes.isPresent()) {
            ctxVolumes.get().addAll(set);
        }
        else {
            ctx.put(ctxKey, new LinkedHashSet<>(set));
        }
    }

    private SecureConfigInterpolator.InterpolationResult interpolateConfig(Context<KafkaProxy> context, KafkaProtocolFilterSpec spec) {
        SecureConfigInterpolator secureConfigInterpolator = KafkaProxyReconciler.secureConfigInterpolator(context);
        Object configTemplate = Objects.requireNonNull(spec.getConfigTemplate(), "ConfigTemplate is required in the KafkaProtocolFilterSpec");
        return secureConfigInterpolator.interpolate(configTemplate);
    }

    private static Optional<VirtualCluster> getVirtualCluster(Context<KafkaProxy> context,
                                                              VirtualKafkaCluster cluster,
                                                              KafkaService kafkaService,
                                                              ProxyIngressModel ingressModel) {
        try {
            ProxyIngressModel.VirtualClusterIngressModel virtualClusterIngressModel = ingressModel.clusterIngressModel(cluster).orElseThrow();
            String bootstrap = kafkaService.getSpec().getBootstrapServers();
            return Optional.of(new VirtualCluster(
                    ResourcesUtil.name(cluster),
                    new TargetCluster(bootstrap, buildTargetClusterTls(context, cluster, kafkaService)),
                    null,
                    Optional.empty(),
                    virtualClusterIngressModel.gateways(),
                    false, false,
                    filterNamesForCluster(cluster)));
        }
        catch (InvalidClusterException e) {
            SharedKafkaProxyContext.addClusterCondition(context, cluster, e.accepted());
        }
        return Optional.empty();
    }

    @NonNull
    private static Optional<Tls> buildTargetClusterTls(Context<KafkaProxy> context,
                                                       VirtualKafkaCluster cluster,
                                                       KafkaService kafkaService) {
        return Optional.ofNullable(kafkaService.getSpec())
                .map(KafkaServiceSpec::getTls)
                .map(io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls::getCertificateRef)
                .map(certRef -> ResourcesUtil.applyDefaults(certRef, "", "Secret"))
                .map(certRef -> {
                    return new Tls(new KeyPair(
                            virtualClusterTargetTlsKeyPath(context, cluster, certRef).containerPath().toString(),
                            virtualClusterTargetTlsCertPath(context, cluster, certRef).containerPath().toString(),
                            null),
                            null, null, null);
                });
    }

    private static ContainerFileReference virtualClusterTargetTlsKeyPath(Context<KafkaProxy> context,
                                                                         VirtualKafkaCluster cluster,
                                                                         AnyLocalRef certRef) {
        return virtualClusterTargetTlsPath(context, cluster, certRef, "tls.key");
    }

    private static ContainerFileReference virtualClusterTargetTlsCertPath(Context<KafkaProxy> context,
                                                                          VirtualKafkaCluster cluster,
                                                                          AnyLocalRef certRef) {
        return virtualClusterTargetTlsPath(context, cluster, certRef, "tls.crt");
    }

    private static ContainerFileReference virtualClusterTargetTlsPath(Context<KafkaProxy> context,
                                                                      VirtualKafkaCluster cluster,
                                                                      AnyLocalRef certRef,
                                                                      String file) {
        if ("Secret".equals(certRef.getKind())
                && certRef.getGroup().isEmpty()) {
            // Assume it's compatible with a Secret of `type: kubernetes.io/tls`
            String other = "vc-target-tls-" + ResourcesUtil.name(cluster);
            Path volumeMountPath = Path.of("/opt/kroxylicious/cluster")
                    .resolve(ResourcesUtil.name(cluster))
                    .resolve("target")
                    .resolve("tls-key");
            Volume volume = new VolumeBuilder()
                    .withName(other)
                    .withNewSecret()
                    .withSecretName(certRef.getName())
                    .addNewItem("tls.key", null, "tls.key")
                    .addNewItem("tls.crt", null, "tls.crt")
                    .endSecret()
                    .build();
            VolumeMount volumeMount = new VolumeMountBuilder()
                    .withName(other)
                    .withMountPath(volumeMountPath.toString())
                    .withReadOnly(true)
                    .build();
            var ctx = context.managedWorkflowAndDependentResourceContext();
            putOrMerged(ctx, TARGET_TLS_VOLUME_KEY, Set.of(volume));
            putOrMerged(ctx, TARGET_TLS_VOLUME_MOUNT_KEY, Set.of(volumeMount));

            return new ContainerFileReference(
                    volume,
                    volumeMount,
                    volumeMountPath.resolve(file));
        }
        else {
            throw new InvalidClusterException(ClusterCondition.refNotSupported(
                    ResourcesUtil.name(cluster),
                    certRef,
                    "spec.targetCluster.tls.certificateRef",
                    "kind: \"Secret\" in group \"\""));
        }
    }

}
