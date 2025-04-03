/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
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

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.ManagedWorkflowAndDependentResourceContext;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.FilterRef;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterSpec;
import io.kroxylicious.kubernetes.operator.model.ProxyModel;
import io.kroxylicious.kubernetes.operator.model.ingress.IngressConflictException;
import io.kroxylicious.kubernetes.operator.model.ingress.ProxyIngressModel;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.admin.EndpointsConfiguration;
import io.kroxylicious.proxy.config.admin.ManagementConfiguration;
import io.kroxylicious.proxy.config.admin.PrometheusMetricsConfig;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

/**
 * Generates a Kube {@code ConfigMap} containing the proxy config YAML.
 */
@KubernetesDependent
public class ProxyConfigConfigMap
        extends CRUDKubernetesDependentResource<ConfigMap, KafkaProxy> {

    public static String REASON_INVALID = "Invalid";

    /**
     * The key of the {@code config.yaml} entry in the desired {@code Secret}.
     */

    public static final String SECURE_VOLUME_KEY = "secure-volumes";
    public static final String SECURE_VOLUME_MOUNT_KEY = "secure-volume-mounts";

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
        if (volumes.stream().map(Volume::getName).distinct().count() != volumes.size()) {
            throw new IllegalStateException("Two volumes with different definitions share the same name");
        }
        return volumes.stream().toList();
    }

    public static List<VolumeMount> secureVolumeMounts(ManagedWorkflowAndDependentResourceContext managedDependentResourceContext) {
        Set<VolumeMount> mounts = managedDependentResourceContext.get(ProxyConfigConfigMap.SECURE_VOLUME_MOUNT_KEY, Set.class).orElse(Set.of());
        if (mounts.stream().map(VolumeMount::getMountPath).distinct().count() != mounts.size()) {
            throw new IllegalStateException("Two volume mounts with different definitions share the same mount path");
        }
        return mounts.stream().toList();
    }

    @Override
    protected ConfigMap desired(KafkaProxy primary,
                                Context<KafkaProxy> context) {
        Clock clock = KafkaProxyContext.proxyContext(context).clock();
        var data = new ProxyConfigData();
        data.setProxyConfiguration(generateProxyConfig(context));

        ProxyModel proxyModel = KafkaProxyContext.proxyContext(context).model();
        addResolvedRefsConditions(clock, proxyModel, data);
        addAcceptedConditions(clock, proxyModel, data);

        // @formatter:off
        return new ConfigMapBuilder()
                .editOrNewMetadata()
                    .withName(configMapName(primary))
                    .withNamespace(namespace(primary))
                    .addToLabels(standardLabels(primary))
                    .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(primary)).endOwnerReference()
                .endMetadata()
                .withData(data.build())
                .build();
        // @formatter:on
    }

    private static void addAcceptedConditions(Clock clock, ProxyModel proxyModel, ProxyConfigData data) {
        var model = proxyModel.ingressModel();
        for (ProxyIngressModel.VirtualClusterIngressModel virtualClusterIngressModel : model.clusters()) {
            VirtualKafkaCluster cluster = virtualClusterIngressModel.cluster();

            VirtualKafkaCluster patch;
            if (!virtualClusterIngressModel.ingressExceptions().isEmpty()) {
                IngressConflictException first = virtualClusterIngressModel.ingressExceptions().iterator().next();
                patch = Conditions.newFalseConditionStatusPatch(clock, cluster,
                        Condition.Type.Accepted, Condition.REASON_INVALID,
                        "Ingress(es) [" + first.getIngressName() + "] of cluster conflicts with another ingress");
            }
            else {
                patch = Conditions.newTrueConditionStatusPatch(clock, cluster,
                        Condition.Type.Accepted);
            }
            if (!data.hasStatusPatchForCluster(ResourcesUtil.name(cluster))) {
                data.addStatusPatchForCluster(ResourcesUtil.name(cluster), patch);
            }
        }
    }

    private static void addResolvedRefsConditions(Clock clock, ProxyModel proxyModel, ProxyConfigData data) {
        proxyModel.resolutionResult().clusterResults().stream()
                .filter(ResolutionResult.ClusterResolutionResult::isAnyDependencyUnresolved)
                .forEach(clusterResolutionResult -> {

                    Comparator<LocalRef<?>> comparator = Comparator.<LocalRef<?>, String> comparing(LocalRef::getGroup)
                            .thenComparing(LocalRef::getKind)
                            .thenComparing(LocalRef::getName);

                    LocalRef<?> firstUnresolvedDependency = clusterResolutionResult.unresolvedDependencySet().stream()
                            .sorted(comparator).findFirst()
                            .orElseThrow();
                    String message = String.format("Resource %s was not found.",
                            ResourcesUtil.namespacedSlug(firstUnresolvedDependency, clusterResolutionResult.cluster()));
                    VirtualKafkaCluster cluster = clusterResolutionResult.cluster();

                    data.addStatusPatchForCluster(
                            ResourcesUtil.name(cluster),
                            Conditions.newFalseConditionStatusPatch(clock, cluster,
                                    Condition.Type.ResolvedRefs, Condition.REASON_INVALID, message));
                });
    }

    Configuration generateProxyConfig(Context<KafkaProxy> context) {

        var model = KafkaProxyContext.proxyContext(context).model();

        List<NamedFilterDefinition> allFilterDefinitions = buildFilterDefinitions(context, model);
        Map<String, NamedFilterDefinition> namedDefinitions = allFilterDefinitions.stream().collect(Collectors.toMap(NamedFilterDefinition::name, f -> f));

        var virtualClusters = buildVirtualClusters(namedDefinitions.keySet(), model);

        List<NamedFilterDefinition> referencedFilters = virtualClusters.stream().flatMap(c -> Optional.ofNullable(c.filters()).stream().flatMap(Collection::stream))
                .distinct()
                .map(namedDefinitions::get).toList();

        return new Configuration(
                new ManagementConfiguration(null, null, new EndpointsConfiguration(new PrometheusMetricsConfig())), referencedFilters,
                null, // no defaultFilters <= each of the virtualClusters specifies its own
                virtualClusters,
                List.of(), false,
                // micrometer
                Optional.empty());
    }

    @NonNull
    private static List<VirtualCluster> buildVirtualClusters(Set<String> successfullyBuiltFilterNames, ProxyModel model) {
        return model.clustersWithValidIngresses().stream()
                .filter(cluster -> Optional.ofNullable(cluster.getSpec().getFilterRefs()).stream().flatMap(Collection::stream).allMatch(
                        filters -> successfullyBuiltFilterNames.contains(filterDefinitionName(filters))))
                .map(cluster -> getVirtualCluster(cluster, model.resolutionResult().kafkaServiceRef(cluster).orElseThrow(), model.ingressModel()))
                .toList();
    }

    @NonNull

    private List<NamedFilterDefinition> buildFilterDefinitions(Context<KafkaProxy> context,
                                                               ProxyModel model) {
        List<NamedFilterDefinition> filterDefinitions = new ArrayList<>();
        Set<NamedFilterDefinition> uniqueValues = new HashSet<>();
        for (VirtualKafkaCluster cluster : model.clustersWithValidIngresses()) {
            for (NamedFilterDefinition namedFilterDefinition : filterDefinitions(context, cluster, model.resolutionResult())) {
                if (uniqueValues.add(namedFilterDefinition)) {
                    filterDefinitions.add(namedFilterDefinition);
                }
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
    private List<NamedFilterDefinition> filterDefinitions(Context<KafkaProxy> context, VirtualKafkaCluster cluster, ResolutionResult resolutionResult) {

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
        SecureConfigInterpolator secureConfigInterpolator = KafkaProxyContext.proxyContext(context).secureConfigInterpolator();
        Object configTemplate = Objects.requireNonNull(spec.getConfigTemplate(), "ConfigTemplate is required in the KafkaProtocolFilterSpec");
        return secureConfigInterpolator.interpolate(configTemplate);
    }

    private static VirtualCluster getVirtualCluster(VirtualKafkaCluster cluster,
                                                    KafkaService kafkaServiceRef,
                                                    ProxyIngressModel ingressModel) {

        ProxyIngressModel.VirtualClusterIngressModel virtualClusterIngressModel = ingressModel.clusterIngressModel(cluster).orElseThrow();
        String bootstrap = kafkaServiceRef.getSpec().getBootstrapServers();
        return new VirtualCluster(
                ResourcesUtil.name(cluster), new TargetCluster(bootstrap, Optional.empty()),
                null,
                Optional.empty(),
                virtualClusterIngressModel.gateways(),
                false, false,
                filterNamesForCluster(cluster));
    }

}
