/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.ManagedWorkflowAndDependentResourceContext;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaClusterRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Filters;
import io.kroxylicious.kubernetes.operator.ingress.IngressAllocator;
import io.kroxylicious.kubernetes.operator.ingress.IngressConflictException;
import io.kroxylicious.kubernetes.operator.ingress.ProxyIngressModel;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.admin.EndpointsConfiguration;
import io.kroxylicious.proxy.config.admin.ManagementConfiguration;
import io.kroxylicious.proxy.config.admin.PrometheusMetricsConfig;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.clusterRefs;
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

    public static final String SECURE_VOLUME_KEY = "secure-volumes";
    public static final String SECURE_VOLUME_MOUNT_KEY = "secure-volume-mounts";

    private static final ObjectMapper OBJECT_MAPPER = ConfigParser.createObjectMapper();

    private static String toYaml(Object filterDefs) {
        try {
            return OBJECT_MAPPER.writeValueAsString(filterDefs);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private final SecureConfigInterpolator secureConfigInterpolator;

    public ProxyConfigConfigMap() {
        super(ConfigMap.class);
        var providerMap = Map.<String, SecureConfigProvider> of(
                "secret", MountedResourceConfigProvider.SECRET_PROVIDER,
                "configmap", MountedResourceConfigProvider.CONFIGMAP_PROVIDER);
        secureConfigInterpolator = new SecureConfigInterpolator("/opt/kroxylicious/secure", providerMap);
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

        List<VirtualKafkaCluster> virtualKafkaClusters = ResourcesUtil.clustersInNameOrder(context).toList();
        Set<KafkaProxyIngress> ingresses = context.getSecondaryResources(KafkaProxyIngress.class);
        ProxyIngressModel ingressModel = getProxyIngressModel(primary, context, virtualKafkaClusters, ingresses);

        Map<ResourceID, KafkaClusterRef> clusterRefs = clusterRefs(context);

        // TODO fix this double invocation of buildFilterDefinitions which is a workaround for https://github.com/kroxylicious/kroxylicious/issues/1916
        // first invocation rejects some virtual clusters with unresolved refs
        buildFilterDefinitions(context, virtualKafkaClusters);

        var virtualClusters = buildVirtualClusters(context, virtualKafkaClusters, clusterRefs, ingressModel);

        // second invocation excludes filters that belong to clusters broken in `buildVirtualClusters`
        List<NamedFilterDefinition> filterDefinitions = buildFilterDefinitions(context, virtualKafkaClusters);

        Configuration configuration = new Configuration(
                new ManagementConfiguration(null, null, new EndpointsConfiguration(new PrometheusMetricsConfig())), filterDefinitions,
                null, // no defaultFilters <= each of the virtualClusters specifies its own
                virtualClusters,
                List.of(), false,
                // micrometer
                Optional.empty());

        return toYaml(configuration);
    }

    private static @NonNull ProxyIngressModel getProxyIngressModel(KafkaProxy primary, Context<KafkaProxy> context, List<VirtualKafkaCluster> virtualKafkaClusters,
                                                                   Set<KafkaProxyIngress> ingresses) {
        ProxyIngressModel ingressModel = IngressAllocator.allocateProxyIngressModel(primary, virtualKafkaClusters, ingresses);
        for (ProxyIngressModel.VirtualClusterIngressModel virtualClusterIngressModel : ingressModel.clusters()) {
            Set<IngressConflictException> exceptions = virtualClusterIngressModel.ingressExceptions();
            if (!exceptions.isEmpty()) {
                VirtualKafkaCluster cluster = virtualClusterIngressModel.cluster();
                SharedKafkaProxyContext.addClusterCondition(context, cluster, ClusterCondition.ingressConflict(ResourcesUtil.name(cluster), exceptions));
            }
        }
        return ingressModel;
    }

    @NonNull
    private static List<VirtualCluster> buildVirtualClusters(Context<KafkaProxy> context,
                                                             List<VirtualKafkaCluster> clusters,
                                                             Map<ResourceID, KafkaClusterRef> clusterRefs,
                                                             ProxyIngressModel ingressModel) {
        Map<VirtualKafkaCluster, Optional<KafkaClusterRef>> clusterToRefMap = clusters.stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        cluster -> clusterTargetClusterResourceID(cluster).map(clusterRefs::get)));

        clusterToRefMap.entrySet().stream()
                .filter(e -> e.getValue().isEmpty())
                .forEach(e -> {
                    var cluster = e.getKey();
                    SharedKafkaProxyContext.addClusterCondition(context, cluster, targetClusterResourceNotFound(cluster).accepted());
                });

        return clusters.stream()
                .filter(cluster -> !SharedKafkaProxyContext.isBroken(context, cluster))
                .map(cluster -> getVirtualCluster(cluster, clusterToRefMap.get(cluster).get(), ingressModel))
                .toList();
    }

    @NonNull

    private List<NamedFilterDefinition> buildFilterDefinitions(Context<KafkaProxy> context, List<VirtualKafkaCluster> clusters) {
        List<NamedFilterDefinition> filterDefinitions = new ArrayList<>();
        Set<NamedFilterDefinition> uniqueValues = new HashSet<>();
        for (VirtualKafkaCluster cluster1 : clusters) {
            if (SharedKafkaProxyContext.isBroken(context, cluster1)) {
                continue;
            }
            try {
                for (NamedFilterDefinition namedFilterDefinition : filterDefinitions(context, cluster1)) {
                    if (uniqueValues.add(namedFilterDefinition)) {
                        filterDefinitions.add(namedFilterDefinition);
                    }
                }
            }
            catch (InvalidClusterException e) {
                SharedKafkaProxyContext.addClusterCondition(context, cluster1, e.accepted());
            }
        }
        filterDefinitions.sort(Comparator.comparing(NamedFilterDefinition::name));
        return filterDefinitions;
    }

    private static List<String> filterNamesForCluster(VirtualKafkaCluster cluster) {
        return Optional.ofNullable(cluster.getSpec().getFilters())
                .orElse(List.of())
                .stream()
                .map(ProxyConfigConfigMap::filterDefinitionName)
                .toList();
    }

    @NonNull
    private static String filterDefinitionName(Filters filterCrRef) {
        return filterCrRef.getName() + "." + filterCrRef.getKind() + "." + filterCrRef.getGroup();
    }

    @NonNull
    private List<NamedFilterDefinition> filterDefinitions(Context<KafkaProxy> context, VirtualKafkaCluster cluster)
            throws InvalidClusterException {

        return Optional.ofNullable(cluster.getSpec().getFilters()).orElse(List.of()).stream().map(filterCrRef -> {

            String filterDefinitionName = filterDefinitionName(filterCrRef);

            var filterCr = filterResourceFromRef(cluster, context, filterCrRef);
            if (filterCr.getAdditionalProperties().get("spec") instanceof Map<?, ?> spec) {
                String type = (String) spec.get("type");
                SecureConfigInterpolator.InterpolationResult interpolationResult = interpolateConfig(spec);
                ManagedWorkflowAndDependentResourceContext ctx = context.managedWorkflowAndDependentResourceContext();
                putOrMerged(ctx, SECURE_VOLUME_KEY, interpolationResult.volumes());
                putOrMerged(ctx, SECURE_VOLUME_MOUNT_KEY, interpolationResult.mounts());
                return new NamedFilterDefinition(filterDefinitionName, type, interpolationResult.config());
            }
            else {
                throw new InvalidClusterException(ClusterCondition.filterInvalid(ResourcesUtil.name(cluster), filterDefinitionName, "`spec` was not an `object`."));
            }

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

    private @NonNull SecureConfigInterpolator.InterpolationResult interpolateConfig(Map<?, ?> spec) {
        SecureConfigInterpolator.InterpolationResult result;
        Object configTemplate = spec.get("configTemplate");
        if (configTemplate != null) {
            result = secureConfigInterpolator.interpolate(configTemplate);
        }
        else {
            result = new SecureConfigInterpolator.InterpolationResult(spec.get("config"), Set.of(), Set.of());
        }
        return result;
    }

    @NonNull
    private static InvalidClusterException filterResourceNotFound(VirtualKafkaCluster cluster, Filters filterRef) {
        return new InvalidClusterException(ClusterCondition.filterNotFound(ResourcesUtil.name(cluster), filterRef.getName()));
    }

    @NonNull
    private static InvalidClusterException targetClusterResourceNotFound(VirtualKafkaCluster cluster) {
        return new InvalidClusterException(ClusterCondition.targetClusterRefNotFound(ResourcesUtil.name(cluster), cluster.getSpec().getTargetCluster()));
    }

    /**
     * Look up a Filter CR from the group, kind and name given in the cluster.
     */
    @NonNull
    private static GenericKubernetesResource filterResourceFromRef(VirtualKafkaCluster cluster, Context<KafkaProxy> context, Filters filterRef)
            throws InvalidClusterException {
        return context.getSecondaryResources(GenericKubernetesResource.class).stream()
                .filter(filterResource -> {
                    String apiVersion = filterResource.getApiVersion();
                    var filterResourceGroup = apiVersion.substring(0, apiVersion.indexOf("/"));
                    return filterResourceGroup.equals(filterRef.getGroup())
                            && filterResource.getKind().equals(filterRef.getKind())
                            && ResourcesUtil.name(filterResource).equals(filterRef.getName());
                })
                .findFirst()
                .orElseThrow(() -> filterResourceNotFound(cluster, filterRef));
    }

    private static VirtualCluster getVirtualCluster(VirtualKafkaCluster cluster,
                                                    KafkaClusterRef kafkaClusterRef,
                                                    ProxyIngressModel ingressModel) {

        ProxyIngressModel.VirtualClusterIngressModel virtualClusterIngressModel = ingressModel.clusterIngressModel(cluster).orElseThrow();
        String bootstrap = kafkaClusterRef.getSpec().getBootstrapServers();
        return new VirtualCluster(
                ResourcesUtil.name(cluster), new TargetCluster(bootstrap, Optional.empty()),
                null,
                Optional.empty(),
                virtualClusterIngressModel.gateways(),
                false, false,
                filterNamesForCluster(cluster));
    }

    private static Optional<ResourceID> clusterTargetClusterResourceID(VirtualKafkaCluster cluster) {
        return Optional.ofNullable(cluster.getSpec())
                .map(VirtualKafkaClusterSpec::getTargetCluster)
                .map(io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.TargetCluster::getClusterRef)
                .map(r -> new ResourceID(r.getName(), namespace(cluster)));
    }
}
