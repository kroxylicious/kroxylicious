/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.managed.ManagedDependentResourceContext;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaClusterRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Filters;
import io.kroxylicious.kubernetes.operator.ingress.IngressAllocator;
import io.kroxylicious.kubernetes.operator.ingress.ProxyIngressModel;
import io.kroxylicious.kubernetes.operator.resolver.DependencyResolver;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.admin.AdminHttpConfiguration;
import io.kroxylicious.proxy.config.admin.EndpointsConfiguration;
import io.kroxylicious.proxy.config.admin.PrometheusMetricsConfig;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

/**
 * Generates a Kube {@code Secret} containing the proxy config YAML.
 * We use a {@code Secret} (rather than a {@code ConfigMap})
 * because the config might contain sensitive settings like passwords
 */
@KubernetesDependent
public class ProxyConfigSecret
        extends CRUDKubernetesDependentResource<Secret, KafkaProxy> {

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

    public ProxyConfigSecret() {
        super(Secret.class);
        var providerMap = Map.<String, SecureConfigProvider> of(
                "secret", MountedResourceConfigProvider.SECRET_PROVIDER,
                "configmap", MountedResourceConfigProvider.CONFIGMAP_PROVIDER);
        secureConfigInterpolator = new SecureConfigInterpolator("/opt/kroxylicious/secure", providerMap);
    }

    /**
     * @return The {@code metadata.name} of the desired Secret {@code Secret}.
     */
    static String secretName(KafkaProxy primary) {
        return name(primary);
    }

    public static List<Volume> secureVolumes(ManagedDependentResourceContext managedDependentResourceContext) {
        Set<Volume> volumes = managedDependentResourceContext.get(ProxyConfigSecret.SECURE_VOLUME_KEY, Set.class).orElse(Set.of());
        if (volumes.stream().map(Volume::getName).distinct().count() != volumes.size()) {
            throw new IllegalStateException("Two volumes with different definitions share the same name");
        }
        return volumes.stream().toList();
    }

    public static List<VolumeMount> secureVolumeMounts(ManagedDependentResourceContext managedDependentResourceContext) {
        Set<VolumeMount> mounts = managedDependentResourceContext.get(ProxyConfigSecret.SECURE_VOLUME_MOUNT_KEY, Set.class).orElse(Set.of());
        if (mounts.stream().map(VolumeMount::getMountPath).distinct().count() != mounts.size()) {
            throw new IllegalStateException("Two volume mounts with different definitions share the same mount path");
        }
        return mounts.stream().toList();
    }

    @Override
    protected Secret desired(KafkaProxy primary,
                             Context<KafkaProxy> context) {
        // @formatter:off
        return new SecretBuilder()
                .editOrNewMetadata()
                    .withName(secretName(primary))
                    .withNamespace(namespace(primary))
                    .addToLabels(standardLabels(primary))
                    .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(primary)).endOwnerReference()
                .endMetadata()
                .withStringData(Map.of(CONFIG_YAML_KEY, generateProxyConfig(primary, context)))
                .build();
        // @formatter:on
    }

    String generateProxyConfig(KafkaProxy primary,
                               Context<KafkaProxy> context) {
        ResolutionResult resolutionResult = DependencyResolver.deepResolve(context);
        List<VirtualKafkaCluster> virtualKafkaClusters = resolutionResult.fullyResolvedClustersInNameOrder();

        Set<KafkaProxyIngress> ingresses = resolutionResult.getIngresses();
        List<VirtualKafkaCluster> virtualKafkaClusters1 = resolutionResult.allClustersInNameOrder();

        ProxyIngressModel ingressModel = IngressAllocator.allocateProxyIngressModel(primary, virtualKafkaClusters1, ingresses, context);
        List<VirtualKafkaCluster> clustersWithValidIngresses = virtualKafkaClusters.stream()
                .filter(cluster -> ingressModel.clusterIngressModel(cluster).map(i -> i.ingressExceptions().isEmpty()).orElse(false)).toList();

        List<NamedFilterDefinition> allFilterDefinitions = buildFilterDefinitions(context, clustersWithValidIngresses, resolutionResult);
        Map<String, NamedFilterDefinition> namedDefinitions = allFilterDefinitions.stream().collect(Collectors.toMap(NamedFilterDefinition::name, f -> f));
        var virtualClusters = buildVirtualClusters(clustersWithValidIngresses, resolutionResult, ingressModel, namedDefinitions.keySet());
        List<NamedFilterDefinition> referencedFilters = virtualClusters.stream().flatMap(c -> Optional.ofNullable(c.filters()).stream().flatMap(Collection::stream))
                .distinct()
                .map(namedDefinitions::get).toList();

        Configuration configuration = new Configuration(
                new AdminHttpConfiguration(null, null, new EndpointsConfiguration(new PrometheusMetricsConfig())), referencedFilters,
                null, // no defaultFilters <= each of the virtualClusters specifies its own
                virtualClusters,
                List.of(), false,
                // micrometer
                Optional.empty());

        return toYaml(configuration);
    }

    @NonNull
    private static List<VirtualCluster> buildVirtualClusters(List<VirtualKafkaCluster> clusters,
                                                             ResolutionResult resolutionResult,
                                                             ProxyIngressModel ingressModel,
                                                             Set<String> builtFilters) {
        return clusters.stream()
                .filter(cluster -> Optional.ofNullable(cluster.getSpec().getFilters()).stream().flatMap(Collection::stream).allMatch(
                        o -> builtFilters.contains(filterDefinitionName(o))))
                .map(cluster -> getVirtualCluster(cluster, resolutionResult.kafkaClusterRefFor(cluster).orElseThrow(), ingressModel))
                .toList();
    }

    @NonNull

    private List<NamedFilterDefinition> buildFilterDefinitions(Context<KafkaProxy> context, List<VirtualKafkaCluster> clusters, ResolutionResult resolutionResult) {
        List<NamedFilterDefinition> filterDefinitions = new ArrayList<>();
        Set<NamedFilterDefinition> uniqueValues = new HashSet<>();
        for (VirtualKafkaCluster cluster1 : clusters) {
            try {
                for (NamedFilterDefinition namedFilterDefinition : filterDefinitions(context, cluster1, resolutionResult)) {
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
                .map(ProxyConfigSecret::filterDefinitionName)
                .toList();
    }

    @NonNull
    private static String filterDefinitionName(Filters filterCrRef) {
        return filterCrRef.getName() + "." + filterCrRef.getKind() + "." + filterCrRef.getGroup();
    }

    @NonNull
    private List<NamedFilterDefinition> filterDefinitions(Context<KafkaProxy> context, VirtualKafkaCluster cluster, ResolutionResult resolutionResult)
            throws InvalidClusterException {

        return Optional.ofNullable(cluster.getSpec().getFilters()).orElse(List.of()).stream().map(filterCrRef -> {

            String filterDefinitionName = filterDefinitionName(filterCrRef);

            var filterCr = filterResourceFromRef(cluster, filterCrRef, resolutionResult);
            if (filterCr.getAdditionalProperties().get("spec") instanceof Map<?, ?> spec) {
                String type = (String) spec.get("type");
                SecureConfigInterpolator.InterpolationResult interpolationResult = interpolateConfig(spec);
                var ctx = context.managedDependentResourceContext();
                putOrMerged(ctx, SECURE_VOLUME_KEY, interpolationResult.volumes());
                putOrMerged(ctx, SECURE_VOLUME_MOUNT_KEY, interpolationResult.mounts());
                return new NamedFilterDefinition(filterDefinitionName, type, interpolationResult.config());
            }
            else {
                throw new InvalidClusterException(ClusterCondition.filterInvalid(name(cluster), filterDefinitionName, "`spec` was not an `object`."));
            }

        }).toList();
    }

    private static <T> void putOrMerged(ManagedDependentResourceContext ctx, String ctxKey, Set<T> set) {
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
    private static InvalidClusterException filterResourceNotFound(VirtualKafkaCluster cluster, String filterRefName) {
        return new InvalidClusterException(ClusterCondition.filterNotFound(name(cluster), filterRefName));
    }

    /**
     * Look up a Filter CR from the group, kind and name given in the cluster.
     */
    @NonNull
    private static GenericKubernetesResource filterResourceFromRef(VirtualKafkaCluster cluster,
                                                                   Filters filterRef,
                                                                   ResolutionResult resolutionResult)
            throws InvalidClusterException {
        return resolutionResult.filters().stream()
                .filter(filterResource -> {
                    String apiVersion = filterResource.getApiVersion();
                    var filterResourceGroup = apiVersion.substring(0, apiVersion.indexOf("/"));
                    return filterResourceGroup.equals(filterRef.getGroup())
                            && filterResource.getKind().equals(filterRef.getKind())
                            && name(filterResource).equals(filterRef.getName());
                })
                .findFirst()
                .orElseThrow(() -> filterResourceNotFound(cluster, filterRef.getName()));
    }

    private static VirtualCluster getVirtualCluster(VirtualKafkaCluster cluster,
                                                    KafkaClusterRef kafkaClusterRef,
                                                    ProxyIngressModel ingressModel) {

        ProxyIngressModel.VirtualClusterIngressModel virtualClusterIngressModel = ingressModel.clusterIngressModel(cluster).orElseThrow();
        String bootstrap = kafkaClusterRef.getSpec().getBootstrapServers();
        return new VirtualCluster(
                name(cluster), new TargetCluster(bootstrap, Optional.empty()),
                null,
                Optional.empty(),
                virtualClusterIngressModel.gateways(),
                false, false,
                filterNamesForCluster(cluster));
    }

}
