/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyspec.Clusters;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyspec.clusters.Filters;
import io.kroxylicious.kubernetes.operator.config.FilterApiDecl;
import io.kroxylicious.kubernetes.operator.config.RuntimeDecl;
import io.kroxylicious.proxy.config.ClusterNetworkAddressConfigProviderDefinition;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.admin.AdminHttpConfiguration;
import io.kroxylicious.proxy.config.admin.EndpointsConfiguration;
import io.kroxylicious.proxy.config.admin.PrometheusMetricsConfig;
import io.kroxylicious.proxy.internal.clusternetworkaddressconfigprovider.PortPerBrokerClusterNetworkAddressConfigProvider;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;

/**
 * Generates a Kube {@code Secret} containing the proxy config YAML.
 * We use a {@code Secret} (rather than a {@code ConfigMap})
 * because the config might contain sensitive settings like passwords
 */
@KubernetesDependent
public class ProxyConfigSecret
        extends CRUDKubernetesDependentResource<Secret, KafkaProxy> {

    private static final ObjectMapper OBJECT_MAPPER = ConfigParser.createObjectMapper();

    private static String toYaml(Object filterDefs) {
        try {
            return OBJECT_MAPPER.writeValueAsString(filterDefs);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * The key of the {@code config.yaml} entry in the desired {@code Secret}.
     */
    public static final String CONFIG_YAML_KEY = "proxy-config.yaml";

    public ProxyConfigSecret() {
        super(Secret.class);
    }

    /**
     * @return The {@code metadata.name} of the desired Secret {@code Secret}.
     */
    static String secretName(KafkaProxy primary) {
        return primary.getMetadata().getName();
    }

    @Override
    protected Secret desired(KafkaProxy primary,
                             Context<KafkaProxy> context) {
        // @formatter:off
        return new SecretBuilder()
                .editOrNewMetadata()
                    .withName(secretName(primary))
                    .withNamespace(primary.getMetadata().getNamespace())
                    .addToLabels(standardLabels(primary))
                    .addNewOwnerReferenceLike(ResourcesUtil.ownerReferenceTo(primary)).endOwnerReference()
                .endMetadata()
                .withStringData(Map.of(CONFIG_YAML_KEY, generateProxyConfig(primary, context)))
                .build();
        // @formatter:on
    }

    String generateProxyConfig(KafkaProxy primary,
                               Context<KafkaProxy> context) {

        List<Clusters> clusters = ResourcesUtil.distinctClusters(primary);
        // TODO handle dupes here

        List<List<FilterDefinition>> filterDefinitionses = new ArrayList<>(clusters.size());
        for (Clusters cluster : clusters) {
            try {
                filterDefinitionses.add(filterDefinitions(cluster, context));
            }
            catch (InvalidClusterException e) {
                SharedKafkaProxyContext.addClusterCondition(context, cluster, e.accepted());
            }
        }
        if (filterDefinitionses.stream().map(ProxyConfigSecret::toYaml).distinct().count() > 1) {
            throw new InvalidResourceException("Currently the proxy only supports a single, global, filter chain");
        }
        List<FilterDefinition> filterDefinitions = filterDefinitionses.isEmpty() ? List.of() : filterDefinitionses.get(0);

        var virtualClusters = clusters.stream()
                .filter(cluster -> !SharedKafkaProxyContext.isBroken(context, cluster))
                .collect(Collectors.toMap(
                        Clusters::getName,
                        cluster -> getVirtualCluster(primary, cluster),
                        (v1, v2) -> v1, // Dupes handled below
                        LinkedHashMap::new));

        Configuration configuration = new Configuration(
                new AdminHttpConfiguration(null, null, new EndpointsConfiguration(new PrometheusMetricsConfig())),
                virtualClusters,
                filterDefinitions,
                List.of(),
                false,
                Optional.empty());

        return toYaml(configuration);
    }

    @NonNull
    private static List<FilterDefinition> filterDefinitions(Clusters cluster, Context<KafkaProxy> context)
            throws InvalidClusterException {
        List<Filters> filters = cluster.getFilters();
        if (filters == null) {
            return List.of();
        }
        return filters.stream().map(filterRef -> {
            RuntimeDecl runtimeDecl = SharedKafkaProxyContext.runtimeDecl(context);
            FilterApiDecl matchingFilterApi = runtimeDecl.filterApis().stream()
                    .filter(decl -> matches(decl, filterRef))
                    .findFirst()
                    .orElseThrow(() -> unknownFilterKind(cluster, filterRef));

            var filt = filterResourceFromRef(cluster, context, filterRef);
            if ("filter.kroxylicious.io".equals(matchingFilterApi.group())
                    && "Filter".equals(matchingFilterApi.kind())) {
                Map<String, Object> spec = (Map<String, Object>) filt.getAdditionalProperties().get("spec");
                String type = (String) spec.get("type");
                Object config = spec.get("config");
                return new FilterDefinition(type, config);
            }
            else {
                return filterDefFromFilterResource(matchingFilterApi.className(), filt);
            }
        }).toList();
    }

    @NonNull
    private static InvalidClusterException unknownFilterKind(Clusters cluster, Filters filterRef) {
        return new InvalidClusterException(ClusterCondition.filterKindNotKnown(
                cluster.getName(),
                filterRef.getName(),
                filterRef.getKind()));
    }

    @NonNull
    private static InvalidClusterException resourceNotFound(Clusters cluster, Filters filterRef) {
        return new InvalidClusterException(ClusterCondition.filterNotExists(cluster.getName(), filterRef.getName()));
    }

    private static boolean matches(FilterApiDecl decl, Filters filterRef) {
        return decl.kind().equals(filterRef.getKind())
                && decl.group().equals(filterRef.getGroup());
    }

    @NonNull
    private static FilterDefinition filterDefFromFilterResource(String filterClassName, GenericKubernetesResource filterResource) {
        Object spec = filterResource.getAdditionalProperties().get("spec");
        return new FilterDefinition(filterClassName, spec);
    }

    @NonNull
    private static GenericKubernetesResource filterResourceFromRef(Clusters cluster, Context<KafkaProxy> context, Filters filterRef) throws InvalidClusterException {
        return context.getSecondaryResources(GenericKubernetesResource.class).stream()
                .filter(filterResource -> {
                    String apiVersion = filterResource.getApiVersion();
                    var filterResourceGroup = apiVersion.substring(0, apiVersion.indexOf("/"));
                    return filterResourceGroup.equals(filterRef.getGroup())
                            && filterResource.getKind().equals(filterRef.getKind())
                            && filterResource.getMetadata().getName().equals(filterRef.getName());
                })
                .findFirst()
                .orElseThrow(() -> resourceNotFound(cluster, filterRef));
    }

    private static VirtualCluster getVirtualCluster(KafkaProxy primary, Clusters cluster) {
        var o = ResourcesUtil.distinctClusters(primary);
        var clusterNum = o.indexOf(cluster);
        return new VirtualCluster(
                new TargetCluster(cluster.getUpstream().getBootstrapServers(), Optional.empty()),
                new ClusterNetworkAddressConfigProviderDefinition(
                        "PortPerBrokerClusterNetworkAddressConfigProvider",
                        new PortPerBrokerClusterNetworkAddressConfigProvider.PortPerBrokerClusterNetworkAddressConfigProviderConfig(
                                new HostPort("localhost", 9292 + (100 * clusterNum)),
                                ClusterService.absoluteServiceHost(primary, cluster),
                                null,
                                null,
                                null)),
                Optional.empty(),
                false, false);
    }
}
