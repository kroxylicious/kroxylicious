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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
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

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;

import edu.umd.cs.findbugs.annotations.NonNull;


/**
 * Generates a Kube {@code Secret} containing the proxy config YAML.
 * We use a {@code Secret} (rather than a {@code ConfigMap})
 * because the config might contain sensitive settings like passwords
 */
@KubernetesDependent
public class ProxyConfigSecret
        extends CRUDKubernetesDependentResource<Secret, KafkaProxy> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyConfigSecret.class);

    private static final ObjectMapper OBJECT_MAPPER = ConfigParser.createObjectMapper();

    /**
     * The key of the {@code config.yaml} entry in the desired {@code Secret}.
     */
    public static final String CONFIG_YAML_KEY = "config.yaml";

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

        List<List<FilterDefinition>> filterDefinitionses = new ArrayList<>(clusters.size());
        for (Clusters cluster : clusters) {
            try {
                filterDefinitionses.add(filterDefinitions(cluster, context));
            }
            catch (InvalidClusterException e) {
                SharedKafkaProxyContext.addClusterError(context, cluster, e);
            }
        }
        if (filterDefinitionses.stream().map(filterDefs -> {
            try {
                return OBJECT_MAPPER.writeValueAsString(filterDefs);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).distinct().count() > 1) {
            throw new InvalidResourceException("Currently the proxy only supports a single, global, filter chain");
        }
        List<FilterDefinition> filterDefinitions = filterDefinitionses.isEmpty() ? List.of() : filterDefinitionses.get(0);

        var virtualClusters = clusters.stream()
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
                false);

        try {
            return OBJECT_MAPPER.writeValueAsString(configuration);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
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
            FilterKindDecl matchingKind = runtimeDecl.filterKindDecls().stream()
                    .filter(decl -> matches(decl, filterRef))
                    .findFirst()
                    .orElseThrow(() -> unknownFilterKind(cluster, filterRef));

            var filt = filterResourceFromRef(cluster, context, filterRef);
            return filterDefFromFilterResource(matchingKind.klass(), filt);
        }).toList();
    }

    @NonNull
    private static InvalidClusterException unknownFilterKind(Clusters cluster, Filters filterRef) {
        return new InvalidClusterException(cluster.getName(),
                "UnknownKind",
                "Filter in API group " + filterRef.getGroup()
                        + " with kind " + filterRef.getKind()
                        + " is not known to this operator");
    }

    @NonNull
    private static InvalidClusterException resourceNotFound(Clusters cluster, Filters filterRef) {
        return new InvalidClusterException(cluster.getName(),
                "ResourceNotFound",
                "Resource in API group " + filterRef.getGroup()
                        + " with kind " + filterRef.getKind()
                        + " and name " + filterRef.getName()
                        + " was not found by this operator");
    }

    private static boolean matches(FilterKindDecl decl, Filters filterRef) {
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
                                ClusterService.serviceName(cluster),
                                null,
                                null,
                                null)),
                Optional.empty(),
                false, false);
    }
}
