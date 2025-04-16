/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ProxyConfigStateData.CONFIG_OBJECT_MAPPER;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

/**
 * Generates a Kube {@code ConfigMap} containing the proxy config YAML.
 */
@KubernetesDependent
public class ProxyConfigDependentResource extends CRUDKubernetesDependentResource<ConfigMap, KafkaProxy> {
    public static final String PROXY_CONFIG_CONFIG_MAP_SUFFIX = "-proxy-config";

    public static final String CONFIG_YAML_KEY = "proxy-config.yaml";
    public static final String REASON_INVALID = "Invalid";

    /**
     * The key of the {@code config.yaml} entry in the desired {@code Secret}.
     */

    public ProxyConfigDependentResource() {
        super(ConfigMap.class);
    }

    /**
     * @return The {@code metadata.name} of the desired ConfigMap {@code Secret}.
     */
    static String configMapName(KafkaProxy primary) {
        return ResourcesUtil.name(primary) + PROXY_CONFIG_CONFIG_MAP_SUFFIX;
    }

    public static List<Volume> secureVolumes(Context<KafkaProxy> context) {
        Set<Volume> volumes = KafkaProxyContext.proxyContext(context).configuration().map(ConfigurationFragment::volumes).orElse(Set.of());
        if (volumes.stream().map(Volume::getName).distinct().count() != volumes.size()) {
            throw new IllegalStateException("Two volumes with different definitions share the same name");
        }
        return volumes.stream().toList();
    }

    public static List<VolumeMount> secureVolumeMounts(Context<KafkaProxy> context) {
        Set<VolumeMount> mounts = KafkaProxyContext.proxyContext(context).configuration().map(ConfigurationFragment::mounts).orElse(Set.of());
        if (mounts.stream().map(VolumeMount::getMountPath).distinct().count() != mounts.size()) {
            throw new IllegalStateException("Two volume mounts with different definitions share the same mount path");
        }
        return mounts.stream().toList();
    }

    @Override
    protected ConfigMap desired(KafkaProxy primary,
                                Context<KafkaProxy> context) {
        // the configuration object won't be present if isMet has returned false
        // this is the case if the dependant resource is to be removed.
        var data = KafkaProxyContext.proxyContext(context)
                .configuration()
                .map(ConfigurationFragment::fragment)
                .map(c -> Map.of(CONFIG_YAML_KEY, toYaml(c)))
                .orElse(Map.of());

        // @formatter:off
        return new ConfigMapBuilder()
                .editOrNewMetadata()
                    .withName(configMapName(primary))
                    .withNamespace(namespace(primary))
                    .addToLabels(standardLabels(primary))
                    .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(primary)).endOwnerReference()
                .endMetadata()
                .withData(data)
                .build();
        // @formatter:on
    }

    private static String toYaml(Object filterDefs) {
        try {
            return CONFIG_OBJECT_MAPPER.writeValueAsString(filterDefs).stripTrailing();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
