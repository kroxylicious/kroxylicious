/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.fabric8.kubernetes.api.model.ObjectMeta;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterStatus;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Encapsulates the reading and writing of the {@code data} section of the Proxy {@code ConfigMap}.
 */
public class ProxyConfigData {

    public static final String CONFIG_YAML_KEY = "proxy-config.yaml";
    public static final String CLUSTER_KEY_PREFIX = "cluster-";
    static final ObjectMapper CONFIG_OBJECT_MAPPER = ConfigParser.createObjectMapper()
            .registerModule(new JavaTimeModule());

    private static String toYaml(Object filterDefs) {
        try {
            // .stripTrailing() is required to standardise the trailing white space. Standardising ensures that we don't trigger Jackson/SnakeYaml magic for enabling
            // or disabling the chomp operator when appending this string to a YAML document. stripTrailing in preference to strip as its fractionally cheaper.
            return CONFIG_OBJECT_MAPPER.writeValueAsString(filterDefs).stripTrailing();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @NonNull
    private static String clusterKey(String clusterName) {
        return CLUSTER_KEY_PREFIX + clusterName;
    }

    private final Map<String, String> data;

    public ProxyConfigData() {
        this(new LinkedHashMap<>());
    }

    public ProxyConfigData(Map<String, String> data) {
        this.data = data;
    }

    public void setProxyConfiguration(Configuration proxyConfiguration) {
        data.put(CONFIG_YAML_KEY, toYaml(proxyConfiguration));
    }

    public String getProxyConfiguration() {
        return data.get(CONFIG_YAML_KEY);
    }

    record VirtualKafkaClusterPatch(ObjectMeta metadata,
                                    VirtualKafkaClusterStatus status) {
        // Use a dedicated class for JSON serialization because if we use a VKC itself
        // we get some extra fields which we don't really need
        // (like kind, apigroup and one for io.fabric8.kubernetes.client.CustomResource#getCRDName())
        public VirtualKafkaCluster toResource() {
            return new VirtualKafkaClusterBuilder().withMetadata(metadata()).withStatus(status()).build();
        }
    }

    public ProxyConfigData addStatusPatchForCluster(String clusterName, VirtualKafkaCluster patch) {
        data.put(clusterKey(clusterName), toYaml(new VirtualKafkaClusterPatch(patch.getMetadata(), patch.getStatus())));
        return this;
    }

    public boolean hasStatusPatchForCluster(String clusterName) {
        return data.containsKey(clusterKey(clusterName));
    }

    public Optional<VirtualKafkaCluster> getStatusPatchForCluster(String clusterName) {
        var str = data.get(clusterKey(clusterName));
        if (str == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(CONFIG_OBJECT_MAPPER.readValue(str, VirtualKafkaClusterPatch.class))
                    .map(VirtualKafkaClusterPatch::toResource);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    Map<String, String> build() {
        return data;
    }

}
