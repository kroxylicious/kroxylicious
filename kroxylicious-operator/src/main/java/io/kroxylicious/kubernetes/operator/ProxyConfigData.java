/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Encapsulates the reading and writing of the {@code data} section of the Proxy {@code ConfigMap}.
 */
public class ProxyConfigData {

    public static final String CONFIG_YAML_KEY = "proxy-config.yaml";
    public static final String CLUSTER_KEY_PREFIX = "cluster-";
    static final ObjectMapper OBJECT_MAPPER = ConfigParser.createObjectMapper();

    private static String toYaml(Object filterDefs) {
        try {
            return OBJECT_MAPPER.writeValueAsString(filterDefs);
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

    public ProxyConfigData addConditionsForCluster(String clusterName, List<Condition> conditions) {
        data.put(clusterKey(clusterName), toYaml(conditions));
        return this;
    }

    public boolean hasConditionsForCluster(String clusterName) {
        return data.containsKey(clusterKey(clusterName));
    }

    public @Nullable List<Condition> getConditionsForCluster(String clusterName) {
        var str = data.get(clusterKey(clusterName));
        if (str == null) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(str, new TypeReference<List<Condition>>() {
            });
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    Map<String, String> build() {
        return data;
    }

}
