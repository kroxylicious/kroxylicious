/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.*;

public class KroxyConfigBuilder {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory().disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));

    public record Proxy(String address,
                        @JsonInclude(NON_NULL) String keyStoreFile,
                        @JsonInclude(NON_NULL) String keyPassword) {
    }

    public record Cluster(@JsonGetter("bootstrap_servers") String bootstrapServers) {
    }

    public record Filter(String type, @JsonInclude(NON_EMPTY) Map<String, Object> config) {
    }

    private Proxy proxy;
    private final Map<String, Cluster> clusters = new LinkedHashMap<>();
    private final List<Filter> filters = new ArrayList<>();

    public KroxyConfigBuilder(String proxyAddress) {
        proxy = new Proxy(proxyAddress, null, null);
    }

    public KroxyConfigBuilder withDefaultCluster(String bootstrapServers) {
        clusters.put("demo", new Cluster(bootstrapServers));
        return this;
    }

    public KroxyConfigBuilder withKeyStoreConfig(String keystoreFile, String keyPassword) {
        proxy = new Proxy(proxy.address, keystoreFile, keyPassword);
        return this;
    }

    public KroxyConfigBuilder addFilter(String type) {
        filters.add(new Filter(type, null));
        return this;
    }

    public KroxyConfigBuilder addFilter(String type, String configKey, Object configValue) {
        filters.add(new Filter(type, Map.of(configKey, configValue)));
        return this;
    }

    public String build() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public Proxy getProxy() {
        return proxy;
    }

    public Map<String, Cluster> getClusters() {
        return clusters;
    }

    public List<Filter> getFilters() {
        return filters;
    }
}
