/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;
import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

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

    public record MicrometerConfig(String type, @JsonInclude(NON_EMPTY) Map<String, Object> config) {
    }

    public record AdminHttp(Endpoints endpoints) {
    }

    public record Endpoints(@JsonGetter("prometheus") @JsonInclude(NON_NULL) Map<String, String> prometheusEndpointConfig) {
    }

    private Proxy proxy;
    private final Map<String, Cluster> clusters = new LinkedHashMap<>();
    private final List<Filter> filters = new ArrayList<>();

    @JsonInclude(NON_NULL)
    private AdminHttp adminHttp = null;

    @JsonInclude(NON_NULL)
    private List<MicrometerConfig> micrometer = null;

    public KroxyConfigBuilder(String proxyAddress) {
        proxy = new Proxy(proxyAddress, null, null);
    }

    public KroxyConfigBuilder withDefaultCluster(String bootstrapServers) {
        return withCluster("demo", bootstrapServers);
    }

    public KroxyConfigBuilder withMicrometerBinder(String binder) {
        MicrometerConfig config = getMicrometerConfigForType("StandardBinders");
        @SuppressWarnings("unchecked")
        List<String> binderNames = (List<String>) config.config.computeIfAbsent("binderNames", s -> new ArrayList<String>());
        binderNames.add(binder);
        return this;
    }

    public KroxyConfigBuilder withMicrometerCommonTag(String key, String value) {
        MicrometerConfig config = getMicrometerConfigForType("CommonTags");
        @SuppressWarnings("unchecked")
        Map<String, String> commonTags = (Map<String, String>) config.config.computeIfAbsent("commonTags", s -> new HashMap<String, String>());
        commonTags.put(key, value);
        return this;
    }

    private MicrometerConfig getMicrometerConfigForType(String type) {
        Optional<MicrometerConfig> standardBinders = getOrCreateMicrometer().stream().filter(micrometerConfig -> micrometerConfig.type.equals(type)).findFirst();
        MicrometerConfig config;
        if (standardBinders.isPresent()) {
            config = standardBinders.get();
        }
        else {
            config = new MicrometerConfig(type, new HashMap<>());
            micrometer.add(config);
        }
        return config;
    }

    private List<MicrometerConfig> getOrCreateMicrometer() {
        if (micrometer == null) {
            micrometer = new ArrayList<>();
        }
        return micrometer;
    }

    public KroxyConfigBuilder withCluster(String clusterName, String bootstrapServers) {
        clusters.put(clusterName, new Cluster(bootstrapServers));
        return this;
    }

    public KroxyConfigBuilder withKeyStoreConfig(String keystoreFile, String keyPassword) {
        String address = proxy == null ? null : proxy.address;
        proxy = new Proxy(address, keystoreFile, keyPassword);
        return this;
    }

    public KroxyConfigBuilder withPrometheusEndpoint() {
        adminHttp = new AdminHttp(new Endpoints(Map.of()));
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

    public AdminHttp getAdminHttp() {
        return adminHttp;
    }

    public Map<String, Cluster> getClusters() {
        return clusters;
    }

    public List<Filter> getFilters() {
        return filters;
    }

    public List<MicrometerConfig> getMicrometer() {
        return micrometer;
    }
}
