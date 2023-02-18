/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.util.ArrayList;
import java.util.HashMap;
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

    public record AddressManager(String type, @JsonInclude(NON_EMPTY) Map<String, Object> config) {
    }

    public record AdminHttp(Endpoints endpoints) {
    }

    public record MicrometerConfig(@JsonInclude(NON_EMPTY) List<String> binders,
                                   @JsonInclude(NON_NULL) String configurationHookClass,
                                   @JsonInclude(NON_EMPTY) Map<String, String> commonTags) {
        MicrometerConfig withTag(String key, String value){
            Map<String, String> tags = commonTags != null ? new HashMap<>(commonTags) : new HashMap<>();
            tags.put(key, value);
            return new MicrometerConfig(this.binders, this.configurationHookClass, tags);
        }

        public MicrometerConfig withConfigHook(String className) {
            return new MicrometerConfig(this.binders, className, this.commonTags);
        }

        public MicrometerConfig withBinder(String binder) {
            List<String> binders = this.binders != null ? new ArrayList<>(this.binders) : new ArrayList<>();
            binders.add(binder);
            return new MicrometerConfig(binders, this.configurationHookClass, this.commonTags);
        }
    }

    public record Endpoints(@JsonGetter("prometheus") @JsonInclude(NON_NULL) Map<String, String> prometheusEndpointConfig) {
    }

    private Proxy proxy;
    private final List<Filter> filters = new ArrayList<>();
    private AddressManager addressManager;

    @JsonInclude(NON_NULL)
    private AdminHttp adminHttp = null;

    @JsonInclude(NON_NULL)
    private MicrometerConfig micrometer = null;

    public KroxyConfigBuilder(String proxyAddress) {
        proxy = new Proxy(proxyAddress, null, null);
    }

    public KroxyConfigBuilder withMicrometerBinder(String binder) {
        MicrometerConfig original = currentOrNewMicrometerConfig();
        micrometer = original.withBinder(binder);
        return this;
    }

    public KroxyConfigBuilder withMicrometerConfigHook(String className) {
        MicrometerConfig original = currentOrNewMicrometerConfig();
        micrometer = original.withConfigHook(className);
        return this;
    }

    public KroxyConfigBuilder withMicrometerCommonTag(String key, String value) {
        MicrometerConfig original = currentOrNewMicrometerConfig();
        micrometer = original.withTag(key, value);
        return this;
    }

    private MicrometerConfig currentOrNewMicrometerConfig() {
        return micrometer != null ? micrometer : new MicrometerConfig(null, null, null);
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

    public KroxyConfigBuilder withAddressManager(String type, String configKey, Object configValue) {
        addressManager = new AddressManager(type, Map.of(configKey, configValue));
        return this;
    }

    public KroxyConfigBuilder withDefaultAddressMapper(String bootstrapServers) {
        return withAddressManager("FixedCluster", "bootstrap_servers", bootstrapServers);
    }

    public AddressManager getAddressManager() {
        return addressManager;
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

    public List<Filter> getFilters() {
        return filters;
    }

    public MicrometerConfig getMicrometer() {
        return micrometer;
    }
}
