/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractDefinitionBuilder<D> {
    protected static final ObjectMapper mapper;

    static {
        mapper = ConfigParser.createObjectMapper();
    }

    private final String type;
    private Map<String, Object> config;

    protected AbstractDefinitionBuilder(String type) {
        Objects.requireNonNull(type);
        this.type = type;
    }

    public AbstractDefinitionBuilder<D> withConfig(Map<String, Object> config) {
        Objects.requireNonNull(config);
        if (this.config == null) {
            this.config = new LinkedHashMap<>();
        }
        this.config.putAll(config);
        return this;
    }

    public AbstractDefinitionBuilder<D> withConfig(String k1, Object v1) {
        return withConfig(Map.of(k1, v1));
    }

    public AbstractDefinitionBuilder<D> withConfig(String k1, Object v1, String k2, Object v2) {
        return withConfig(Map.of(k1, v1, k2, v2));
    }

    public AbstractDefinitionBuilder<D> withConfig(String k1, Object v1, String k2, Object v2, String k3, Object v3) {
        return withConfig(Map.of(k1, v1, k2, v2, k3, v3));
    }

    @SuppressWarnings("java:S107") // Methods should not have too many parameters - ignored as this convenience shouldn't blow any minds
    public AbstractDefinitionBuilder<D> withConfig(String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4) {
        return withConfig(Map.of(k1, v1, k2, v2, k3, v3, k4, v4));
    }

    public D build() {
        return buildInternal(type, config);
    }

    protected abstract D buildInternal(String type, Map<String, Object> config);
}
