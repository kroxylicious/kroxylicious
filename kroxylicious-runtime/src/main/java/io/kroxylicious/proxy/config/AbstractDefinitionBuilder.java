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

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Base builder for definitions of named plugin instances with an arbitrary configuration map.
 *
 * @param <D> the type of definition being built
 */
public abstract class AbstractDefinitionBuilder<D> {
    /** The mapper used to convert the configuration map into the definition's configuration type. */
    protected static final ObjectMapper mapper;
    static {
        mapper = ConfigParser.createObjectMapper();
    }

    private final String type;
    private @Nullable Map<String, Object> config;

    /**
     * Creates a builder for a definition of the given plugin type.
     *
     * @param type the name of the plugin type
     */
    protected AbstractDefinitionBuilder(String type) {
        Objects.requireNonNull(type);
        this.type = type;
    }

    /**
     * Adds all entries of the given map to the definition's configuration.
     *
     * @param config the configuration entries to add
     * @return this builder
     */
    public AbstractDefinitionBuilder<D> withConfig(Map<String, Object> config) {
        Objects.requireNonNull(config);
        if (this.config == null) {
            this.config = new LinkedHashMap<>();
        }
        this.config.putAll(config);
        return this;
    }

    /**
     * Adds a configuration entry to the definition's configuration.
     *
     * @param k1 the key
     * @param v1 the value
     * @return this builder
     */
    public AbstractDefinitionBuilder<D> withConfig(String k1, Object v1) {
        return withConfig(Map.of(k1, v1));
    }

    /**
     * Adds two configuration entries to the definition's configuration.
     *
     * @param k1 the first key
     * @param v1 the first value
     * @param k2 the second key
     * @param v2 the second value
     * @return this builder
     */
    public AbstractDefinitionBuilder<D> withConfig(String k1, Object v1, String k2, Object v2) {
        return withConfig(Map.of(k1, v1, k2, v2));
    }

    /**
     * Adds three configuration entries to the definition's configuration.
     *
     * @param k1 the first key
     * @param v1 the first value
     * @param k2 the second key
     * @param v2 the second value
     * @param k3 the third key
     * @param v3 the third value
     * @return this builder
     */
    public AbstractDefinitionBuilder<D> withConfig(String k1, Object v1, String k2, Object v2, String k3, Object v3) {
        return withConfig(Map.of(k1, v1, k2, v2, k3, v3));
    }

    /**
     * Adds four configuration entries to the definition's configuration.
     *
     * @param k1 the first key
     * @param v1 the first value
     * @param k2 the second key
     * @param v2 the second value
     * @param k3 the third key
     * @param v3 the third value
     * @param k4 the fourth key
     * @param v4 the fourth value
     * @return this builder
     */
    @SuppressWarnings("java:S107") // Methods should not have too many parameters - ignored as this convenience shouldn't blow any minds
    public AbstractDefinitionBuilder<D> withConfig(String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4) {
        return withConfig(Map.of(k1, v1, k2, v2, k3, v3, k4, v4));
    }

    /**
     * Builds the definition.
     *
     * @return the definition
     */
    public D build() {
        return buildInternal(type, config);
    }

    /**
     * Builds the definition from the accumulated type and configuration.
     *
     * @param type the name of the plugin type
     * @param config the configuration entries, or null if none were supplied
     * @return the definition
     */
    protected abstract D buildInternal(String type, @Nullable Map<String, Object> config);
}
