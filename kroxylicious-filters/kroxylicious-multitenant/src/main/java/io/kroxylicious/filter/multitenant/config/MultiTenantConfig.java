/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.multitenant.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for the Multi Tenant Filter.
 * @param prefixResourceNameSeparator separator used between the tenant prefix and the resource name
 * when forming the prefixed resource name. If null, {@link #DEFAULT_SEPARATOR} is used.
 */
public record MultiTenantConfig(String prefixResourceNameSeparator) {

    /**
     * Separator used between the tenant prefix and the resource name when no separator is configured.
     */
    public static final String DEFAULT_SEPARATOR = "-";

    /**
     * Constructs a MultiTenantConfig.
     * @param prefixResourceNameSeparator separator used between the tenant prefix and the resource name
     * when forming the prefixed resource name. If null, {@link #DEFAULT_SEPARATOR} is used.
     */
    public MultiTenantConfig(@Nullable @JsonProperty(required = false) String prefixResourceNameSeparator) {
        this.prefixResourceNameSeparator = Objects.requireNonNullElse(prefixResourceNameSeparator, DEFAULT_SEPARATOR);
    }
}
