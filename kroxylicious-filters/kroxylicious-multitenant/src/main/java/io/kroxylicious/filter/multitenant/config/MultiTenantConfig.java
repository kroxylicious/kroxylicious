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
 * @param prefixResourceNameSeparator separator character used to form the prefixed resource name, if not null
 * a default is used.
 * @param prefixResourceName optional custom prefix for resource names. If specified, it overrides the virtual
 * cluster name as the prefix. If not specified (null), the virtual cluster name is used.
 */
public record MultiTenantConfig(String prefixResourceNameSeparator, @Nullable String prefixResourceName) {

    public static final String DEFAULT_SEPARATOR = "-";

    public MultiTenantConfig(@Nullable @JsonProperty(required = false) String prefixResourceNameSeparator,
                             @JsonProperty(required = false) @Nullable String prefixResourceName) {
        this.prefixResourceNameSeparator = Objects.requireNonNullElse(prefixResourceNameSeparator, DEFAULT_SEPARATOR);
        this.prefixResourceName = prefixResourceName;
    }
}
