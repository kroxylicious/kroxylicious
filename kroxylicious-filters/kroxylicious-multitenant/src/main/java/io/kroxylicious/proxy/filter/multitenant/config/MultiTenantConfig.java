/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.multitenant.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for the Multi Tenant Filter.
 * @param prefixResourceNameSeparator separator character used to form the prefixed resource name, if not null
 * a default is used.
 */
public record MultiTenantConfig(String prefixResourceNameSeparator) {

    public static final String DEFAULT_SEPARATOR = "-";

    public MultiTenantConfig(
            @JsonProperty(required = false)
            String prefixResourceNameSeparator
    ) {
        this.prefixResourceNameSeparator = Objects.requireNonNullElse(prefixResourceNameSeparator, DEFAULT_SEPARATOR);
    }
}
