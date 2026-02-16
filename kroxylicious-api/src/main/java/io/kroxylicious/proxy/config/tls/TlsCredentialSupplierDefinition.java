/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.tls.ServerTlsCredentialSupplierFactory;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for a TLS credential supplier that dynamically provides TLS credentials.
 * <p>
 * This follows the same pattern as filter configuration, with a type specifying the
 * {@link ServerTlsCredentialSupplierFactory} implementation and an optional config object
 * for supplier-specific configuration.
 * </p>
 *
 * @param type The type of TLS credential supplier (references a {@link ServerTlsCredentialSupplierFactory} implementation)
 * @param config Supplier-specific configuration (optional)
 */
public record TlsCredentialSupplierDefinition(
                                              @PluginImplName(ServerTlsCredentialSupplierFactory.class) @JsonProperty(required = true) String type,
                                              @Nullable @PluginImplConfig(implNameProperty = "type") Object config) {

    @JsonCreator
    public TlsCredentialSupplierDefinition {
        Objects.requireNonNull(type, "TLS credential supplier type must not be null");
    }
}
