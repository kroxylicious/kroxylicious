/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;

import edu.umd.cs.findbugs.annotations.Nullable;

public record NetworkDefinition(@Nullable NettySettings management, @Nullable NettySettings proxy) {

    @JsonIgnore
    public NettySettings managementSettings() {
        return Objects.nonNull(management) ? management : new NettySettings(Optional.of(1), Optional.empty());
    }

    @JsonIgnore
    public NettySettings proxySettings() {
        return Objects.nonNull(proxy) ? proxy : new NettySettings(Optional.empty(), Optional.empty());
    }

    public static NetworkDefinition defaultNetworkDefinition() {
        return new NetworkDefinition(null, null);
    }
}
