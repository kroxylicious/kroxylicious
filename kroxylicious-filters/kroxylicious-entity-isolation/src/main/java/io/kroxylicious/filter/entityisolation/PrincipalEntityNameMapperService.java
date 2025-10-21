/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

@Plugin(configType = PrincipalEntityNameMapperService.Config.class)
public class PrincipalEntityNameMapperService implements EntityNameMapperService<PrincipalEntityNameMapperService.Config> {
    private static final Config DEFAULT_CONFIG = new Config(User.class);

    @Nullable
    private Config config;

    @Override
    public void initialize(@Nullable Config config) {
        this.config = Optional.ofNullable(config)
                .filter(c -> c.principalType() != null)
                .orElse(DEFAULT_CONFIG);
    }

    @Override
    public EntityNameMapper build() {
        Objects.requireNonNull(config, "config is required");
        return new PrincipalEntityNameMapper(Objects.requireNonNull(config.principalType()));
    }

    @Nullable
    @VisibleForTesting
    Config getEffectiveConfig() {
        return config;
    }

    record Config(@Nullable @JsonProperty() Class<? extends Principal> principalType) {}
}
