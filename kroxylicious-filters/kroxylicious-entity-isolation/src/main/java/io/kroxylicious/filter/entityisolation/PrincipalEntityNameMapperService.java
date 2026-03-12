/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

@Plugin(configType = PrincipalEntityNameMapperService.Config.class)
public class PrincipalEntityNameMapperService implements EntityNameMapperService<PrincipalEntityNameMapperService.Config> {
    private static final Config DEFAULT_CONFIG = new Config(User.class, "-");

    @Nullable
    private Config config;

    @Override
    public void initialize(@Nullable Config c) {
        if (c == null || DEFAULT_CONFIG.equals(c)) {
            config = DEFAULT_CONFIG;
        }
        else {
            config = new Config(c.principalType() == null ? DEFAULT_CONFIG.principalType() : c.principalType(),
                    c.separator() == null ? DEFAULT_CONFIG.separator() : c.separator());
        }
    }

    @Override
    public EntityNameMapper build() {
        Objects.requireNonNull(config, "config is required");
        return new PrincipalEntityNameMapper(Objects.requireNonNull(config.principalType()),
                Objects.requireNonNull(config.separator()));
    }

    @Nullable
    @VisibleForTesting
    Config getEffectiveConfig() {
        return config;
    }

    /**
     * Configuration for the {@link PrincipalEntityNameMapperService}
     *
     * @param principalType the type of principal that will be prepended to the resource name to isolate the entity.
     * @param separator the separator character
     */
    record Config(@Nullable @JsonProperty() Class<? extends Principal> principalType,
                  @Nullable @JsonProperty String separator) {}
}
