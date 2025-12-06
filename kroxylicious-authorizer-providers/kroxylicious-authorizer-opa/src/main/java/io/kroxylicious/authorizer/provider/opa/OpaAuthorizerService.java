/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.opa;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Objects;

import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.AuthorizerService;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.Nullable;

@Plugin(configType = OpaAuthorizerConfig.class)
public class OpaAuthorizerService implements AuthorizerService<OpaAuthorizerConfig> {

    private @Nullable OpaAuthorizer authorizer;

    @Override
    public void initialize(@Nullable OpaAuthorizerConfig config1) {
        var config = Plugins.requireConfig(this, config1);
        var builder = OpaAuthorizer.builder().withOpaPolicy(Path.of(config.opaFile()));

        if (config.dataFile() != null) {
            try {
                var dataJson = java.nio.file.Files.readString(Path.of(config.dataFile()));
                builder.withData(dataJson);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to load OPA data from file: " + config.dataFile(), e);
            }
        }

        authorizer = builder.build();
    }

    @Override
    public Authorizer build() throws IllegalStateException {
        return Objects.requireNonNull(authorizer);
    }

}
