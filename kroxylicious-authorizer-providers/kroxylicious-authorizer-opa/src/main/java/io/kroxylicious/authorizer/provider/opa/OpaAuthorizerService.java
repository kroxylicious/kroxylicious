/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.opa;

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
        authorizer = OpaAuthorizer.builder().withOpaPolicy(Path.of(config.opaFile())).build();
    }

    @Override
    public Authorizer build() throws IllegalStateException {
        return Objects.requireNonNull(authorizer);
    }
}
