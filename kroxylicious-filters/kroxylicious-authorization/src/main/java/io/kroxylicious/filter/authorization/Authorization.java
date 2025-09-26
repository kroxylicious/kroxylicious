/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import io.kroxylicious.authorizer.service.AuthorizerService;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

public class Authorization implements FilterFactory<AuthorizationConfig, Authorization.InitializationContext> {

    public record InitializationContext(
                                        AuthorizerService authorizer) {}

    @Override
    public InitializationContext initialize(FilterFactoryContext context, AuthorizationConfig configuration) throws PluginConfigurationException {
        var authorizerService = context.pluginInstance(AuthorizerService.class, configuration.authorizer());
        authorizerService.initialize(configuration.authorizerConfig());

        return new InitializationContext(authorizerService);
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, InitializationContext initializationData) {
        return new AuthorizationFilter(initializationData.authorizer().build(),
                null);
    }

    @Override
    public void close(InitializationContext initializationData) {
        initializationData.authorizer.close();
    }
}
