/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.AuthorizerService;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The FilterFactory (service) for the {@link AuthorizationFilter}.
 */
@Plugin(configType = AuthorizationConfig.class)
public class Authorization implements FilterFactory<AuthorizationConfig, Authorizer> {

    private @Nullable AuthorizerService authorizerService = null;

    @SuppressWarnings("unchecked")
    @Override
    public Authorizer initialize(FilterFactoryContext context,
                                 AuthorizationConfig authorizationConfig)
            throws PluginConfigurationException {
        var configuration = Plugins.requireConfig(this, authorizationConfig);
        this.authorizerService = context.pluginInstance(AuthorizerService.class, configuration.authorizer());
        authorizerService.initialize(configuration.authorizerConfig());

        Authorizer authorizer = authorizerService.build();
        authorizer.supportedResourceTypes().ifPresent(usedTypes -> {
            var unsupportedResourceTypes = new HashSet<>(usedTypes);
            unsupportedResourceTypes.removeAll(Set.of(TopicResource.class));
            if (!unsupportedResourceTypes.isEmpty()) {
                throw new PluginConfigurationException(("%s specifies access controls for resource types which cannot be enforced by this filter. "
                        + "The unsupported types are: %s.").formatted(
                                configuration.authorizer(),
                                unsupportedResourceTypes.stream().map(Class::getName).collect(Collectors.joining(", "))));
            }
        });
        return authorizer;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, @NonNull Authorizer authorizer) {
        return new AuthorizationFilter(authorizer);
    }

    @Override
    public void close(@NonNull Authorizer authorizer) {
        if (authorizerService != null) {
            authorizerService.close();
        }
    }
}
