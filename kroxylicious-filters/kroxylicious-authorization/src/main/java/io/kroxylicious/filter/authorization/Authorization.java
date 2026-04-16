/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.AuthorizerService;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.Plugins;
import io.kroxylicious.proxy.plugin.ResolvedPluginRegistry;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The FilterFactory (service) for the {@link AuthorizationFilter}.
 */
@Plugin(configType = AuthorizationConfig.class)
@Plugin(configVersion = "v1alpha1", configType = AuthorizationConfigV1.class)
public class Authorization implements FilterFactory<Object, Authorizer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Authorization.class);

    private @Nullable AuthorizerService<?> authorizerService = null;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Authorizer initialize(FilterFactoryContext context,
                                 Object config)
            throws PluginConfigurationException {
        LOGGER.atWarn().log("Authorization is an experimental Filter not yet recommended for production environments");
        var configuration = Plugins.requireConfig(this, config);
        if (configuration instanceof AuthorizationConfig legacy) {
            return initializeLegacy(context, legacy);
        }
        else if (configuration instanceof AuthorizationConfigV1 v1) {
            return initializeV1(context, v1);
        }
        throw new PluginConfigurationException("Unsupported config type: " + configuration.getClass().getName());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Authorizer initializeLegacy(FilterFactoryContext context,
                                        AuthorizationConfig configuration) {
        this.authorizerService = context.pluginInstance(AuthorizerService.class, configuration.authorizer());
        ((AuthorizerService) authorizerService).initialize(configuration.authorizerConfig());
        return validateAndBuild(configuration.authorizer());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Authorizer initializeV1(FilterFactoryContext context,
                                    AuthorizationConfigV1 configuration) {
        ResolvedPluginRegistry registry = context.resolvedPluginRegistry()
                .orElseThrow(() -> new PluginConfigurationException(
                        "v1alpha1 config requires a ResolvedPluginRegistry but none is available"));
        this.authorizerService = registry.pluginInstance(AuthorizerService.class, configuration.authorizer());
        Object authorizerConfig = registry.pluginConfig(AuthorizerService.class.getName(), configuration.authorizer());
        ((AuthorizerService) authorizerService).initialize(authorizerConfig);
        return validateAndBuild(configuration.authorizer());
    }

    private Authorizer validateAndBuild(String authorizerName) {
        Authorizer authorizer = authorizerService.build();
        authorizer.supportedResourceTypes().ifPresent(usedTypes -> {
            var unsupportedResourceTypes = new HashSet<>(usedTypes);
            unsupportedResourceTypes.removeAll(Set.of(TopicResource.class,
                    TransactionalIdResource.class,
                    GroupResource.class));
            if (!unsupportedResourceTypes.isEmpty()) {
                throw new PluginConfigurationException(("%s specifies access controls for resource types which cannot be enforced by this filter. "
                        + "The unsupported types are: %s.").formatted(
                                authorizerName,
                                unsupportedResourceTypes.stream().map(Class::getName).collect(Collectors.joining(", "))));
            }
        });
        return authorizer;
    }

    @Override
    @SuppressWarnings("java:S2638") // Tightening UnknownNullness
    public Filter createFilter(FilterFactoryContext context, @NonNull Authorizer authorizer) {
        return new AuthorizationFilter(authorizer);
    }

    @Override
    @SuppressWarnings("java:S2638") // Tightening UnknownNullness
    public void close(@NonNull Authorizer authorizer) {
        if (authorizerService != null) {
            authorizerService.close();
        }
    }
}
