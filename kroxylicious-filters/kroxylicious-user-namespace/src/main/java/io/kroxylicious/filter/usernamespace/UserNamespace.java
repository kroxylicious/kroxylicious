/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.filter.usernamespace;

import java.util.Objects;
import java.util.Set;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

/**
 * A {@link FilterFactory} for {@link UserNamespaceFilter}.
 */
@Plugin(configType = UserNamespace.Config.class)
public class UserNamespace implements FilterFactory<UserNamespace.Config, UserNamespace.Config> {

    @Override
    public Config initialize(FilterFactoryContext context, Config config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public UserNamespaceFilter createFilter(FilterFactoryContext context, Config configuration) {
        return new UserNamespaceFilter(configuration);
    }

    public enum ResourceType {
        GROUP_ID,
        TRANSACTIONAL_ID
    }

    public record Config(Set<ResourceType> resourceTypes) {

        public Config {
            Objects.requireNonNull(resourceTypes);
        }
    }
}
