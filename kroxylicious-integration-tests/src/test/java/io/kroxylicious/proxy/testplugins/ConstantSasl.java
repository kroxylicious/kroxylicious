/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

import java.util.Objects;

import org.apache.kafka.common.protocol.ApiKeys;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.testplugins.ConstantSasl.Config;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A filter for testing purposes which simply calls
 * {@link io.kroxylicious.proxy.filter.FilterContext#clientSaslAuthenticationSuccess(String, String)} or
 * {@link io.kroxylicious.proxy.filter.FilterContext#clientSaslAuthenticationFailure(String, String, Exception)}
 * when it first observes a specific request type.
 *
 * This is used with the {@link ClientAuthAwareLawyerFilter} to test the API contract is supported by the runtime.
 */
@Plugin(configType = Config.class)
public class ConstantSasl implements FilterFactory<Config, Config> {

    public record Config(
            ApiKeys api,
            @Nullable String mechanism,
            @Nullable String authorizedId,
            @Nullable String exceptionClassName,
            @Nullable String exceptionMessage
    ) {
        public Config {
            if (exceptionClassName == null) {
                Objects.requireNonNull(mechanism);
                Objects.requireNonNull(authorizedId);
            }
        }

        Class<? extends Exception> exceptionClass() {
            Class<?> cls;
            try {
                cls = Class.forName(exceptionClassName());
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            if (!Exception.class.isAssignableFrom(cls)) {
                throw new RuntimeException(exceptionClassName() + " is not a subclass of " + Exception.class.getName());
            }
            return cls.asSubclass(Exception.class);
        }

        Exception newException(String msg) {
            try {
                var ctor = exceptionClass().getDeclaredConstructor(String.class);
                return ctor.newInstance(msg);
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Config initialize(FilterFactoryContext context, Config config) throws PluginConfigurationException {
        return config;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Config config) {
        return new ConstantSaslFilter(config);
    }
}
