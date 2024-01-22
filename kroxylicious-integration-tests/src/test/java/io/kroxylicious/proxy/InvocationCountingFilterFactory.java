/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.RequestFilter;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

@Plugin(configType = InvocationCountingFilterFactory.Config.class)
public class InvocationCountingFilterFactory implements FilterFactory<InvocationCountingFilterFactory.Config, InvocationCountingFilterFactory.Config> {

    private static final Map<UUID, AtomicInteger> invocationTracker = new ConcurrentHashMap<>();

    @Override
    public Config initialize(FilterFactoryContext context, Config config) throws PluginConfigurationException {
        invocationTracker.computeIfAbsent(config.configInstanceId, uuid -> new AtomicInteger(0)).getAndIncrement();
        return config;
    }

    @NonNull
    @Override
    public Filter createFilter(FilterFactoryContext context, Config initializationData) {
        return (RequestFilter) (apiKey, header, request, requestFilterContext) -> requestFilterContext.forwardRequest(header, request);
    }

    public static void assertInvocationCount(UUID configId, int count) {
        assertThat(invocationTracker.get(configId)).hasValue(count);
    }

    public record Config(UUID configInstanceId) {

    }

}
