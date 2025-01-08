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

    private static final Map<UUID, AtomicInteger> initializeCounts = new ConcurrentHashMap<>();
    private static final Map<UUID, AtomicInteger> closeCounts = new ConcurrentHashMap<>();

    @Override
    public Config initialize(FilterFactoryContext context, Config config) throws PluginConfigurationException {
        initializeCounts.computeIfAbsent(config.configInstanceId, uuid -> new AtomicInteger(0)).getAndIncrement();
        return config;
    }

    @NonNull
    @Override
    public Filter createFilter(FilterFactoryContext context, Config initializationData) {
        return (RequestFilter) (apiKey, header, request, requestFilterContext) -> requestFilterContext.forwardRequest(header, request);
    }

    @Override
    public void close(Config config) {
        closeCounts.computeIfAbsent(config.configInstanceId, uuid -> new AtomicInteger(0)).getAndIncrement();
    }

    public static void assertInitializationCount(UUID configId, int count) {
        assertThat(initializeCounts.get(configId)).hasValue(count);
    }

    public static void assertAllClosedAndResetCounts() {
        // Everything that was initialised gets closed
        for (var entry : initializeCounts.entrySet()) {
            UUID uuid = entry.getKey();
            assertThat(closeCounts.get(uuid)).hasValue(entry.getValue().intValue());
        }
        // Nothing what closed that wasn't initialized
        closeCounts.keySet().forEach(uuid -> {
            assertThat(initializeCounts).containsKey(uuid);
        });

        initializeCounts.clear();
        closeCounts.clear();
    }

    public record Config(UUID configInstanceId) {

    }

}
