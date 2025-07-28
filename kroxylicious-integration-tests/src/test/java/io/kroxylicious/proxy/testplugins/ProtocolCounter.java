/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.testplugins;

import java.util.EnumMap;
import java.util.Set;

import org.apache.kafka.common.protocol.ApiKeys;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.Plugins;
import io.kroxylicious.proxy.testplugins.ProtocolCounter.Config;

@Plugin(configType = Config.class)
public class ProtocolCounter implements FilterFactory<Config, Config> {

    @Override
    public Config initialize(FilterFactoryContext context, Config config) throws PluginConfigurationException {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Config config) {
        EnumMap<ApiKeys, Integer> requestMap = new EnumMap<>(ApiKeys.class);
        for (ApiKeys apiKey : config.countRequests()) {
            requestMap.put(apiKey, 0);
        }

        EnumMap<ApiKeys, Integer> responseMap = new EnumMap<>(ApiKeys.class);
        for (ApiKeys apiKey : config.countResponses()) {
            responseMap.put(apiKey, 0);
        }

        return new ProtocolCounterFilter(requestMap, responseMap);
    }

    public record Config(Set<ApiKeys> countRequests,
                         Set<ApiKeys> countResponses) {

    }

}
