/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.micrometer;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = CommonTagsHook.CommonTagsHookConfig.class)
public class CommonTagsHook implements MicrometerConfigurationHookService<CommonTagsHook.CommonTagsHookConfig> {

    private static final Logger log = LoggerFactory.getLogger(CommonTagsHook.class);

    @NonNull
    @Override
    public MicrometerConfigurationHook build(CommonTagsHookConfig config) {
        return new Hook(config);
    }

    public static class CommonTagsHookConfig {
        private final Map<String, String> commonTags;

        @JsonCreator
        public CommonTagsHookConfig(Map<String, String> commonTags) {
            this.commonTags = commonTags == null ? Map.of() : commonTags;
        }
    }

    private static class Hook implements MicrometerConfigurationHook {
        private final CommonTagsHookConfig config;

        private Hook(CommonTagsHookConfig config) {
            if (config == null) {
                throw new IllegalArgumentException("config must be non null");
            }
            this.config = config;
        }

        @Override
        public void configure(MeterRegistry targetRegistry) {
            List<Tag> tags = config.commonTags.entrySet().stream().map(entry -> Tag.of(entry.getKey(), entry.getValue())).collect(Collectors.toList());
            targetRegistry.config().commonTags(tags);
            log.info("configured micrometer registry with tags: {}", tags);
        }
    }

}
