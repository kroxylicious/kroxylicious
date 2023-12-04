/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.filter.encryption.inband.BufferPool;
import io.kroxylicious.filter.encryption.inband.InBandKeyManager;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link FilterFactory} for {@link EnvelopeEncryptionFilter}.
 * @param <E> The type of encrypted DEK
 */
@Plugin(configType = EnvelopeEncryption.Config.class)
public class EnvelopeEncryption<E> implements FilterFactory<EnvelopeEncryption.Config, EnvelopeEncryption.Config> {

    record Config(
                  @JsonProperty(required = true) @PluginImplName(KmsService.class) String kms,
                  @PluginImplConfig(implNameProperty = "kms") Object kmsConfig,

                  @JsonProperty(required = true) @PluginImplName(KekSelectorService.class) String selector,
                  @PluginImplConfig(implNameProperty = "selector") Object selectorConfig) {

    }

    @Override
    public Config initialize(FilterFactoryContext context, Config config) throws PluginConfigurationException {
        return config;
    }

    @NonNull
    @Override
    public EnvelopeEncryptionFilter createFilter(FilterFactoryContext context, Config configuration) {
        KmsService<Object, E> kmsPlugin = context.pluginInstance(KmsService.class, configuration.kms());
        Kms<E> kms = kmsPlugin.buildKms(configuration.kmsConfig());

        var keyManager = new InBandKeyManager<>(kms, BufferPool.allocating(), 500_000);

        KekSelectorService<Object> ksPlugin = context.pluginInstance(KekSelectorService.class, configuration.selector());
        TopicNameBasedKekSelector kekSelector = ksPlugin.buildSelector(kms, configuration.selectorConfig());
        return new EnvelopeEncryptionFilter(keyManager, kekSelector);
    }
}