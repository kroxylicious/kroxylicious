/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import io.kroxylicious.filter.encryption.config.KekSelectorService;
import io.kroxylicious.filter.encryption.config.TemplateConfig;
import io.kroxylicious.filter.encryption.config.TopicNameBasedKekSelector;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link KekSelectorService} which builds KEK selectors that derive the KEK alias
 * from the topic name using a template.
 * @param <K> The type of KEK id.
 */
@Plugin(configType = TemplateConfig.class)
public class TemplateKekSelector<K> implements KekSelectorService<TemplateConfig, K> {

    /**
     * Creates a new selector service instance, invoked by the plugin framework.
     */
    public TemplateKekSelector() {
        // nothing to initialise: state is created in buildSelector(Kms, TemplateConfig)
    }

    @NonNull
    @Override
    public TopicNameBasedKekSelector<K> buildSelector(@NonNull Kms<K, ?> kms, TemplateConfig config) {
        return new TemplateTopicNameKekSelector<>(kms, config.template());
    }

}
