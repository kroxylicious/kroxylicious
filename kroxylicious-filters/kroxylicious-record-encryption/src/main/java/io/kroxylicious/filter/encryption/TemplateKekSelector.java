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

@Plugin(configType = TemplateConfig.class)
public class TemplateKekSelector<K> implements KekSelectorService<TemplateConfig, K> {

    @NonNull
    @Override
    public TopicNameBasedKekSelector<K> buildSelector(@NonNull
    Kms<K, ?> kms, TemplateConfig config) {
        return new TemplateTopicNameKekSelector<>(kms, config.template());
    }

}
