/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

import io.kroxylicious.filter.encryption.config.KekSelectorService;
import io.kroxylicious.filter.encryption.config.TopicNameBasedKekSelector;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.filter.encryption.TemplateKekSelector.TemplateConfig;

/**
 * Simple {@link KekSelectorService} that uses a template to derive a KEK alias.  The template
 * may include {@code ${topicName}} substitution variable.  If this variable is included, it is replaced
 * by the topic name.
 *
 * @param <K> The type of Key Encryption Key id.
 */
@Plugin(configType = TemplateConfig.class)
public class TemplateKekSelector<K> implements KekSelectorService<TemplateConfig, K> {

    private static final Pattern PATTERN = Pattern.compile("\\$\\{(.*?)}");

    @NonNull
    @Override
    public TopicNameBasedKekSelector<K> buildSelector(@NonNull Kms<K, ?> kms, @NonNull TemplateConfig config) {
        Objects.requireNonNull(kms);
        Objects.requireNonNull(config);
        return new Selector<>(kms, config);
    }

    /**
     * Configuration for the {@link TemplateKekSelector}.
     *
     * @param template template string
     */
    public record TemplateConfig(@NonNull String template) {
        public TemplateConfig {
            Objects.requireNonNull(template);
        }
    }

    private static class Selector<K> extends AbstractTemplateTopicNameKekSelector<K> {

        private static final String PARAMETER_NAME = "topicName";

        Selector(Kms<K, ?> kms, TemplateConfig config) {
            super(kms, PATTERN, config.template(), PARAMETER_NAME);
        }

        @Override
        protected Optional<String> evaluateTemplate(String topicName) {
            var matcher = PATTERN.matcher(template);
            StringBuilder sb = new StringBuilder();
            while (matcher.find()) {
                String replacement;
                if (matcher.group(1).equals(PARAMETER_NAME)) {
                    replacement = topicName;
                }
                else { // this should be impossible because of the check in the constructor
                    throw new IllegalStateException();
                }
                matcher.appendReplacement(sb, replacement);
            }
            matcher.appendTail(sb);
            return Optional.of(sb.toString());
        }

    }
}
