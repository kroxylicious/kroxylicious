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

import static io.kroxylicious.filter.encryption.TopicPrefixBasedKekSelector.TemplateConfig;

/**
 * Simple {@link KekSelectorService} that derives the KEK alias using a template and topic name prefix.
 * <br/>
 * This topic prefix is demarcated by the first occurrence of a {@code prefixSeparator} within the topic's name.
 * <br/>
 * The template is used to form the KEK alias.  It may include the substitution variable
 * {@code ${topicPrefix}} which gets replaced by the value of the topic name prefix (without the separator).
 * <br/>
 * If the topic name does not include the {@code prefixSeparator} or the prefix is the empty string,
 * no KEK will be selected for that topic.
 *
 * @param <K> The type of Key Encryption Key id.
 */

@Plugin(configType = TemplateConfig.class)
public class TopicPrefixBasedKekSelector<K> implements KekSelectorService<TemplateConfig, K> {

    @NonNull
    @Override
    public TopicNameBasedKekSelector<K> buildSelector(@NonNull Kms<K, ?> kms, TemplateConfig config) {
        return new Selector<>(kms, config.template(), config.prefixSeparator());
    }

    public record TemplateConfig(
                                 @NonNull String prefixSeparator,
                                 @NonNull String template) {}

    private static class Selector<K> extends AbstractTemplateTopicNameKekSelector<K> {

        private static final Pattern PATTERN = Pattern.compile("\\$\\{(.*?)}");
        public static final String TOPIC_PREFIX = "topicPrefix";
        private final String prefixSeparator;

        Selector(@NonNull Kms<K, ?> kms, @NonNull String template, String prefixSeparator) {
            super(kms, PATTERN, template, TOPIC_PREFIX);
            this.prefixSeparator = Objects.requireNonNull(prefixSeparator);

            if (prefixSeparator.isEmpty()) {
                throw new IllegalArgumentException("Prefix separator cannot be empty");
            }
        }

        protected Optional<String> evaluateTemplate(String topicName) {
            var separatorIndex = topicName.indexOf(prefixSeparator);
            if (separatorIndex < 0) {
                return Optional.empty();
            }
            var prefix = topicName.substring(0, separatorIndex);
            var matcher = PATTERN.matcher(template);
            StringBuilder sb = new StringBuilder();
            while (matcher.find()) {
                String replacement;
                if (matcher.group(1).equals(TOPIC_PREFIX)) {
                    replacement = prefix;
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
