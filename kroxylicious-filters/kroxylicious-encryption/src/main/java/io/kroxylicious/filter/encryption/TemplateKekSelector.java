/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

@Plugin(configType = TemplateKekSelector.Config.class)
public class TemplateKekSelector<K> implements KekSelectorService<TemplateKekSelector.Config, K> {

    public record TopicNameMatcher(String topicName,
                                   String topicNamePrefix,
                                   String template) {
        public TopicNameMatcher {
            if ((topicName == null || topicName.isEmpty()) == (topicNamePrefix == null)) {
                throw new IllegalArgumentException("Either topicName or topicNamePrefix (but not both), must be specified");
            }
        }
    }

    public record Config(List<TopicNameMatcher> templates) {}

    @NonNull
    @Override
    public TopicNameBasedKekSelector<K> buildSelector(@NonNull Kms<K, ?> kms, Config config) {
        return new KekSelector<>(kms, config.templates());
    }

    static class KekSelector<K> extends TopicNameBasedKekSelector<K> {

        private static final char END_EXACT = '\0'; // a char before the smallest allowed character in a topic name
        private static final char END_PREFIX = '{'; // the char after the largest allowed character in a topic name
        public static final Pattern PATTERN = Pattern.compile("\\$\\{(.*?)}");
        private final TreeMap<String, String> templates;
        private final Kms<K, ?> kms;

        KekSelector(@NonNull Kms<K, ?> kms, @NonNull List<TopicNameMatcher> topicNameMatchers) {
            var validatedTemplates = new TreeMap<String, String>();
            for (var topicNameMatcher : topicNameMatchers) {
                var template = topicNameMatcher.template();
                var matcher = PATTERN.matcher(Objects.requireNonNull(template));
                while (matcher.find()) {
                    if (matcher.group(1).equals("topicName")) {
                        continue;
                    }
                    throw new IllegalArgumentException("Unknown template parameter: " + matcher.group(1));
                }
                if (topicNameMatcher.topicName() != null && !topicNameMatcher.topicName().isEmpty()) {
                    validatedTemplates.put(topicNameMatcher.topicName(), template);
                    validatedTemplates.put(topicNameMatcher.topicName() + END_EXACT, null);
                }
                else {
                    validatedTemplates.put(topicNameMatcher.topicNamePrefix(), template);
                    validatedTemplates.put(topicNameMatcher.topicNamePrefix() + END_PREFIX, null);
                }
            }

            this.templates = Objects.requireNonNull(validatedTemplates);
            this.kms = Objects.requireNonNull(kms);
        }

        @Override
        public @NonNull Map<String, CompletionStage<Optional<K>>> selectKek(@NonNull Set<String> topicNames) {
            return topicNames.stream()
                    .collect(Collectors.toMap(
                            topicName -> topicName,
                            topicName -> {
                                String template = lookup(topicName);
                                if (template == null) {
                                    return CompletableFuture.completedFuture(Optional.<K> empty());
                                }
                                String alias = evaluateTemplate(template, topicName);
                                return kms.resolveAlias(alias).thenApply(Optional::ofNullable);
                            }));
        }

        private @NonNull String evaluateTemplate(@NonNull String template, @NonNull String topicName) {

            var matcher = PATTERN.matcher(template);
            StringBuilder sb = new StringBuilder();
            while (matcher.find()) {
                String replacement;
                if (matcher.group(1).equals("topicName")) {
                    replacement = topicName;
                }
                else { // this should be impossible because of the check in the constructor
                    throw new IllegalStateException();
                }
                matcher.appendReplacement(sb, replacement);
            }
            matcher.appendTail(sb);
            return sb.toString();
        }

        private @Nullable String lookup(@NonNull String topicName) {
            var geEntry = templates.ceilingEntry(topicName);
            if (geEntry == null) {
                return null;
            }
            String geKey = geEntry.getKey();
            if (geKey.equals(topicName)) { // exact match
                return geEntry.getValue();
            }
            if (geKey.endsWith("" + END_EXACT)) { // failed on exact match
                return null;
            }
            else if (geKey.endsWith("{")) {
                // it might be a prefix match
                var leEntry = templates.floorEntry(topicName);
                if (leEntry != null && topicName.startsWith(leEntry.getKey())) {
                    return leEntry.getValue();
                }
                throw new IllegalArgumentException("topicName: " + topicName + ", ge: " + geKey + ", le: " + leEntry);
            }
            return null;
        }
    }
}
