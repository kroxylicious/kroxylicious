/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

import io.kroxylicious.kms.service.KekId;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

@Plugin(configType = TemplateKekSelector.Config.class)
public class TemplateKekSelector implements KekSelectorService<TemplateKekSelector.Config> {
    public record Config(String template) {}

    @NonNull
    @Override
    public TopicNameBasedKekSelector buildSelector(@NonNull Kms<?> kms, Config config) {
        return new KekSelector(kms, config.template());
    }

    static class KekSelector extends TopicNameBasedKekSelector {

        public static final Pattern PATTERN = Pattern.compile("\\$\\{(.*?)}");
        private final String template;
        private final Kms<?> kms;

        KekSelector(@NonNull Kms<?> kms, @NonNull String template) {
            var matcher = PATTERN.matcher(Objects.requireNonNull(template));
            while (matcher.find()) {
                if (matcher.group(1).equals("topicName")) {
                    continue;
                }
                throw new IllegalArgumentException("Unknown template parameter: " + matcher.group(1));
            }
            this.template = Objects.requireNonNull(template);
            this.kms = Objects.requireNonNull(kms);
        }

        private record Pair<K>(String topicName, K kekId) {}

        @NonNull
        @Override
        public CompletionStage<Map<String, KekId>> selectKek(@NonNull Set<String> topicNames) {
            var collect = topicNames.stream()
                    .map(
                            topicName -> kms.resolveAlias(evaluateTemplate(topicName))
                                    .exceptionallyCompose(e -> {
                                        if (e instanceof UnknownAliasException
                                                || (e instanceof CompletionException ce && ce.getCause() instanceof UnknownAliasException)) {
                                            return CompletableFuture.completedFuture(null);
                                        }
                                        return CompletableFuture.failedFuture(e);
                                    })
                                    .thenApply(kekId -> new Pair<>(topicName, kekId)))
                    .toList();
            return EnvelopeEncryptionFilter.join(collect).thenApply(list -> {
                // Note we can't use `java.util.stream...(Collectors.toMap())` to build the map, because it has null values
                // which Collectors.toMap() does now allow.
                Map<String, KekId> map = new HashMap<>();
                for (Pair<KekId> pair : list) {
                    map.put(pair.topicName(), pair.kekId());
                }
                return map;
            });
        }

        String evaluateTemplate(String topicName) {
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
    }
}