/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.kroxylicious.filter.encryption.common.RecordEncryptionUtil;
import io.kroxylicious.filter.encryption.config.TopicNameBasedKekSelector;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.UnknownAliasException;

import edu.umd.cs.findbugs.annotations.NonNull;

abstract class AbstractTemplateTopicNameKekSelector<K> extends TopicNameBasedKekSelector<K> {
    protected final String template;
    private final Kms<K, ?> kms;

    AbstractTemplateTopicNameKekSelector(@NonNull Kms<K, ?> kms, @NonNull Pattern templatePattern, @NonNull String template, @NonNull String parameterName) {
        validateTemplate(templatePattern.matcher(Objects.requireNonNull(template)), parameterName);
        this.template = Objects.requireNonNull(template);
        this.kms = Objects.requireNonNull(kms);
    }

    private record Pair<K>(String topicName, K kekId) {}

    @NonNull
    @Override
    public CompletionStage<Map<String, K>> selectKek(@NonNull Set<String> topicNames) {
        var collect = topicNames.stream().map(
                topicName -> evaluateTemplate(topicName)
                        .map(alias -> kms.resolveAlias(alias)
                                .exceptionallyCompose(e -> {
                                    if (e instanceof UnknownAliasException
                                            || (e instanceof CompletionException ce && ce.getCause() instanceof UnknownAliasException)) {
                                        return CompletableFuture.completedFuture(null);
                                    }
                                    return CompletableFuture.failedFuture(e);
                                })
                                .thenApply(kekId -> new Pair<>(topicName, kekId)))
                        .orElse(CompletableFuture.completedFuture(new Pair<>(topicName, null))))
                .toList();
        return RecordEncryptionUtil.join(collect).thenApply(list -> {
            // Note we can't use `java.util.stream...(Collectors.toMap())` to build the map, because it has null values
            // which Collectors.toMap() does now allow.
            Map<String, K> map = new HashMap<>();
            for (Pair<K> pair : list) {
                map.put(pair.topicName(), pair.kekId());
            }
            return map;
        });
    }

    protected abstract Optional<String> evaluateTemplate(String topicName);

    private void validateTemplate(@NonNull Matcher templateMatcher, @NonNull String parameterName) {
        while (templateMatcher.find()) {
            if (templateMatcher.group(1).equals(parameterName)) {
                continue;
            }
            throw new IllegalArgumentException("Unknown template parameter: " + templateMatcher.group(1));
        }
    }

}
