/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.kroxylicious.filter.encryption.common.RecordEncryptionUtil;
import io.kroxylicious.filter.encryption.config.TopicNameBasedKekSelector;
import io.kroxylicious.filter.encryption.config.TopicNameKekSelection;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.UnknownAliasException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

class TemplateTopicNameKekSelector<K> extends TopicNameBasedKekSelector<K> {

    public static final Pattern PATTERN = Pattern.compile("\\$\\{(.*?)}");
    private final String template;
    private final Kms<K, ?> kms;

    TemplateTopicNameKekSelector(@NonNull Kms<K, ?> kms, @NonNull String template) {
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
    public CompletionStage<TopicNameKekSelection<K>> selectKek(@NonNull Set<String> topicNames) {
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
        return RecordEncryptionUtil.join(collect).thenApply(list -> {
            Map<Boolean, List<Pair<K>>> partitioned = list.stream().collect(Collectors.partitioningBy(x -> x.kekId == null));
            Set<String> unresolvedTopicNames = partitioned.get(true).stream().map(Pair::topicName).collect(toSet());
            Map<String, K> topicNametoKey = partitioned.get(false).stream().collect(toMap(topicKek -> topicKek.topicName, topicKek -> topicKek.kekId));
            return new TopicNameKekSelection<>(topicNametoKey, unresolvedTopicNames);
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
