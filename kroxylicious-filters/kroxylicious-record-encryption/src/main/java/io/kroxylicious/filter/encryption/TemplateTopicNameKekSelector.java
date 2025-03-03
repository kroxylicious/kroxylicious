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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.filter.encryption.common.RecordEncryptionUtil;
import io.kroxylicious.filter.encryption.config.TopicNameBasedKekSelector;
import io.kroxylicious.filter.encryption.config.TopicNameKekSelection;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.UnknownAliasException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

class TemplateTopicNameKekSelector<K> extends TopicNameBasedKekSelector<K> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateTopicNameKekSelector.class);
    private static final AtomicBoolean emittedPlaceholderDeprecationWarning = new AtomicBoolean(false);

    public static final Pattern PATTERN = Pattern.compile("\\$(?:\\((.*?)\\)|\\{(.*?)})");
    private final String template;
    private final Kms<K, ?> kms;

    TemplateTopicNameKekSelector(@NonNull Kms<K, ?> kms, @NonNull String template) {
        var matcher = PATTERN.matcher(Objects.requireNonNull(template));
        while (matcher.find()) {
            String parameterName = templateParameterName(matcher);
            if (parameterName.equals("topicName")) {
                continue;
            }
            throw new IllegalArgumentException("Unknown template parameter: " + parameterName);
        }
        this.template = Objects.requireNonNull(template);
        this.kms = Objects.requireNonNull(kms);
    }

    private static String templateParameterName(Matcher matcher) {
        String group = matcher.group(1);
        if (group == null) {
            group = matcher.group(2);
            if (!emittedPlaceholderDeprecationWarning.getAndSet(true)) {
                LOGGER.warn("Use of $\\{}-style placeholders in the KEK template of the record encryption filter is deprecated and will be removed in a future version. "
                        + "Please replace '${{}}' in the template with '$({})'.", group, group);
            }
        }
        return group;
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
            if (templateParameterName(matcher).equals("topicName")) {
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
