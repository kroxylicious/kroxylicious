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
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.kroxylicious.filter.encryption.common.RecordEncryptionUtil;
import io.kroxylicious.filter.encryption.config.RegexTopicNameConfig;
import io.kroxylicious.filter.encryption.config.TopicNameBasedKekSelector;
import io.kroxylicious.filter.encryption.config.TopicNameKekSelection;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.UnknownAliasException;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class RegexTopicNameKekSelector<K> extends TopicNameBasedKekSelector<K> {

    private static final Pattern PATTERN = Pattern.compile("\\$\\((.*?)\\)");

    @NonNull
    private final Kms<K, ?> kms;
    private final List<Rule> rules;
    private final ConcurrentHashMap<String, Rule> cache = new ConcurrentHashMap<>();

    public RegexTopicNameKekSelector(@NonNull Kms<K, ?> kms, @NonNull RegexTopicNameConfig config) {
        this.kms = Objects.requireNonNull(kms);
        Objects.requireNonNull(config);
        this.rules = config.rules().stream().map(Rule::from).toList();
    }

    public sealed interface Rule permits Rule.EncryptData, Rule.PassthroughData, Rule.NoRule {

        record EncryptData(String kekTemplate, List<Pattern> topicNamePatterns) implements Rule {
            public EncryptData {
                var matcher = PATTERN.matcher(Objects.requireNonNull(kekTemplate));
                while (matcher.find()) {
                    String parameterName = matcher.group(1);
                    if (parameterName.equals("topicName")) {
                        continue;
                    }
                    throw new IllegalArgumentException("Unknown template parameter: " + parameterName);
                }
            }
        }

        record PassthroughData(List<Pattern> topicNamePatterns) implements Rule {

        }

        // used as a marker in the cache that a topic had no rule
        record NoRule() implements Rule {

            @Override
            public List<Pattern> topicNamePatterns() {
                return List.of();
            }
        }

        List<Pattern> topicNamePatterns();

        default boolean matches(String topicName) {
            return topicNamePatterns().stream().anyMatch(p -> p.matcher(topicName).matches());
        }

        static Rule from(RegexTopicNameConfig.Rule configRule) {
            List<Pattern> topicNamePatterns = configRule.topicPatterns().stream().map(Pattern::compile).toList();
            if (configRule.behaviour() == RegexTopicNameConfig.Behaviour.ENCRYPT) {
                return new Rule.EncryptData(configRule.kekTemplate(), topicNamePatterns);
            }
            else {
                return new Rule.PassthroughData(topicNamePatterns);
            }
        }

    }

    enum Outcome {
        MAPPED,
        UNRESOLVED,
        PASSTHROUGH
    }

    private record Result<K>(String topicName, @Nullable K kekId, Outcome outcome) {
        private Result {
            if (outcome == Outcome.MAPPED) {
                Objects.requireNonNull(kekId, "if outcome is MAPPED, kekId must not be null");
            }
        }
    }

    @NonNull
    @Override
    public CompletionStage<TopicNameKekSelection<K>> selectKek(@NonNull Set<String> topicNames) {
        return mapTopicNameToResult(topicNames).thenApply((List<Result<K>> list) -> {
            Map<Outcome, List<Result<K>>> grouped = list.stream().collect(Collectors.groupingBy(Result::outcome));
            Set<String> unresolvedTopicNames = grouped.getOrDefault(Outcome.UNRESOLVED, List.of()).stream().map(Result::topicName).collect(toSet());
            Set<String> passthroughTopicNames = grouped.getOrDefault(Outcome.PASSTHROUGH, List.of()).stream().map(Result::topicName).collect(toSet());
            Map<String, K> topicNametoKey = grouped.getOrDefault(Outcome.MAPPED, List.of()).stream()
                    .collect(toMap(topicKek -> topicKek.topicName, topicKek -> Objects.requireNonNull(topicKek.kekId)));
            return new TopicNameKekSelection<>(topicNametoKey, passthroughTopicNames, unresolvedTopicNames);
        });
    }

    private CompletionStage<List<Result<K>>> mapTopicNameToResult(@NonNull Set<String> topicNames) {
        List<CompletionStage<Result<K>>> collect = topicNames.stream()
                .map(this::mapToResult).toList();
        return RecordEncryptionUtil.join(collect);
    }

    @NonNull
    private CompletionStage<Result<K>> mapToResult(String topicName) {
        Rule rule = findRule(topicName);
        if (rule instanceof Rule.PassthroughData) {
            return CompletableFuture.completedFuture(new Result<>(topicName, null, Outcome.PASSTHROUGH));
        }
        if (rule instanceof Rule.NoRule) {
            return CompletableFuture.completedFuture(new Result<>(topicName, null, Outcome.UNRESOLVED));
        }
        else if (rule instanceof Rule.EncryptData encryptData) {
            return mapToKek(topicName, encryptData);
        }
        else {
            throw new IllegalStateException("rule type " + rule.getClass().getSimpleName() + " not implemented");
        }
    }

    @NonNull
    private Rule findRule(String topicName) {
        return cache.computeIfAbsent(topicName, s -> rules.stream().filter(candidateRule -> candidateRule.matches(topicName)).findFirst().orElse(new Rule.NoRule()));
    }

    @NonNull
    private CompletionStage<Result<K>> mapToKek(String topicName, Rule.EncryptData encryptData) {
        String kekName = evaluateTemplate(topicName, encryptData.kekTemplate());
        return kms.resolveAlias(kekName)
                .exceptionallyCompose(e -> {
                    if (e instanceof UnknownAliasException
                            || (e instanceof CompletionException ce && ce.getCause() instanceof UnknownAliasException)) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return CompletableFuture.failedFuture(e);
                })
                .thenApply(kekId -> new Result<>(topicName, kekId, kekId == null ? Outcome.UNRESOLVED : Outcome.MAPPED));
    }

    static String evaluateTemplate(String topicName, String template) {
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
