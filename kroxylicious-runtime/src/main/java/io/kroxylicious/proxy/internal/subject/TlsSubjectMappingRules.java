/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class TlsSubjectMappingRules implements UnaryOperator<String> {

    static final Pattern RULE_PATTERN = Pattern.compile("^(?<pattern>.*?)/(?<replacement>.*?)/(?<flags>[LU]?)$");

    record Rule(Pattern pattern, String replacement, UnaryOperator<String> flags) {}

    private final List<UnaryOperator<String>> rules;

    TlsSubjectMappingRules(List<String> mappingRules) {
        List<UnaryOperator<String>> operators = new ArrayList<>();
        for (int i = 0; i < mappingRules.size(); i++) {
            String mappingRule = mappingRules.get(i);
            if (mappingRule.equals("DEFAULT")) {
                if (i == mappingRules.size() - 1) {
                    operators.add(UnaryOperator.identity());
                }
                else {
                    throw new IllegalArgumentException("DEFAULT rule must be last in the list");
                }
            }
            else {
                Matcher ruleSyntaxMatcher = RULE_PATTERN.matcher(mappingRule);
                if (!ruleSyntaxMatcher.matches()) {
                    throw new IllegalArgumentException("Invalid TLS subject mapping rule at index %s: '%s' should have the format PATTERN/REPLACEMENT/FLAGS".formatted(
                            i,
                            mappingRule));
                }
                Pattern pattern = Pattern.compile(ruleSyntaxMatcher.group("pattern"));
                String replacement = ruleSyntaxMatcher.group("replacement");
                UnaryOperator<String> flags = switch (ruleSyntaxMatcher.group("flags")) {
                    case "L" -> s -> s.toLowerCase(Locale.ROOT);
                    case "U" -> s -> s.toUpperCase(Locale.ROOT);
                    case "" -> TlsSubjectBuilder.DEFAULT;
                    default -> throw new IllegalArgumentException("Invalid rule: " + mappingRule);
                };

                operators.add(s -> {
                    var ruleMatcher = pattern.matcher(s);
                    if (ruleMatcher.matches()) {
                        return flags.apply(ruleMatcher.replaceAll(replacement));
                    }
                    return null;
                });
            }
        }
        this.rules = operators;
    }

    @Override
    public String apply(String name) {
        for (var operator : this.rules) {
            var applied = operator.apply(name);
            if (applied != null) {
                return applied;
            }
        }
        // TODO check that this behaviour is actually what Kafka implements
        throw new IllegalArgumentException("No matching rule for '%s'".formatted(name));
    }

}
