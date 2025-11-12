/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.subject;

import java.util.Locale;
import java.util.Optional;
import java.util.function.UnaryOperator;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;

public final class ReplaceMatchMappingRule implements MappingRule {
    private final Pattern pattern;
    private final UnaryOperator<String> flags;
    private final String replacement;

    public ReplaceMatchMappingRule(String mappingRule) {
        if (mappingRule.isEmpty()) {
            throw new IllegalArgumentException("Invalid mapping rule: rule is empty, but it should have the format `cPATTERNcREPLACEMENTcFLAGS`, "
                    + "where `c` is a separator character of your choosing.");
        }
        var separator = mappingRule.charAt(0);
        var endPattern = mappingRule.indexOf(separator, 1);
        if (endPattern == -1) {
            throw new IllegalArgumentException(("Invalid mapping rule at index %s: '%s' should have the format `cPATTERNcREPLACEMENTcFLAGS`, "
                    + "where `c` is a separator character of your choosing. "
                    + "You seem to be using '%s' as the separator character, but it only occurs once, at the start of the string. "
                    + "(Hint: The rule format is not the same as Kafka's).").formatted(mappingRule.length() - 1, mappingRule, separator));
        }
        else if (endPattern == mappingRule.length() - 1) {
            throw new IllegalArgumentException(("Invalid mapping rule at index %s: '%s' should have the format `cPATTERNcREPLACEMENTcFLAGS`, "
                    + "where `c` is a separator character of your choosing. "
                    + "You seem to be using '%s' as the separator character, but it only occurs twice. "
                    + "(Hint: The rule format is not the same as Kafka's).").formatted(mappingRule.length() - 1, mappingRule, separator));
        }
        var endReplacement = mappingRule.indexOf(separator, endPattern + 1);
        if (endReplacement == -1) {
            throw new IllegalArgumentException(("Invalid mapping rule at index %s: '%s' should have the format `cPATTERNcREPLACEMENTcFLAGS`, "
                    + "where `c` is a separator character of your choosing. "
                    + "You seem to be using '%s' as the separator character, but it only occurs twice. "
                    + "(Hint: The rule format is not the same as Kafka's).").formatted(mappingRule.length() - 1, mappingRule, separator));
        }
        String patternStr = mappingRule.substring(1, endPattern);
        Pattern pattern;
        try {
            pattern = Pattern.compile(patternStr);
        }
        catch (PatternSyntaxException e) {
            throw new IllegalArgumentException(("Invalid mapping rule at index %s: The pattern part of the rule, '%s', is not a valid "
                    + "regular expression in RE2 format: %s.").formatted((e.getIndex() == -1 ? 0 : e.getIndex()) + 1, patternStr, e.getDescription()));
        }
        String replacement = mappingRule.substring(endPattern + 1, endReplacement);
        String flagsStr = mappingRule.substring(endReplacement + 1);
        UnaryOperator<String> flags = switch (flagsStr) {
            case "L" -> s -> s.toLowerCase(Locale.ROOT);
            case "U" -> s -> s.toUpperCase(Locale.ROOT);
            case "" -> UnaryOperator.identity();
            default -> {
                throw new IllegalArgumentException(("Invalid mapping rule at index %s: The given flags, '%s', are not valid. "
                        + "The flags may be empty or 'L' or 'U'.").formatted(endReplacement + 1, flagsStr));
            }
        };
        this.pattern = pattern;
        this.replacement = replacement;
        this.flags = flags;

    }

    @Override
    public Optional<String> apply(String s) {
        var ruleMatcher = pattern.matcher(s);
        if (ruleMatcher.matches()) {
            return Optional.of(flags.apply(ruleMatcher.replaceAll(replacement)));
        }
        return Optional.empty();
    }

    @Override
    public String toString() {
        return "ReplaceMatchMappingRule{" +
                "flags=" + flags +
                ", pattern=" + pattern +
                ", replacement='" + replacement + '\'' +
                '}';
    }
}
